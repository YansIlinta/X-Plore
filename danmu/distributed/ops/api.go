package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// API 是 Ops Console 的 HTTP 面。GET 全部只读，读取 Collector 的最新快照；
// 仅实验/压测/sweep 的 start/stop 是 ACTION（启停 loadtest 子进程）。
// 状态机的唯一主人是 ExperimentManager；SweepManager 只编排顺序执行。
type API struct {
	col      *Collector
	em       *ExperimentManager
	sweepMgr *SweepManager
}

func NewAPI(col *Collector, em *ExperimentManager) *API {
	return &API{col: col, em: em}
}

// WithSweeps 附加 sweep 管理器（无则 sweep 端点不可用）。
func (a *API) WithSweeps(sm *SweepManager) *API {
	a.sweepMgr = sm
	return a
}

// Handler 返回路由 mux。
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", a.handleHealth)
	mux.HandleFunc("/api/overview", a.handleOverview)
	mux.HandleFunc("/api/services", a.handleServices)
	mux.HandleFunc("/api/topology", a.handleTopology)
	mux.HandleFunc("/api/events", a.handleEvents)
	mux.HandleFunc("/api/traces", a.handleTraces)
	mux.HandleFunc("/api/rooms", a.handleRooms)
	mux.HandleFunc("/api/rooms/", a.handleRoomDetail)
	// Realtime Systems Lab（实验/对比/证据/预设）
	a.registerExperimentRoutes(mux)
	// Phase 1.5：Sweep / Regime
	a.registerSweepRoutes(mux)
	// 旧 loadtest 端点保留为 ExperimentManager 的兼容入口
	mux.HandleFunc("/api/loadtest/status", a.handleLoadtestStatus)
	mux.HandleFunc("/api/loadtest/start", a.handleLoadtestStart)
	mux.HandleFunc("/api/loadtest/stop", a.handleLoadtestStop)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleOverview 回答"系统现在是否正常"：核心 KPI 只来自真实聚合。
func (a *API) handleOverview(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()

	var activeConns, activeRooms *float64
	var msgIn, msgOut *float64
	rateSeen := false
	cometTotal, cometHealthy := 0, 0
	for _, svc := range snap.Services {
		if svc.Name != "comet" {
			continue
		}
		for _, it := range svc.Instances {
			cometTotal++
			if it.Healthy {
				cometHealthy++
			}
			if v := asFloat(it.Stats["conn_count"]); v != nil {
				activeConns = addF(activeConns, *v)
			}
			if v := asFloat(it.Stats["room_count"]); v != nil {
				activeRooms = addF(activeRooms, *v)
			}
			if it.MsgInRate != nil || it.MsgOutRate != nil {
				rateSeen = true
			}
			msgIn = addF(msgIn, derefOrZero(it.MsgInRate))
			msgOut = addF(msgOut, derefOrZero(it.MsgOutRate))
		}
	}
	// 没有任何速率样本时保持 null（前端显示 N/A），不能把"没有数据"算成 0。
	if !rateSeen {
		msgIn, msgOut = nil, nil
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mock":               snap.Mock,
		"ts":                 snap.TS,
		"health":             snap.Health,
		"health_detail":      snap.HealthDetail,
		"active_connections": activeConns,
		"active_rooms":       activeRooms,
		"msg_in_rate":        msgIn,
		"msg_out_rate":       msgOut,
		"comet_instances":    map[string]int{"total": cometTotal, "healthy": cometHealthy},
		"kafka":              snap.Kafka,
	})
}

// handleServices 统一服务实例视图：按组件分组，透传实例原始 stats。
func (a *API) handleServices(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"mock":     snap.Mock,
		"ts":       snap.TS,
		"services": snap.Services,
	})
}

// handleTopology 实时架构拓扑：节点健康来自采集，nginx/clients 无法观测 → healthy=null。
func (a *API) handleTopology(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()

	nodes := []map[string]any{
		node("clients", "client", "Clients", nil),
		node("nginx", "nginx", "NGINX :8088", nil), // 无观测端点，honest null
	}
	edges := []map[string]string{
		{"from": "clients", "to": "nginx", "kind": "ws"},
	}

	var cometIDs, logicIDs, jobIDs []string
	for _, svc := range snap.Services {
		for _, it := range svc.Instances {
			id := it.HTTPAddr
			label := it.HTTPAddr
			if it.RPCAddr != "" {
				label = it.RPCAddr + " / " + it.HTTPAddr
			}
			var healthy *bool
			if it.Healthy {
				healthy = boolPtr(true)
			} else {
				healthy = boolPtr(false)
			}
			nodes = append(nodes, node(id, svc.Name, label, healthy))
			switch svc.Name {
			case "comet":
				cometIDs = append(cometIDs, id)
			case "logic":
				logicIDs = append(logicIDs, id)
			case "job":
				jobIDs = append(jobIDs, id)
			}
		}
	}

	// 固定拓扑边：nginx→comet、comet→logic、logic→kafka、kafka→job、job→comet
	kafkaHealthy := boolPtr(snap.Kafka.Available)
	etcdHealthy := boolPtr(snap.EtcdUp)
	nodes = append(nodes,
		node("kafka", "kafka", "Kafka", kafkaHealthy),
		node("etcd", "etcd", "etcd :2379", etcdHealthy),
	)
	for _, cid := range cometIDs {
		edges = append(edges, map[string]string{"from": "nginx", "to": cid, "kind": "ws"})
	}
	for _, cid := range cometIDs {
		for _, lid := range logicIDs {
			edges = append(edges, map[string]string{"from": cid, "to": lid, "kind": "rpc"})
		}
	}
	for _, lid := range logicIDs {
		edges = append(edges, map[string]string{"from": lid, "to": "kafka", "kind": "produce"})
	}
	for _, jid := range jobIDs {
		edges = append(edges, map[string]string{"from": "kafka", "to": jid, "kind": "consume"})
		for _, cid := range cometIDs {
			edges = append(edges, map[string]string{"from": jid, "to": cid, "kind": "rpc"})
		}
	}
	// etcd 与服务发现/注册关系（虚线，前端按 kind 区分）
	for _, cid := range cometIDs {
		edges = append(edges, map[string]string{"from": "etcd", "to": cid, "kind": "discover"})
	}
	for _, lid := range logicIDs {
		edges = append(edges, map[string]string{"from": "etcd", "to": lid, "kind": "discover"})
	}
	for _, jid := range jobIDs {
		edges = append(edges, map[string]string{"from": "etcd", "to": jid, "kind": "discover"})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mock":  snap.Mock,
		"ts":    snap.TS,
		"nodes": nodes,
		"edges": edges,
	})
}

// handleEvents 返回最近事件（默认 100 条，上限 buffer 容量 500）。
func (a *API) handleEvents(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= eventBufferSize {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mock":   snap.Mock,
		"ts":     snap.TS,
		"events": a.col.Events(limit),
	})
}

// handleTraces 返回汇聚好的消息链路（最新在前，默认 50 条，上限 traceMaxKept）。
// sources 透传各节点自述的采样状态——包含缓冲溢出计数，让"没采全"可见。
func (a *API) handleTraces(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= traceMaxKept {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mock":    snap.Mock,
		"ts":      snap.TS,
		"sources": a.col.TraceSources(),
		"traces":  a.col.Traces(limit),
	})
}

// node 构造拓扑节点；healthy 为 nil 表示未观测。
func node(id, kind, label string, healthy *bool) map[string]any {
	m := map[string]any{"id": id, "kind": kind, "label": label}
	if healthy != nil {
		m["healthy"] = *healthy
	} else {
		m["healthy"] = nil
	}
	return m
}

func boolPtr(b bool) *bool { return &b }

// asFloat 从实例 stats JSON 里取值（json.Number / float64 / int64 均可）。
func asFloat(v any) *float64 {
	switch n := v.(type) {
	case float64:
		return &n
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return &f
		}
	case int64:
		f := float64(n)
		return &f
	case int:
		f := float64(n)
		return &f
	}
	return nil
}

func derefOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

func addF(acc *float64, v float64) *float64 {
	s := derefOrZero(acc) + v
	return &s
}

// ---- Rooms（按需扇出查询，不进周期采集循环）----

// handleRooms GET /api/rooms：合并各 comet 的房间列表（在线数降序，上限每 comet 100 条）。
func (a *API) handleRooms(w http.ResponseWriter, r *http.Request) {
	snap := a.col.Snapshot()
	if snap.Mock {
		writeJSON(w, http.StatusOK, map[string]any{
			"mock": true, "ts": snap.TS, "partial": false,
			"rooms": []RoomView{
				{RoomID: "room-1842", OnlineCount: 5291, Comets: []string{"comet1:8080"}, Active: true},
				{RoomID: "room-921", OnlineCount: 3102, Comets: []string{"comet2:8080"}, Active: true},
			},
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	rooms, failed := a.col.aggregateRooms(ctx, "")
	writeJSON(w, http.StatusOK, map[string]any{
		"mock":    false,
		"ts":      snap.TS,
		"partial": failed > 0, // 有 comet 拉取失败：结果可能不全
		"total":   len(rooms),
		"rooms":   rooms,
	})
}

// handleRoomDetail GET /api/rooms/{id}：定位房间在哪些 comet 上。
func (a *API) handleRoomDetail(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimPrefix(r.URL.Path, "/api/rooms/")
	if roomID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing room id"})
		return
	}
	snap := a.col.Snapshot()
	if snap.Mock {
		// mock 模式：与 /api/rooms 的演示数据保持一致
		if roomID == "room-1842" {
			writeJSON(w, http.StatusOK, map[string]any{
				"mock": true, "ts": snap.TS, "partial": false,
				"room": RoomView{RoomID: "room-1842", OnlineCount: 5291, Comets: []string{"comet1:8080"}, Active: true},
			})
			return
		}
		if roomID == "room-921" {
			writeJSON(w, http.StatusOK, map[string]any{
				"mock": true, "ts": snap.TS, "partial": false,
				"room": RoomView{RoomID: "room-921", OnlineCount: 3102, Comets: []string{"comet2:8080"}, Active: true},
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"mock": true, "ts": snap.TS, "partial": false, "room": nil})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	rooms, failed := a.col.aggregateRooms(ctx, roomID)
	var rv *RoomView
	if len(rooms) > 0 {
		rv = &rooms[0]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mock":    snap.Mock,
		"ts":      snap.TS,
		"partial": failed > 0,
		"room":    rv, // 不存在（任何 comet 都没有该房间）→ null
	})
}

// ---- Loadtest（兼容 ACTION：全部委托 ExperimentManager——单状态机，见 OPS.md）----

func (a *API) handleLoadtestStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.em.LegacyStatus())
}

func (a *API) handleLoadtestStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var params map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&params) // 空 body 用默认参数
	}
	if err := a.em.LegacyStart(params); err != nil {
		code := http.StatusConflict
		if strings.Contains(err.Error(), "not found") {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"started": true})
}

func (a *API) handleLoadtestStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	if err := a.em.LegacyStop(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stopped": true})
}
