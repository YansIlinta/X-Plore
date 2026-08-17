package main

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// API 处理所有 REST 接口和 WebSocket 升级
type API struct {
	hub           *Hub
	upgrader      websocket.Upgrader
	startTime     time.Time
	qpsCount      atomic.Int64 // 累计请求数（qpsMiddleware 累加）
	lastSecondQPS atomic.Int64 // 最近一秒的请求数，由 StartQPSTracker 每秒刷新
	authToken     string
	historyDB     HistoryQuerier // 历史弹幕查询接口（可选）
}

// HistoryQuerier 历史弹幕查询接口
type HistoryQuerier interface {
	Query(roomID string, page, limit int) ([]HistoryItem, int, error)
}

type HistoryItem struct {
	UID     string `json:"uid"`
	Content string `json:"content"`
	TimeMS  int64  `json:"time_ms"`
}

func NewAPI(hub *Hub, authToken string) *API {
	return &API{
		hub:       hub,
		startTime: time.Now(),
		authToken: authToken,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

// SetupRoutes 注册所有路由
func (a *API) SetupRoutes(mux *http.ServeMux) {
	// 无需鉴权
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/ws", a.handleWebSocket)
	mux.Handle("/metrics", promhttp.Handler())

	// 需要鉴权的 API（通过 authMiddleware 包装）
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/stats", a.handleStats)
	apiMux.HandleFunc("/api/v1/rooms", a.handleRooms)
	apiMux.HandleFunc("/api/v1/broadcast", a.handleBroadcast)
	apiMux.HandleFunc("/api/v1/clients", a.handleClients)
	apiMux.HandleFunc("/api/v1/history", a.handleHistory)
	apiMux.HandleFunc("/api/v1/session-token", a.handleSessionToken)

	// 带路径参数的路由需要手动匹配
	apiMux.HandleFunc("/api/v1/rooms/", a.handleRoomByID)
	apiMux.HandleFunc("/api/v1/clients/", a.handleClientByID)
	apiMux.HandleFunc("/api/v1/admin/", a.handleAdmin)

	mux.Handle("/api/", authMiddleware(a.authToken, apiMux))

	// 静态文件
	mux.Handle("/", http.FileServer(http.Dir("web")))
}

// --- 响应工具函数 ---

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":       code,
			"message":    message,
			"request_id": getRequestID(r),
		},
	})
}

// --- 接口实现 ---

// GET /health - 健康检查，无需鉴权
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /ws?uid=&room=&token= - WebSocket 升级入口
func (a *API) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	roomID := r.URL.Query().Get("room")
	token := r.URL.Query().Get("token")

	if uid == "" || roomID == "" {
		http.Error(w, "missing uid or room", http.StatusBadRequest)
		return
	}

	// WebSocket 握手鉴权（恒定时间比较，避免计时侧信道）
	if a.authToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.authToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// 房间封禁检查（禁言期内拒绝握手）
	if a.hub.bans.IsBanned(roomID, uid) {
		http.Error(w, "banned", http.StatusForbidden)
		return
	}

	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[API] websocket upgrade error: %v", err)
		return
	}

	client := NewClient(a.hub, conn, uid, roomID, a.hub.ctx)
	a.hub.register <- client

	go client.writePump()
	go client.readPump()

	// 握手成功后立即下发一个绑定uid+room、限时的会话令牌，客户端需在到期前
	// 通过 /api/v1/session-token 刷新并以 {"type":"reauth"} 消息续期
	if a.hub.tokenIssuer != nil {
		sessionToken, expiresAt := a.hub.tokenIssuer.Issue(uid, roomID, sessionTTL)
		payload, _ := json.Marshal([]map[string]interface{}{{
			"type":       "session_token",
			"token":      sessionToken,
			"expires_at": expiresAt.UnixMilli(),
		}})
		select {
		case client.sendCh <- payload:
		default:
		}
	}
}

// GET /api/v1/stats - 服务器统计
func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET allowed")
		return
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"server_id":  a.hub.serverID,
		"conn_count": a.hub.GetConnCount(),
		"room_count": a.hub.GetRoomCount(),
		"qps":        a.lastSecondQPS.Load(),
		"heap_mb":    mem.HeapAlloc / 1024 / 1024,
		"goroutines": runtime.NumGoroutine(),
		"gc_count":   mem.NumGC,
		"uptime_ms":  time.Since(a.startTime).Milliseconds(),
	})
}

// GET /api/v1/rooms?page=&limit= - 房间列表
func (a *API) handleRooms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET allowed")
		return
	}

	page, limit := parsePagination(r)
	rooms := a.hub.GetRoomList()
	total := len(rooms)

	start := (page - 1) * limit
	end := start + limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total": total,
		"page":  page,
		"limit": limit,
		"items": rooms[start:end],
	})
}

// GET/DELETE /api/v1/rooms/{room_id}
func (a *API) handleRoomByID(w http.ResponseWriter, r *http.Request) {
	// 提取路径参数
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/rooms/")
	roomID := strings.TrimSuffix(path, "/")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_ROOM_ID", "room_id is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// GET /api/v1/rooms/{room_id} - 房间详情
		page, limit := parsePagination(r)
		uids, ok := a.hub.GetRoomClients(roomID)
		if !ok {
			writeError(w, r, http.StatusNotFound, "ROOM_NOT_FOUND", "房间不存在")
			return
		}
		total := len(uids)
		start := (page - 1) * limit
		end := start + limit
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"room_id":      roomID,
			"online_count": total,
			"total":        total,
			"page":         page,
			"limit":        limit,
			"items":        uids[start:end],
		})

	case http.MethodDelete:
		// DELETE /api/v1/rooms/{room_id} - 关闭房间
		if !a.hub.CloseRoom(roomID) {
			writeError(w, r, http.StatusNotFound, "ROOM_NOT_FOUND", "房间不存在")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET/DELETE allowed")
	}
}

// POST /api/v1/broadcast - 管理员广播
func (a *API) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only POST allowed")
		return
	}

	var req struct {
		RoomID  string `json:"room_id"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body")
		return
	}
	if req.Content == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_CONTENT", "content is required")
		return
	}
	if len(req.Content) > 500 {
		writeError(w, r, http.StatusBadRequest, "CONTENT_TOO_LONG", "content exceeds 500 characters")
		return
	}

	sentRooms := 0
	if req.RoomID == "" {
		// 广播到（本机已知的）所有房间。跨机方面：仅覆盖在本机也存在的房间，
		// 只存在于其它 server 的房间收不到 all-broadcast——见 REVIEW.md M2 已知限制。
		for _, room := range a.hub.GetRoomList() {
			a.broadcastRoom(room.RoomID, req.Content)
			sentRooms++
		}
	} else {
		a.broadcastRoom(req.RoomID, req.Content)
		sentRooms = 1
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sent_rooms": sentRooms,
	})
}

// broadcastRoom 生成一条系统广播消息，本机广播 + 经 Redis 跨机广播到其它 server。
// SourceServer=本机 ID，使其它 server 收到后会重播、而本机通过 Redis 回环时会跳过（避免重复）。
func (a *API) broadcastRoom(roomID, content string) {
	msg := &Message{
		Type:         "broadcast",
		MsgID:        a.hub.nextMsgID(),
		RoomID:       roomID,
		UID:          "system",
		Content:      content,
		ServerTS:     time.Now().UnixMilli(),
		SourceServer: a.hub.serverID,
	}
	// 与 worker 批量路径一致：先打号入热历史、再广播，重连补发可覆盖管理员广播
	msg.Seq = a.hub.nextRoomSeq(roomID)
	data, _ := json.Marshal([]*Message{msg})
	a.hub.hist.AppendBatch(roomID, []*Message{msg})
	a.hub.BroadcastToRoom(roomID, data)
	if a.hub.redisHub != nil {
		if err := a.hub.redisHub.PublishBatch(roomID, data); err != nil {
			log.Printf("[API] broadcast redis publish error: %v", err)
		}
	}
}

// GET /api/v1/clients?room=&page=&limit= - 客户端列表
func (a *API) handleClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET allowed")
		return
	}

	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_ROOM", "room parameter is required")
		return
	}

	page, limit := parsePagination(r)
	uids, ok := a.hub.GetRoomClients(roomID)
	if !ok {
		writeError(w, r, http.StatusNotFound, "ROOM_NOT_FOUND", "房间不存在")
		return
	}
	total := len(uids)
	start := (page - 1) * limit
	end := start + limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total": total,
		"page":  page,
		"limit": limit,
		"items": uids[start:end],
	})
}

// DELETE /api/v1/clients/{uid}?room= - 踢出用户
func (a *API) handleClientByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only DELETE allowed")
		return
	}

	uid := strings.TrimPrefix(r.URL.Path, "/api/v1/clients/")
	uid = strings.TrimSuffix(uid, "/")
	roomID := r.URL.Query().Get("room")
	if uid == "" || roomID == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_PARAMS", "uid and room are required")
		return
	}

	if !a.hub.KickClient(roomID, uid) {
		writeError(w, r, http.StatusNotFound, "CLIENT_NOT_FOUND", "用户或房间不存在")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/history?room=&page=&limit= - 历史弹幕
func (a *API) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET allowed")
		return
	}

	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_ROOM", "room parameter is required")
		return
	}

	page, limit := parsePagination(r)

	if a.historyDB == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"total": 0,
			"page":  page,
			"limit": limit,
			"items": []interface{}{},
		})
		return
	}

	items, total, err := a.historyDB.Query(roomID, page, limit)
	if err != nil {
		log.Printf("[API] history query error: %v", err)
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to query history")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total": total,
		"page":  page,
		"limit": limit,
		"items": items,
	})
}

// POST /api/v1/session-token - 刷新 WebSocket 长连接的会话令牌（鉴权续期）
// 客户端持有的静态 Bearer token 用于调用本接口，换取一个短时效、绑定 uid+room
// 的新令牌，再通过已建立的 WebSocket 连接发送 {"type":"reauth","token":"..."}续期
func (a *API) handleSessionToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only POST allowed")
		return
	}
	if a.hub.tokenIssuer == nil {
		writeError(w, r, http.StatusServiceUnavailable, "UNAVAILABLE", "session token issuance not configured")
		return
	}

	var req struct {
		UID    string `json:"uid"`
		RoomID string `json:"room_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.RoomID == "" {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "uid and room_id are required")
		return
	}

	token, expiresAt := a.hub.tokenIssuer.Issue(req.UID, req.RoomID, sessionTTL)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":      token,
		"expires_at": expiresAt.UnixMilli(),
	})
}

// parsePagination 解析分页参数，默认 page=1, limit=20, 最大 limit=100
func parsePagination(r *http.Request) (int, int) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return page, limit
}

// handleAdmin 管理面路由：
//
//	GET/POST/DELETE /api/v1/admin/rooms/{id}/wordbank
//	POST            /api/v1/admin/rooms/{id}/slow-mode
//	POST            /api/v1/admin/rooms/{id}/kick
//	POST            /api/v1/admin/rooms/{id}/close
func (a *API) handleAdmin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	// rooms/{id}/{action}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 3 || parts[0] != "rooms" {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown admin path")
		return
	}
	roomID := parts[1]
	action := parts[2]
	if roomID == "" {
		writeError(w, r, http.StatusBadRequest, "MISSING_ROOM_ID", "room_id is required")
		return
	}

	switch action {
	case "wordbank":
		a.handleAdminWordbank(w, r, roomID)
	case "slow-mode":
		a.handleAdminSlowMode(w, r, roomID)
	case "kick":
		a.handleAdminKick(w, r, roomID)
	case "close":
		a.handleAdminClose(w, r, roomID)
	default:
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "unknown admin action")
	}
}

func (a *API) handleAdminWordbank(w http.ResponseWriter, r *http.Request, roomID string) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"room_id": roomID,
			"items":   a.hub.wordBank.RoomWords(roomID),
		})
	case http.MethodPost:
		var e WordEntry
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body")
			return
		}
		if err := a.hub.wordBank.Set(roomID, e); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_ENTRY", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		word := r.URL.Query().Get("word")
		if word == "" {
			writeError(w, r, http.StatusBadRequest, "MISSING_WORD", "word query param is required")
			return
		}
		a.hub.wordBank.Remove(roomID, word)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only GET/POST/DELETE allowed")
	}
}

func (a *API) handleAdminSlowMode(w http.ResponseWriter, r *http.Request, roomID string) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only POST allowed")
		return
	}
	var req struct {
		Seconds int `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body")
		return
	}
	if req.Seconds < 0 {
		writeError(w, r, http.StatusBadRequest, "INVALID_SECONDS", "seconds must be >= 0")
		return
	}
	a.hub.slowMode.SetInterval(roomID, time.Duration(req.Seconds)*time.Second)
	// 跨机同步慢速模式配置
	a.publishCtrl(ctrlMsg{Type: "slow_mode", RoomID: roomID, Seconds: req.Seconds, Origin: a.hub.serverID})
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"room_id": roomID,
		"seconds": req.Seconds,
	})
}

func (a *API) handleAdminKick(w http.ResponseWriter, r *http.Request, roomID string) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only POST allowed")
		return
	}
	var req struct {
		UID        string `json:"uid"`
		BanSeconds int    `json:"ban_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "uid is required")
		return
	}
	// 本机执行
	a.applyKick(roomID, req.UID, req.BanSeconds)
	// 跨机广播（含本机回环：origin 跳过）
	a.publishCtrl(ctrlMsg{
		Type: "kick", RoomID: roomID, UID: req.UID,
		BanSeconds: req.BanSeconds, Origin: a.hub.serverID,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleAdminClose(w http.ResponseWriter, r *http.Request, roomID string) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusBadRequest, "METHOD_NOT_ALLOWED", "only POST allowed")
		return
	}
	a.hub.CloseRoom(roomID)
	a.publishCtrl(ctrlMsg{Type: "close_room", RoomID: roomID, Origin: a.hub.serverID})
	w.WriteHeader(http.StatusNoContent)
}

// applyKick 本机踢人 + 可选封禁。
func (a *API) applyKick(roomID, uid string, banSeconds int) {
	if banSeconds > 0 {
		a.hub.bans.Ban(roomID, uid, time.Duration(banSeconds)*time.Second)
	}
	a.hub.KickClient(roomID, uid)
}

// ctrlMsg 控制面跨机消息（踢人/关房/慢速模式）。
type ctrlMsg struct {
	Type       string `json:"type"` // "kick" | "close_room" | "slow_mode"
	RoomID     string `json:"room_id"`
	UID        string `json:"uid,omitempty"`
	BanSeconds int    `json:"ban_seconds,omitempty"`
	Seconds    int    `json:"seconds,omitempty"` // slow_mode
	Origin     string `json:"origin"`
}

// publishCtrl 经 Redis 控制频道广播跨机控制面动作（无 Redis 时仅本机生效）。
func (a *API) publishCtrl(msg ctrlMsg) {
	if a.hub.redisHub == nil {
		return
	}
	if err := a.hub.redisHub.PublishCtrl(msg); err != nil {
		log.Printf("[API] ctrl publish error: %v", err)
	}
}

// handleCtrl 处理跨机控制面消息（由 RedisHub 在订阅循环中回调）。
func (h *Hub) handleCtrl(msg ctrlMsg) {
	if msg.Origin == h.serverID {
		return // 本机已执行，跳过回环
	}
	switch msg.Type {
	case "kick":
		if msg.BanSeconds > 0 {
			h.bans.Ban(msg.RoomID, msg.UID, time.Duration(msg.BanSeconds)*time.Second)
		}
		h.KickClient(msg.RoomID, msg.UID)
	case "close_room":
		h.CloseRoom(msg.RoomID)
	case "slow_mode":
		h.slowMode.SetInterval(msg.RoomID, time.Duration(msg.Seconds)*time.Second)
	}
}

// QPS 中间件，计数每个请求
func (a *API) qpsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.qpsCount.Add(1)
		next.ServeHTTP(w, r)
	})
}

// wrapMiddleware 组装中间件链
func wrapMiddleware(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// loggingMiddleware 简单请求日志
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[HTTP] %s %s %s reqid=%s", r.Method, r.URL.Path, time.Since(start), getRequestID(r))
	})
}

// corsMiddleware CORS 支持
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// StartQPSTracker 每秒用累计计数差值刷新最近一秒 QPS，供 /api/v1/stats 展示
func (a *API) StartQPSTracker() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-a.hub.ctx.Done():
				return
			case <-ticker.C:
				cur := a.qpsCount.Load()
				a.lastSecondQPS.Store(cur - last)
				last = cur
			}
		}
	}()
}
