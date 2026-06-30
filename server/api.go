package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// API 处理所有 REST 接口和 WebSocket 升级
type API struct {
	hub       *Hub
	upgrader  websocket.Upgrader
	startTime time.Time
	qpsCount  atomic.Int64 // 用于 QPS 计算
	authToken string
	historyDB HistoryQuerier // 历史弹幕查询接口（可选）
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

	// 需要鉴权的 API（通过 authMiddleware 包装）
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/stats", a.handleStats)
	apiMux.HandleFunc("/api/v1/rooms", a.handleRooms)
	apiMux.HandleFunc("/api/v1/broadcast", a.handleBroadcast)
	apiMux.HandleFunc("/api/v1/clients", a.handleClients)
	apiMux.HandleFunc("/api/v1/history", a.handleHistory)

	// 带路径参数的路由需要手动匹配
	apiMux.HandleFunc("/api/v1/rooms/", a.handleRoomByID)
	apiMux.HandleFunc("/api/v1/clients/", a.handleClientByID)

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

	// WebSocket 握手鉴权
	if a.authToken != "" && token != a.authToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
		"qps":        a.qpsCount.Load(),
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

	msg := &Message{
		Type:     "broadcast",
		RoomID:   req.RoomID,
		UID:      "system",
		Content:  req.Content,
		ServerTS: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal([]*Message{msg})

	sentRooms := 0
	if req.RoomID == "" {
		rooms := a.hub.GetRoomList()
		for _, room := range rooms {
			a.hub.BroadcastToRoom(room.RoomID, data)
			sentRooms++
		}
	} else {
		a.hub.BroadcastToRoom(req.RoomID, data)
		sentRooms = 1
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sent_rooms": sentRooms,
	})
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

// QPS 计算 goroutine
func (a *API) startQPSCounter() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastCount int64
		for {
			select {
			case <-a.hub.ctx.Done():
				return
			case <-ticker.C:
				current := a.qpsCount.Load()
				_ = current - lastCount
				lastCount = current
			}
		}
	}()
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

// StartQPSTracker 启动 QPS 跟踪
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
				qps := cur - last
				last = cur
				_ = qps
				// QPS 值已在 /api/v1/stats 中暴露
			}
		}
	}()

	// 每秒重置 qps 计数用于 stats 接口展示
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-a.hub.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// QPSValue 获取最近一秒的 QPS
func (a *API) QPSValue() int64 {
	return a.qpsCount.Load()
}

// formatUptime 格式化运行时长
func formatUptime(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh%dm%ds", h, m, s)
}
