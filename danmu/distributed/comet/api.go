package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/YansIlinta/danmu-distributed/core"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// auth 简单 Bearer 鉴权中间件（恒定时间比较，与 monolith middleware 对齐）。
func (c *comet) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if c.authToken != "" {
			if !strings.HasPrefix(auth, "Bearer ") {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			provided := strings.TrimPrefix(auth, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(provided), []byte(c.authToken)) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		next(w, r)
	}
}

// qpsMW 统计请求数。
func (c *comet) qpsMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.qpsCount.Add(1)
		next.ServeHTTP(w, r)
	})
}

func (c *comet) startQPSTracker(ctx context.Context) {
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cur := c.qpsCount.Load()
				c.lastSecondQPS.Store(cur - last)
				last = cur
			}
		}
	}()
}

// handleTraces 返回本机采样到的 trace span，供 ops 汇聚成跨服务链路。
func (c *comet) handleTraces(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node":  c.id,
		"stats": c.tracer.Stats(),
		"spans": c.tracer.Recent(core.TraceLimit(r)),
	})
}

func (c *comet) handleStats(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	resp := map[string]any{
		"server_id":      c.id,
		"conn_count":     c.hub.OnlineCount(), // O(1) 原子计数，替代 256 分片扫描
		"room_count":     c.hub.RoomCountFast(),
		"qps":            c.lastSecondQPS.Load(),
		"dropped_uplink": c.droppedUplink.Load(),
		"standalone":     c.standalone,
		"heap_mb":        mem.HeapAlloc / 1024 / 1024,
		"goroutines":     runtime.NumGoroutine(),
		"uptime_ms":      time.Since(c.startTime).Milliseconds(),
	}
	// 进程级资源（/proc/self 本进程自身；非 Linux/null）。
	if pr := sampleProcessResource(); pr != nil {
		resp["rss_bytes"] = pr.rssBytes
		resp["cpu_ns"] = pr.cpuNanos
		resp["gc_pause_ns"] = mem.PauseTotalNs
	}
	writeJSON(w, http.StatusOK, resp)
}

func (c *comet) handleRooms(w http.ResponseWriter, r *http.Request) {
	page, limit := parsePagination(r)
	rooms := c.hub.GetRoomList()
	total := len(rooms)
	start := (page - 1) * limit
	end := start + limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "page": page, "limit": limit, "items": rooms[start:end]})
}

func (c *comet) handleSessionToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "only POST"})
		return
	}
	var req struct {
		UID    string `json:"uid"`
		RoomID string `json:"room_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.RoomID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "uid and room_id required"})
		return
	}
	token, exp := c.hub.TokenIssuer.Issue(req.UID, req.RoomID, core.SessionTTL)
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "expires_at": exp.UnixMilli()})
}

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
