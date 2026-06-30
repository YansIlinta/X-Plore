package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// generateRequestID 生成唯一请求 ID
func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// requestIDMiddleware 为每个请求生成 request_id，写进响应头和 context
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := generateRequestID()
		w.Header().Set("X-Request-Id", reqID)
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// getRequestID 从 context 获取 request_id
func getRequestID(r *http.Request) string {
	if id, ok := r.Context().Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// authMiddleware Bearer Token 鉴权中间件
// token 从环境变量读取，不硬编码
func authMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" || !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid authorization")
			return
		}
		provided := strings.TrimPrefix(auth, "Bearer ")
		if provided != token {
			writeError(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "invalid token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
