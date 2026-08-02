// gateway：统一入口（M3 + M6 硬化）。
//
//	:8088/v1/register|login  -> user  :8001
//	:8088/v1/videos|/v1/me   -> video :8003
//	:8088/ws                 -> comet :8080
//	:8088/health|/ready|/metrics
//
// 环境变量见 deploy/platform/.env.example
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("bad url %q: %v", raw, err)
	}
	return u
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func newProxy(target *url.URL) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)
	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		r.Host = target.Host
		ip := clientIP(r)
		if r.Header.Get("X-Forwarded-For") == "" {
			r.Header.Set("X-Forwarded-For", ip)
		}
		if r.Header.Get("X-Forwarded-Proto") == "" {
			if r.TLS != nil {
				r.Header.Set("X-Forwarded-Proto", "https")
			} else {
				r.Header.Set("X-Forwarded-Proto", "http")
			}
		}
		r.Header.Set("X-Real-IP", ip)
	}
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[gateway] proxy error rid=%s %s %s: %v",
			r.Header.Get("X-Request-Id"), r.Method, r.URL.Path, err)
		http.Error(w, "bad gateway: upstream unavailable", http.StatusBadGateway)
	}
	p.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0, // WebSocket
	}
	return p
}

var (
	reqTotal   atomic.Uint64
	reqLimited atomic.Uint64
)

type rateLimiter struct {
	mu      sync.Mutex
	window  time.Duration
	limit   int
	hits    map[string]*windowCounter
	cleanup time.Time
}

type windowCounter struct {
	count int
	start time.Time
}

func newRateLimiter(perSec int) *rateLimiter {
	if perSec <= 0 {
		return nil
	}
	return &rateLimiter{
		window: time.Second,
		limit:  perSec,
		hits:   make(map[string]*windowCounter),
	}
}

func (rl *rateLimiter) allow(ip string) bool {
	if rl == nil {
		return true
	}
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if now.After(rl.cleanup) {
		for k, v := range rl.hits {
			if now.Sub(v.start) > 2*rl.window {
				delete(rl.hits, k)
			}
		}
		rl.cleanup = now.Add(30 * time.Second)
	}
	c, ok := rl.hits[ip]
	if !ok || now.Sub(c.start) >= rl.window {
		rl.hits[ip] = &windowCounter{count: 1, start: now}
		return true
	}
	if c.count >= rl.limit {
		return false
	}
	c.count++
	return true
}

func parseOrigins(raw string) map[string]bool {
	m := make(map[string]bool)
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			m[p] = true
		}
	}
	return m
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func withMiddleware(next http.Handler, allowedOrigins map[string]bool, rl *rateLimiter, prod bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = newRequestID()
		}
		w.Header().Set("X-Request-Id", rid)
		r.Header.Set("X-Request-Id", rid)

		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		if prod {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}

		origin := r.Header.Get("Origin")
		if len(allowedOrigins) == 0 {
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
		} else if origin != "" && allowedOrigins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		} else if origin != "" && r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, X-Request-Id")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		path := r.URL.Path
		if path != "/health" && path != "/ready" && path != "/metrics" {
			ip := clientIP(r)
			if !rl.allow(ip) {
				reqLimited.Add(1)
				log.Printf("[gateway] rate_limited rid=%s ip=%s path=%s", rid, ip, path)
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
		}

		reqTotal.Add(1)
		next.ServeHTTP(w, r)
		log.Printf("[gateway] rid=%s %s %s %s", rid, r.Method, path, time.Since(start).Truncate(time.Microsecond))
	})
}

func main() {
	addr := env("HTTP_ADDR", ":8088")
	userURL := mustURL(env("USER_HTTP", "http://127.0.0.1:8001"))
	videoURL := mustURL(env("VIDEO_HTTP", "http://127.0.0.1:8003"))
	socialURL := mustURL(env("SOCIAL_HTTP", "http://127.0.0.1:8004"))
	searchURL := mustURL(env("SEARCH_HTTP", "http://127.0.0.1:8005"))
	cometURL := mustURL(env("COMET_HTTP", "http://127.0.0.1:8080"))
	appEnv := env("APP_ENV", "dev")
	prod := appEnv == "prod" || appEnv == "production"
	origins := parseOrigins(env("CORS_ORIGINS", ""))
	rl := newRateLimiter(envInt("RATE_LIMIT_PER_SEC", 100))

	userP := newProxy(userURL)
	videoP := newProxy(videoURL)
	socialP := newProxy(socialURL)
	searchP := newProxy(searchURL)
	cometP := newProxy(cometURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"service":"xplore.gateway"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ready":true}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP xplore_gateway_requests_total Total HTTP requests through middleware\n")
		fmt.Fprintf(w, "# TYPE xplore_gateway_requests_total counter\n")
		fmt.Fprintf(w, "xplore_gateway_requests_total %d\n", reqTotal.Load())
		fmt.Fprintf(w, "# HELP xplore_gateway_rate_limited_total Rate limited requests\n")
		fmt.Fprintf(w, "# TYPE xplore_gateway_rate_limited_total counter\n")
		fmt.Fprintf(w, "xplore_gateway_rate_limited_total %d\n", reqLimited.Load())
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/ws" || strings.HasPrefix(path, "/ws/"):
			cometP.ServeHTTP(w, r)
		case path == "/v1/register" || path == "/v1/login" ||
			strings.HasPrefix(path, "/v1/register/") || strings.HasPrefix(path, "/v1/login/"):
			userP.ServeHTTP(w, r)
		case strings.HasPrefix(path, "/v1/videos") || strings.HasPrefix(path, "/v1/me"):
			videoP.ServeHTTP(w, r)
		case strings.HasPrefix(path, "/v1/users"):
			socialP.ServeHTTP(w, r)
		case strings.HasPrefix(path, "/v1/search"):
			searchP.ServeHTTP(w, r)
		case strings.HasPrefix(path, "/api/") || path == "/healthz":
			cometP.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	log.Printf("[gateway] listen %s env=%s rate=%d/s cors_whitelist=%v",
		addr, appEnv, envInt("RATE_LIMIT_PER_SEC", 100), origins)
	log.Printf("[gateway] user=%s video=%s social=%s search=%s comet=%s",
		userURL, videoURL, socialURL, searchURL, cometURL)

	srv := &http.Server{
		Addr:              addr,
		Handler:           withMiddleware(mux, origins, rl, prod),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
