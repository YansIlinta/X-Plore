// Package registry 实现乞丐版注册中心：HTTP + 内存 map + TTL 租约。
// 它是 etcd 在这个项目里的替身，但机制同源：
//
//	POST /register?service=X&addr=Y   注册/心跳（同一个动作：刷新租约）
//	GET  /services?service=X          拉取存活地址列表（JSON 数组）
//	GET  /services                    无 service 参数时返回全部服务 map（观测用）
//	GET  /health                      {"status":"ok"}（观测端点）
//
// 没有"注销"接口——这是有意的：进程崩溃时来不及注销，
// 所以正确的活性判断只能靠"持续心跳 + 租约过期"，注销接口反而给人虚假安全感。
package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

type Registry struct {
	ttl time.Duration

	mu      sync.Mutex
	entries map[string]map[string]time.Time // service → addr → 租约到期时刻
}

func New(ttl time.Duration) *Registry {
	return &Registry{ttl: ttl, entries: make(map[string]map[string]time.Time)}
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/register":
		r.handleRegister(w, req)
	case "/services":
		r.handleList(w, req)
	case "/health":
		// 观测端点：进程活性。registry 自身无外部依赖，活着即健康。
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	default:
		http.NotFound(w, req)
	}
}

func (r *Registry) handleRegister(w http.ResponseWriter, req *http.Request) {
	service, addr := req.FormValue("service"), req.FormValue("addr")
	if service == "" || addr == "" {
		http.Error(w, "service and addr required", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.entries[service]
	if m == nil {
		m = make(map[string]time.Time)
		r.entries[service] = m
	}
	m[addr] = time.Now().Add(r.ttl)
}

func (r *Registry) handleList(w http.ResponseWriter, req *http.Request) {
	service := req.FormValue("service")
	now := time.Now()

	r.mu.Lock()
	if service == "" {
		// 无 service 参数：返回全部服务的存活地址 map，供观测端（Ops Console）枚举。
		all := make(map[string][]string, len(r.entries))
		for svc, m := range r.entries {
			addrs := make([]string, 0, len(m))
			for addr, expire := range m {
				if now.After(expire) {
					delete(m, addr) // 惰性清理（同下）
					continue
				}
				addrs = append(addrs, addr)
			}
			if len(addrs) > 0 {
				sort.Strings(addrs)
				all[svc] = addrs
			}
		}
		r.mu.Unlock()
		_ = json.NewEncoder(w).Encode(all)
		return
	}

	addrs := make([]string, 0, len(r.entries[service]))
	for addr, expire := range r.entries[service] {
		if now.After(expire) {
			// 惰性清理：读的时候顺手删过期条目，省掉后台清理 goroutine
			delete(r.entries[service], addr)
			continue
		}
		addrs = append(addrs, addr)
	}
	r.mu.Unlock()

	sort.Strings(addrs) // 稳定输出，让 round-robin 的行为可预测
	_ = json.NewEncoder(w).Encode(addrs)
}

// KeepAlive 立刻注册 addr，然后以 ttl/3 的间隔持续续租，直到 ctx 取消。
// 间隔取 ttl/3 而不是贴着 ttl：网络抖动丢一两次心跳，租约也还没过期。
func KeepAlive(ctx context.Context, registryURL, service, addr string, ttl time.Duration) {
	register := func() {
		resp, err := http.PostForm(registryURL+"/register",
			url.Values{"service": {service}, "addr": {addr}})
		if err == nil {
			resp.Body.Close()
		}
	}
	register()
	t := time.NewTicker(ttl / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			register()
		}
	}
}
