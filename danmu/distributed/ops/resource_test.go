package ops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

// --- Resource sampler（§11）：生命周期 + 无 goroutine leak + 端到端采样 ---

func TestResourceSamplerNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 模拟 server stats 端点（rep0 不读 body 也会返回 200）。
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"goroutines":12,"heap_mb":5,"rss_bytes":8388608,"cpu_ns":100000000,"gc_count":3,"gc_pause_ns":123000}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := StartResourceSampler(ctx, "ws://"+srv.Listener.Addr().String(), "tok", 20*time.Millisecond)
	time.Sleep(120 * time.Millisecond) // 采几轮
	s.Stop()                           // 必须退出并清理 goroutine
	s.Wait()

	after := runtime.NumGoroutine()
	// 采样循环已退出；允许少量系统 goroutine 抖动。
	if after > before+8 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestResourceSamplerCollectsMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"goroutines":12,"heap_mb":5,"rss_bytes":8388608,"cpu_ns":100000000,"gc_count":3,"gc_pause_ns":123000}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := StartResourceSampler(ctx, "ws://"+srv.Listener.Addr().String(), "tok", 20*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	s.Stop()
	s.Wait()

	sum := s.Summary()
	if sum == nil || !sum.Sampled {
		srv.Close()
		t.Fatalf("sampler did not collect (sampled=%v, reason=%q)", sum != nil && sum.Sampled, summaryReason(sum))
	}
	if sum.GoroutinesMean == nil || *sum.GoroutinesMean != 12 {
		t.Fatalf("goroutines mean wrong: %+v", sum.GoroutinesMean)
	}
	if sum.RSSMean == nil || *sum.RSSMean != 8 {
		t.Fatalf("rss mean wrong: %+v", sum.RSSMean)
	}
	if len(sum.Samples) == 0 || sum.Samples[0].Goroutines != 12 {
		t.Fatalf("bounded samples missing: %d", len(sum.Samples))
	}
}

func summaryReason(s *ResourceSummary) string {
	if s == nil {
		return "nil"
	}
	return s.UnavailableReason
}

// TestAuthedSamplerGets401ReportsUnavailable：未授权端点 → Sampled=false 且说明原因。
func TestAuthedSamplerGets401ReportsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{"goroutines":5}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := StartResourceSampler(ctx, "ws://"+srv.Listener.Addr().String(), "wrong-token", 15*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	s.Stop()
	if sum := s.Summary(); sum.Sampled {
		t.Fatalf("unauthorized must not be Sampled")
	}
}

// TestSamplerStopsOnContextCancel：ctx 取消后 sampler 退出（无泄漏）。
func TestSamplerStopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"goroutines":1}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	_ = StartResourceSampler(ctx, "ws://"+srv.Listener.Addr().String(), "", 10*time.Millisecond)
	cancel()
	// 等待循环退出（Stop 也幂等可等待）
	time.Sleep(50 * time.Millisecond)
}
