package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// eventBuffer 有界：超出容量丢弃最旧。
func TestEventBufferCap(t *testing.T) {
	b := &eventBuffer{}
	for i := 0; i < eventBufferSize+100; i++ {
		b.Add(Event{Message: "x"})
	}
	got := b.Recent(eventBufferSize + 100)
	if len(got) != eventBufferSize {
		t.Fatalf("len=%d, want %d", len(got), eventBufferSize)
	}
}

// Recent 最新在前。
func TestEventBufferRecentOrder(t *testing.T) {
	b := &eventBuffer{}
	b.Add(Event{Message: "first"})
	b.Add(Event{Message: "second"})
	got := b.Recent(10)
	if len(got) != 2 || got[0].Message != "second" || got[1].Message != "first" {
		t.Fatalf("got %+v", got)
	}
}

// takeAddr：不同主机按主机名对上；同主机多实例按序依次配对；无匹配返回空。
func TestTakeAddr(t *testing.T) {
	// 单实例主机：直接对上
	pool := map[string][]string{"comet1": {"comet1:7500"}, "comet2": {"comet2:7500"}}
	if got := takeAddr("comet1:8080", pool); got != "comet1:7500" {
		t.Fatalf("got %q", got)
	}
	// 同主机多实例：按序弹出，两个 http 地址分别拿到两个 rpc 地址
	pool = map[string][]string{"localhost": {"localhost:17500", "localhost:17501"}}
	if got := takeAddr("localhost:17080", pool); got != "localhost:17500" {
		t.Fatalf("first: got %q", got)
	}
	if got := takeAddr("localhost:17081", pool); got != "localhost:17501" {
		t.Fatalf("second: got %q", got)
	}
	// 无匹配：空
	if got := takeAddr("comet1:8080", map[string][]string{"comet2": {"comet2:7500"}}); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

// probeServices 回归：同一组件多实例只能聚成一个 Service 组（曾经 byComp 只查不写，
// 导致 2 个 comet 实例时 /api/services 出现两个 comet 组、overview 实例数翻倍）。
func TestProbeServicesGrouping(t *testing.T) {
	newSrv := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			switch r.URL.Path {
			case "/health":
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			case "/api/v1/stats":
				_, _ = w.Write([]byte(`{"server_id":"` + id + `"}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
	}
	s1, s2, sl := newSrv("comet1"), newSrv("comet2"), newSrv("logic1")
	defer s1.Close()
	defer s2.Close()
	defer sl.Close()
	a := func(s *httptest.Server) string { return s.Listener.Addr().String() }

	c := NewCollector(Config{Discover: func(ctx context.Context) (map[string][]string, error) { return nil, nil }})
	all := map[string][]string{
		"comet":      {a(s1), a(s2)},
		"comet-http": {a(s1), a(s2)},
		"logic":      {a(sl)},
		"logic-http": {a(sl)},
	}
	svcs := c.probeServices(all)
	if len(svcs) != 2 {
		t.Fatalf("groups=%d, want 2: %+v", len(svcs), svcs)
	}
	if svcs[0].Name != "comet" || len(svcs[0].Instances) != 2 {
		t.Fatalf("comet group: %+v", svcs[0])
	}
	if svcs[1].Name != "logic" || len(svcs[1].Instances) != 1 {
		t.Fatalf("logic group: %+v", svcs[1])
	}
	// 同主机多实例：rpc 地址与 http 同序逐一配对（不重复取第一个）。
	// httptest 端口随机，两者字典序一致，故只断言「配对是 {s1,s2} 的置换」。
	got := []string{svcs[0].Instances[0].RPCAddr, svcs[0].Instances[1].RPCAddr}
	if got[0] == "" || got[1] == "" || got[0] == got[1] {
		t.Fatalf("rpc pairing 重复或缺失: %+v", got)
	}
	sort.Strings(got)
	want := []string{a(s1), a(s2)}
	sort.Strings(want)
	if got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rpc pairing: got %+v, want %+v", got, want)
	}
}

func TestAsFloat(t *testing.T) {
	if v := asFloat(json.Number("12.5")); v == nil || *v != 12.5 {
		t.Fatalf("json.Number: %v", v)
	}
	if v := asFloat(float64(3)); v == nil || *v != 3 {
		t.Fatalf("float64: %v", v)
	}
	if v := asFloat(int64(7)); v == nil || *v != 7 {
		t.Fatalf("int64: %v", v)
	}
	if v := asFloat(nil); v != nil {
		t.Fatalf("nil: %v", v)
	}
}

// evalHealth 规则：全 comet 死 → critical；部分实例死 → degraded；全活 → healthy。
func TestEvalHealth(t *testing.T) {
	c := &Collector{cfg: Config{}} // KafkaBrokers 空：不观测 Kafka

	snap := &Snapshot{
		EtcdUp: true,
		Services: []Service{
			{Name: "comet", Instances: []Instance{
				{HTTPAddr: "a:1", Healthy: true},
				{HTTPAddr: "b:1", Healthy: true},
			}},
			{Name: "logic", Instances: []Instance{{HTTPAddr: "l:1", Healthy: true}}},
		},
	}
	c.evalHealth(snap)
	if snap.Health != healthHealthy {
		t.Fatalf("all up: %s", snap.Health)
	}

	snap = &Snapshot{
		EtcdUp: true,
		Services: []Service{
			{Name: "comet", Instances: []Instance{
				{HTTPAddr: "a:1", Healthy: false},
				{HTTPAddr: "b:1", Healthy: true},
			}},
		},
	}
	c.evalHealth(snap)
	if snap.Health != healthDegraded {
		t.Fatalf("one comet down: %s", snap.Health)
	}

	snap = &Snapshot{
		EtcdUp: true,
		Services: []Service{
			{Name: "comet", Instances: []Instance{
				{HTTPAddr: "a:1", Healthy: false},
				{HTTPAddr: "b:1", Healthy: false},
			}},
		},
	}
	c.evalHealth(snap)
	if snap.Health != healthCritical {
		t.Fatalf("all comets down: %s", snap.Health)
	}

	snap = &Snapshot{EtcdUp: false, Services: []Service{{Name: "comet"}}}
	c.evalHealth(snap)
	if snap.Health != healthCritical {
		t.Fatalf("registry down: %s", snap.Health)
	}

	// Kafka 启用但不可用 → critical
	c2 := &Collector{cfg: Config{KafkaBrokers: "localhost:9092"}}
	snap = &Snapshot{EtcdUp: true, Kafka: KafkaInfo{Available: false},
		Services: []Service{{Name: "comet", Instances: []Instance{{HTTPAddr: "a:1", Healthy: true}}}}}
	c2.evalHealth(snap)
	if snap.Health != healthCritical {
		t.Fatalf("kafka down: %s", snap.Health)
	}

	// 空部署：registry 活但无 comet → degraded
	c3 := &Collector{cfg: Config{}}
	snap = &Snapshot{EtcdUp: true}
	c3.evalHealth(snap)
	if snap.Health != healthDegraded {
		t.Fatalf("empty deploy: %s", snap.Health)
	}
}

// cometRates：/metrics 计数器差分 → 速率；首轮与计数器重置返回 nil。
func TestCometRates(t *testing.T) {
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Load()
		_, _ = w.Write([]byte("danmu_messages_total{direction=\"in\"} " + strconv.FormatInt(n, 10) +
			"\ndanmu_messages_total{direction=\"out\"} " + strconv.FormatInt(n*2, 10) + "\n"))
	}))
	defer srv.Close()

	c := NewCollector(Config{})
	addr := srv.Listener.Addr().String() // 生产场景：registry 返回裸 host:port
	in, out := c.cometRates(addr)
	if in != nil || out != nil {
		t.Fatalf("first sample: in=%v out=%v, want nil", in, out)
	}
	count.Add(100)
	time.Sleep(50 * time.Millisecond)
	in, out = c.cometRates(addr)
	if in == nil || out == nil {
		t.Fatalf("second sample: in=%v out=%v, want non-nil", in, out)
	}
	if *in <= 0 || *out <= 0 || *out != *in*2 {
		t.Fatalf("rates: in=%v out=%v", *in, *out)
	}
	// 量级校验：100 条 / 数十毫秒 ≈ 数千/s。dt 若取了零时间（回归 bug），
	// 速率会是 ~1e-9 量级，只有符号和比例检查会漏掉它。
	if *in < 100 || *in > 100000 {
		t.Fatalf("rate magnitude: in=%v, want ~1000-10000 (dt used: %.3fs)", *in, 100 / *in)
	}

	// 计数器重置（重启模拟）→ 本轮 nil
	count.Store(1)
	in, out = c.cometRates(addr)
	if in != nil || out != nil {
		t.Fatalf("counter reset: in=%v out=%v, want nil", in, out)
	}
}

// pollOnce：注入的 Discover 结果 → probeServices → Services；实例探测走真实 HTTP 端点。
func TestPollOnceUsesEtcdDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/stats":
			_, _ = w.Write([]byte(`{"server_id":"comet1"}`))
		default: // /metrics
			_, _ = w.Write([]byte(`danmu_messages_total{direction="in"} 0`))
		}
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	c := NewCollector(Config{
		Discover: func(ctx context.Context) (map[string][]string, error) {
			return map[string][]string{
				"comet":      {addr},
				"comet-http": {addr},
			}, nil
		},
	})
	c.pollOnce(true)

	snap := c.Snapshot()
	if !snap.EtcdUp {
		t.Fatalf("etcd_up=false, detail=%v", snap.HealthDetail)
	}
	if len(snap.Services) != 1 || snap.Services[0].Name != "comet" {
		t.Fatalf("services=%+v", snap.Services)
	}
	insts := snap.Services[0].Instances
	if len(insts) != 1 || !insts[0].Healthy || insts[0].RPCAddr != addr {
		t.Fatalf("instances=%+v", insts)
	}
}

// etcd 掉线：沿用上一轮已知实例清单继续探测，且 EtcdUp=false 判 critical。
func TestPollOnceEtcdDownKeepsLastServices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/stats":
			_, _ = w.Write([]byte(`{"server_id":"comet1"}`))
		}
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	up := true
	c := NewCollector(Config{
		Discover: func(ctx context.Context) (map[string][]string, error) {
			if !up {
				return nil, fmt.Errorf("etcd down")
			}
			return map[string][]string{"comet-http": {addr}}, nil
		},
	})
	c.pollOnce(true)
	up = false
	c.pollOnce(false)

	snap := c.Snapshot()
	if snap.EtcdUp {
		t.Fatalf("etcd_up=true, want false")
	}
	if snap.Health != healthCritical {
		t.Fatalf("health=%s, want critical", snap.Health)
	}
	if len(snap.Services) != 1 || !snap.Services[0].Instances[0].Healthy {
		t.Fatalf("etcd 掉线后应沿用上一轮清单: %+v", snap.Services)
	}
}

// probeServices：同组件多实例只产出一个 Service 分组。
// 回归：去重循环曾误读空 map，≥2 实例时组件整组重复（compose 恰好 2 台 comet，必触发）。
func TestProbeServicesOneGroupPerComponent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/stats":
			_, _ = w.Write([]byte(`{"server_id":"x"}`))
		default: // /metrics
			_, _ = w.Write([]byte(`danmu_messages_total{direction="in"} 0`))
		}
	}))
	defer srv.Close()
	a := srv.Listener.Addr().String()

	c := NewCollector(Config{
		Discover: func(ctx context.Context) (map[string][]string, error) { return nil, nil },
	})
	svcs := c.probeServices(map[string][]string{
		"comet":      {a, a},
		"comet-http": {a, a},
		"logic":      {a, a},
		"logic-http": {a, a},
	})

	names := []string{}
	for _, s := range svcs {
		names = append(names, s.Name)
	}
	if len(svcs) != 2 || names[0] != "comet" || names[1] != "logic" {
		t.Fatalf("services=%v, want [comet logic]", names)
	}
	if len(svcs[0].Instances) != 2 || len(svcs[1].Instances) != 2 {
		t.Fatalf("instances: comet=%d logic=%d, want 2/2",
			len(svcs[0].Instances), len(svcs[1].Instances))
	}
}
