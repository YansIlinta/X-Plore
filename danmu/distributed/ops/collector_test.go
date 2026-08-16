package ops

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	c := NewCollector(Config{})
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
	// 同主机多实例：rpc 地址按序配对，不能两个都取第一个
	if svcs[0].Instances[0].RPCAddr != a(s1) || svcs[0].Instances[1].RPCAddr != a(s2) {
		t.Fatalf("rpc pairing: %+v", svcs[0].Instances)
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
		RegistryUp: true,
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
		RegistryUp: true,
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
		RegistryUp: true,
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

	snap = &Snapshot{RegistryUp: false, Services: []Service{{Name: "comet"}}}
	c.evalHealth(snap)
	if snap.Health != healthCritical {
		t.Fatalf("registry down: %s", snap.Health)
	}

	// Kafka 启用但不可用 → critical
	c2 := &Collector{cfg: Config{KafkaBrokers: "localhost:9092"}}
	snap = &Snapshot{RegistryUp: true, Kafka: KafkaInfo{Available: false},
		Services: []Service{{Name: "comet", Instances: []Instance{{HTTPAddr: "a:1", Healthy: true}}}}}
	c2.evalHealth(snap)
	if snap.Health != healthCritical {
		t.Fatalf("kafka down: %s", snap.Health)
	}

	// 空部署：registry 活但无 comet → degraded
	c3 := &Collector{cfg: Config{}}
	snap = &Snapshot{RegistryUp: true}
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
		t.Fatalf("rate magnitude: in=%v, want ~1000-10000 (dt used: %.3fs)", *in, 100/ *in)
	}

	// 计数器重置（重启模拟）→ 本轮 nil
	count.Store(1)
	in, out = c.cometRates(addr)
	if in != nil || out != nil {
		t.Fatalf("counter reset: in=%v out=%v, want nil", in, out)
	}
}
