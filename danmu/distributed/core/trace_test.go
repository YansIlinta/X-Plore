package core

import (
	"net/http/httptest"
	"testing"
)

// 采样必须是 msg_id 的纯函数：不同进程（不同 node 名、不同缓冲大小）对同一条消息
// 必须给出相同结论，否则链路永远拼不完整——这是整个 trace 方案的地基。
func TestSampledIsDeterministicAcrossInstances(t *testing.T) {
	logic := NewTraceRecorder("logic1", 10, 64)
	comet := NewTraceRecorder("comet2", 10, 512)

	ids := []string{"logic1-1", "logic1-2", "logic1-3", "logic1-99", "logic1-1000", "comet1-7"}
	for _, id := range ids {
		if logic.Sampled(id) != comet.Sampled(id) {
			t.Fatalf("msg_id %q: logic=%v comet=%v，两端采样结论不一致", id, logic.Sampled(id), comet.Sampled(id))
		}
	}
}

// rate=1 全采，rate=0 全不采（关闭开关）。
func TestSampledRateBoundaries(t *testing.T) {
	all := NewTraceRecorder("n", 1, 8)
	off := NewTraceRecorder("n", 0, 8)
	if !all.Sampled("x-1") {
		t.Error("rate=1 应当全采样")
	}
	if off.Sampled("x-1") {
		t.Error("rate=0 应当关闭采样")
	}
	if off.Enabled() {
		t.Error("rate=0 时 Enabled 应为 false")
	}
	// 关闭时 Record 不该留下任何东西
	off.Record("x-1", HopLogicProduce, "room", "", 1)
	if got := len(off.Recent(0)); got != 0 {
		t.Errorf("关闭状态下仍记录了 %d 条 span", got)
	}
}

// 采样率应当真的稀释流量：1/10 采样下，1000 条里命中数量该在合理区间。
func TestSampledRateRoughlyMatches(t *testing.T) {
	r := NewTraceRecorder("n", 10, 8)
	hit := 0
	for i := 0; i < 1000; i++ {
		if r.Sampled("logic1-" + itoa(i)) {
			hit++
		}
	}
	if hit < 50 || hit > 160 {
		t.Errorf("1/10 采样命中 %d/1000，偏离预期区间太远", hit)
	}
}

// 缓冲必须有界，且溢出要被计数——否则 ops 会把残缺链路当成完整结论。
func TestRecorderIsBoundedAndCountsDrops(t *testing.T) {
	const size = 4
	r := NewTraceRecorder("n", 1, size)
	for i := 0; i < size+6; i++ {
		r.Record("id-"+itoa(i), HopJobConsume, "room1", "", int64(i))
	}
	spans := r.Recent(0)
	if len(spans) != size {
		t.Fatalf("缓冲上限 %d，实际留下 %d 条", size, len(spans))
	}
	if r.Dropped() != 6 {
		t.Errorf("应丢弃 6 条，实际计数 %d", r.Dropped())
	}
	// 留下的应当是最新的那批，且时间升序
	if spans[0].MsgID != "id-6" || spans[len(spans)-1].MsgID != "id-9" {
		t.Errorf("留下的不是最新一批：首=%s 尾=%s", spans[0].MsgID, spans[len(spans)-1].MsgID)
	}
}

// Recent(n) 取最近 n 条，超出则返回全部。
func TestRecentLimit(t *testing.T) {
	r := NewTraceRecorder("n", 1, 16)
	for i := 0; i < 5; i++ {
		r.Record("id-"+itoa(i), HopCometUplink, "r", "", int64(i))
	}
	if got := len(r.Recent(2)); got != 2 {
		t.Errorf("Recent(2) 返回 %d 条", got)
	}
	if got := len(r.Recent(99)); got != 5 {
		t.Errorf("Recent(99) 应返回全部 5 条，实际 %d", got)
	}
}

func TestTraceLimitParsing(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", 200},
		{"?limit=10", 10},
		{"?limit=0", 200},
		{"?limit=-5", 200},
		{"?limit=abc", 200},
		{"?limit=99999", 1000},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/api/v1/traces"+c.query, nil)
		if got := TraceLimit(r); got != c.want {
			t.Errorf("TraceLimit(%q) = %d, want %d", c.query, got, c.want)
		}
	}
}

// nil recorder 应当安全：服务未启用 trace 时字段可能为 nil。
func TestNilRecorderIsSafe(t *testing.T) {
	var r *TraceRecorder
	if r.Enabled() {
		t.Error("nil recorder 不该是 enabled")
	}
	if r.Sampled("x") {
		t.Error("nil recorder 不该采样")
	}
	r.Record("x", HopJobPush, "r", "", 1) // 不得 panic
	if len(r.Recent(5)) != 0 {
		t.Error("nil recorder 不该有 span")
	}
	if r.Dropped() != 0 {
		t.Error("nil recorder 丢弃数应为 0")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
