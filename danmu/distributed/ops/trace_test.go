package ops

import (
	"testing"

	"github.com/YansIlinta/danmu-distributed/core"
)

func span(msgID, hop, node string, ts int64) core.Span {
	return core.Span{MsgID: msgID, Hop: hop, Node: node, TSNano: ts, RoomID: "room1"}
}

// 五个环节齐全 → complete；缺一个 → 报出缺哪个。ops 不能把残缺链路呈现为完整。
func TestTraceCompleteness(t *testing.T) {
	s := newTraceStore()
	s.add(span("m1", core.HopCometUplink, "comet1", 100))
	s.add(span("m1", core.HopLogicProduce, "logic1", 200))
	s.add(span("m1", core.HopJobConsume, "job", 300))
	s.add(span("m1", core.HopJobPush, "job", 400))
	s.add(span("m1", core.HopCometDeliver, "comet1", 500))

	got := s.list(10)
	if len(got) != 1 {
		t.Fatalf("应有 1 条链路，实际 %d", len(got))
	}
	if !got[0].Complete {
		t.Errorf("五环节齐全却判为不完整，missing=%v", got[0].MissingHop)
	}
	if got[0].DurationMS != 0.0004 {
		t.Errorf("耗时应为 (500-100)ns = 0.0004ms，实际 %v", got[0].DurationMS)
	}
	if got[0].RoomID != "room1" {
		t.Errorf("room_id 未继承，得到 %q", got[0].RoomID)
	}
}

func TestTraceMissingHopReported(t *testing.T) {
	s := newTraceStore()
	s.add(span("m1", core.HopCometUplink, "comet1", 100))
	s.add(span("m1", core.HopLogicProduce, "logic1", 200))
	// job 之后全断了：典型的"消息卡在 Kafka 消费"

	got := s.list(1)[0]
	if got.Complete {
		t.Error("缺三个环节却判为完整")
	}
	want := map[string]bool{core.HopJobConsume: true, core.HopJobPush: true, core.HopCometDeliver: true}
	if len(got.MissingHop) != 3 {
		t.Fatalf("应报 3 个缺失环节，实际 %v", got.MissingHop)
	}
	for _, h := range got.MissingHop {
		if !want[h] {
			t.Errorf("报了不该缺的环节 %q", h)
		}
	}
}

// ops 每个采集周期都会把各实例缓冲里的 span 整批拉回来，同一条 span 会被反复看到，
// 必须去重，否则链路里会堆出几十条重复环节。
func TestTraceDedupesRepeatedPolls(t *testing.T) {
	s := newTraceStore()
	for i := 0; i < 5; i++ { // 模拟连拉 5 轮
		s.add(span("m1", core.HopCometUplink, "comet1", 100))
		s.add(span("m1", core.HopLogicProduce, "logic1", 200))
	}
	got := s.list(1)[0]
	if len(got.Spans) != 2 {
		t.Errorf("去重后应剩 2 条 span，实际 %d", len(got.Spans))
	}
}

// 同一环节来自不同 comet 是合法的（job 扇出给每个 comet，各记一条 deliver），不能去掉。
func TestTraceKeepsSameHopFromDifferentNodes(t *testing.T) {
	s := newTraceStore()
	s.add(span("m1", core.HopCometDeliver, "comet1", 500))
	s.add(span("m1", core.HopCometDeliver, "comet2", 510))

	got := s.list(1)[0]
	if len(got.Spans) != 2 {
		t.Fatalf("两个 comet 的投递记录都该保留，实际 %d 条", len(got.Spans))
	}
}

// 汇聚缓冲必须有界，否则 ops 会随运行时长无限吃内存。
func TestTraceStoreEvictsOldest(t *testing.T) {
	s := newTraceStore()
	for i := 0; i < traceMaxKept+30; i++ {
		s.add(span("m"+itoa(i), core.HopCometUplink, "comet1", int64(i)))
	}
	got := s.list(0)
	if len(got) != traceMaxKept {
		t.Fatalf("上限 %d，实际保留 %d", traceMaxKept, len(got))
	}
	// 最新在前
	if got[0].MsgID != "m"+itoa(traceMaxKept+29) {
		t.Errorf("首条应为最新的 msg_id，实际 %q", got[0].MsgID)
	}
	for _, tr := range got {
		if tr.MsgID == "m0" {
			t.Error("最早的 m0 应已被淘汰")
		}
	}
}

// span 到达顺序是乱的（各实例独立轮询），呈现前必须按时间排好。
func TestTraceSpansSortedByTime(t *testing.T) {
	s := newTraceStore()
	s.add(span("m1", core.HopCometDeliver, "comet1", 500))
	s.add(span("m1", core.HopCometUplink, "comet1", 100))
	s.add(span("m1", core.HopJobConsume, "job", 300))

	got := s.list(1)[0]
	for i := 1; i < len(got.Spans); i++ {
		if got.Spans[i-1].TSNano > got.Spans[i].TSNano {
			t.Fatalf("span 未按时间升序：%v", got.Spans)
		}
	}
	if got.Spans[0].Hop != core.HopCometUplink {
		t.Errorf("首个环节应是 %s，实际 %s", core.HopCometUplink, got.Spans[0].Hop)
	}
}

func TestTraceIgnoresEmptySpans(t *testing.T) {
	s := newTraceStore()
	s.add(core.Span{MsgID: "", Hop: core.HopJobPush, Node: "job"})
	s.add(core.Span{MsgID: "m1", Hop: "", Node: "job"})
	if got := len(s.list(0)); got != 0 {
		t.Errorf("空 msg_id / 空 hop 的 span 应被忽略，实际留下 %d 条链路", got)
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
