package main

import (
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/YansIlinta/danmu-distributed/core"
)

// job 是靠 logic 写在 Kafka header 里的标记判断"这条要不要记 trace"的。
// 键名一旦对不上，整条链路会从 job 起全部断掉，而且没有任何报错——
// 只会表现为"trace 永远缺后三段"。这个接缝必须钉住。
func TestTraceIDOfReadsLogicHeader(t *testing.T) {
	tracer = core.NewTraceRecorder("job", 100, 64)

	m := kafka.Message{
		Key:     []byte("room1"),
		Value:   []byte(`{"msg_id":"logic1-42"}`),
		Headers: []kafka.Header{{Key: core.TraceHeaderKey, Value: []byte("logic1-42")}},
	}
	if got := traceIDOf(m); got != "logic1-42" {
		t.Errorf("应从 header 取出 msg_id，得到 %q", got)
	}
}

// 未命中采样的消息不带 header，job 必须原样放过——这是热路径上的常态。
func TestTraceIDOfIgnoresUnsampledMessage(t *testing.T) {
	tracer = core.NewTraceRecorder("job", 100, 64)

	m := kafka.Message{Key: []byte("room1"), Value: []byte(`{"msg_id":"logic1-43"}`)}
	if got := traceIDOf(m); got != "" {
		t.Errorf("无 header 应返回空串，得到 %q", got)
	}
}

// 其他 header 不能被误认成 trace 标记。
func TestTraceIDOfIgnoresUnrelatedHeaders(t *testing.T) {
	tracer = core.NewTraceRecorder("job", 100, 64)

	m := kafka.Message{Headers: []kafka.Header{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "x-whatever", Value: []byte("logic1-99")},
	}}
	if got := traceIDOf(m); got != "" {
		t.Errorf("无关 header 不该被当成 trace 标记，得到 %q", got)
	}
}

// trace 关闭时连 header 都不该扫。
func TestTraceIDOfSkipsWhenDisabled(t *testing.T) {
	tracer = core.NewTraceRecorder("job", 0, 64)

	m := kafka.Message{Headers: []kafka.Header{{Key: core.TraceHeaderKey, Value: []byte("logic1-42")}}}
	if got := traceIDOf(m); got != "" {
		t.Errorf("trace 关闭时应返回空串，得到 %q", got)
	}
}
