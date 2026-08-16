package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/YansIlinta/danmu-distributed/core"
)

// job 用 gRPC metadata（而不是改 proto）把本批采样的 msg_id 传给 comet。
// metadata 的键会被 gRPC 规范化成小写，键名写错不会报错、只会静默丢掉投递环节，
// 所以这个接缝同样要钉住。
func TestTraceIDsFromCtxReadsJobMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(core.TraceMetadataKey, "logic1-1,logic1-2,logic1-3"))

	got := traceIDsFromCtx(ctx)
	want := []string{"logic1-1", "logic1-2", "logic1-3"}
	if len(got) != len(want) {
		t.Fatalf("应解出 %d 个 msg_id，得到 %v", len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 个 msg_id: 得到 %q, 期望 %q", i, got[i], want[i])
		}
	}
}

func TestTraceIDsFromCtxSingleID(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(core.TraceMetadataKey, "logic1-7"))
	got := traceIDsFromCtx(ctx)
	if len(got) != 1 || got[0] != "logic1-7" {
		t.Errorf("单个 msg_id 解析错误，得到 %v", got)
	}
}

// 没有 metadata（未采样批次，或旧版 job）时必须安全返回空，不能 panic。
func TestTraceIDsFromCtxAbsent(t *testing.T) {
	if got := traceIDsFromCtx(context.Background()); got != nil {
		t.Errorf("无 metadata 应返回 nil，得到 %v", got)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("other-key", "v"))
	if got := traceIDsFromCtx(ctx); got != nil {
		t.Errorf("无关 metadata 应返回 nil，得到 %v", got)
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(core.TraceMetadataKey, ""))
	if got := traceIDsFromCtx(ctx); got != nil {
		t.Errorf("空值应返回 nil，得到 %v", got)
	}
}
