package main

import (
	"context"
	"testing"
	"time"
)

func mkHistMsg(seq uint64, content string) *Message {
	return &Message{Seq: seq, MsgID: "m-" + content, RoomID: "room-1", UID: "u1", Content: content}
}

// TestRoomHistAppendReplay 基础追加与补发：afterSeq 过滤、latestSeq 返回。
func TestRoomHistAppendReplay(t *testing.T) {
	h := NewRoomHist(100, time.Minute)
	h.AppendBatch("room-1", []*Message{mkHistMsg(1, "a"), mkHistMsg(2, "b"), mkHistMsg(3, "c")})

	msgs, latest := h.ReplayFrom("room-1", 0, 0)
	if latest != 3 {
		t.Fatalf("latest = %d, want 3", latest)
	}
	if len(msgs) != 3 || msgs[0].Seq != 1 || msgs[2].Seq != 3 {
		t.Fatalf("replay = %+v, want seq 1,2,3", msgs)
	}

	msgs, latest = h.ReplayFrom("room-1", 1, 0)
	if latest != 3 {
		t.Fatalf("latest after filter = %d, want 3", latest)
	}
	if len(msgs) != 2 || msgs[0].Seq != 2 || msgs[1].Seq != 3 {
		t.Fatalf("replay after seq 1 = %+v, want seq 2,3", msgs)
	}
}

// TestRoomHistRingOverflow 环形缓冲溢出：只保留最近 max 条。
func TestRoomHistRingOverflow(t *testing.T) {
	h := NewRoomHist(3, time.Minute)
	for i := 1; i <= 5; i++ {
		h.AppendBatch("room-1", []*Message{mkHistMsg(uint64(i), "m")})
	}
	msgs, latest := h.ReplayFrom("room-1", 0, 0)
	if latest != 5 {
		t.Fatalf("latest = %d, want 5", latest)
	}
	if len(msgs) != 3 || msgs[0].Seq != 3 || msgs[2].Seq != 5 {
		t.Fatalf("ring replay = %+v, want seq 3,4,5", msgs)
	}
}

// TestRoomHistTTLExpiry TTL 过期后补发返回空。
func TestRoomHistTTLExpiry(t *testing.T) {
	h := NewRoomHist(100, 30*time.Millisecond)
	h.AppendBatch("room-1", []*Message{mkHistMsg(1, "a")})
	time.Sleep(60 * time.Millisecond)
	msgs, latest := h.ReplayFrom("room-1", 0, 0)
	if len(msgs) != 0 || latest != 0 {
		t.Fatalf("expired replay = %+v latest=%d, want empty", msgs, latest)
	}
}

// TestRoomHistResetAfterExpiry 过期后追加视为新会话，不跨越旧序号。
func TestRoomHistResetAfterExpiry(t *testing.T) {
	h := NewRoomHist(100, 30*time.Millisecond)
	h.AppendBatch("room-1", []*Message{mkHistMsg(1, "old")})
	time.Sleep(60 * time.Millisecond)
	h.AppendBatch("room-1", []*Message{mkHistMsg(2, "new")})
	msgs, latest := h.ReplayFrom("room-1", 0, 0)
	if len(msgs) != 1 || msgs[0].Seq != 2 || latest != 2 {
		t.Fatalf("after-expiry replay = %+v latest=%d, want only seq 2", msgs, latest)
	}
}

// TestRoomHistReplayLimit limit 生效且保留最新部分。
func TestRoomHistReplayLimit(t *testing.T) {
	h := NewRoomHist(100, time.Minute)
	batch := make([]*Message, 10)
	for i := 0; i < 10; i++ {
		batch[i] = mkHistMsg(uint64(i+1), "m")
	}
	h.AppendBatch("room-1", batch)
	msgs, _ := h.ReplayFrom("room-1", 0, 5)
	if len(msgs) != 5 || msgs[4].Seq != 10 {
		t.Fatalf("limited replay = %+v, want latest 5 (seq 6..10)", msgs)
	}
}

// TestRoomHistSkipsZeroSeq Seq==0 的消息不入热历史。
func TestRoomHistSkipsZeroSeq(t *testing.T) {
	h := NewRoomHist(100, time.Minute)
	h.AppendBatch("room-1", []*Message{{Content: "no-seq"}, mkHistMsg(1, "with-seq")})
	msgs, latest := h.ReplayFrom("room-1", 0, 0)
	if latest != 1 || len(msgs) != 1 || msgs[0].Content != "with-seq" {
		t.Fatalf("replay = %+v latest=%d, want only seq 1", msgs, latest)
	}
}

// TestRoomHistSweep SweepLoop 清理过期房间。
func TestRoomHistSweep(t *testing.T) {
	h := NewRoomHist(100, 30*time.Millisecond)
	h.AppendBatch("room-1", []*Message{mkHistMsg(1, "a")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.SweepLoop(ctx, 20*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	h.mu.RLock()
	_, exists := h.rooms["room-1"]
	h.mu.RUnlock()
	if exists {
		t.Fatal("sweep did not remove expired room")
	}
}

// TestHubRoomSeq 房间序号计数器：单调递增、adopt 只增不减。
func TestHubRoomSeq(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("srv1", "both", ctx, cancel)

	if got := hub.nextRoomSeq("r"); got != 1 {
		t.Fatalf("first seq = %d, want 1", got)
	}
	if got := hub.nextRoomSeq("r"); got != 2 {
		t.Fatalf("second seq = %d, want 2", got)
	}
	hub.adoptRoomSeq("r", 10)
	if got := hub.nextRoomSeq("r"); got != 11 {
		t.Fatalf("seq after adopt(10) = %d, want 11", got)
	}
	hub.adoptRoomSeq("r", 5) // 不得回退
	if got := hub.nextRoomSeq("r"); got != 12 {
		t.Fatalf("seq after adopt(5) = %d, want 12", got)
	}
	if got := hub.nextRoomSeq("other"); got != 1 {
		t.Fatalf("other room seq = %d, want 1", got)
	}
}
