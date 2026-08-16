package main

import (
	"context"
	"testing"
	"time"
)

// newTestClient 构造一个不依赖真实 websocket.Conn 的 Client，
// 供 Hub 分片逻辑的单元测试使用（hub 只操作 roomID/uid/sendCh/cancel）。
func newTestClient(h *Hub, uid, room string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		hub:    h,
		uid:    uid,
		roomID: room,
		sendCh: make(chan []byte, sendChSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

func TestHubAddRemoveClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)

	c1 := newTestClient(h, "u1", "room-a")
	c2 := newTestClient(h, "u2", "room-a")
	c3 := newTestClient(h, "u3", "room-b")

	h.addClient(c1)
	h.addClient(c2)
	h.addClient(c3)
	if got := h.GetConnCount(); got != 3 {
		t.Fatalf("conn count = %d, want 3", got)
	}
	if got := h.GetRoomCount(); got != 2 {
		t.Fatalf("room count = %d, want 2", got)
	}

	h.removeClient(c1)
	h.removeClient(c3)
	if got := h.GetConnCount(); got != 1 {
		t.Fatalf("conn count after remove = %d, want 1", got)
	}
	if got := h.GetRoomCount(); got != 1 {
		t.Fatalf("room count after remove = %d, want 1", got)
	}

	// 房间空后条目应被清理
	if _, ok := h.GetRoomClients("room-b"); ok {
		t.Fatal("room-b should be gone after last client removed")
	}
}

func TestHubDuplicateUIDReplacesOld(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)

	old := newTestClient(h, "u1", "room-a")
	repl := newTestClient(h, "u1", "room-a")
	h.addClient(old)
	h.addClient(repl) // 顶号

	// 旧连接应被 Close（ctx 被 cancel），新连接接管
	select {
	case <-old.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("old client not closed after session replacement")
	}
	uids, ok := h.GetRoomClients("room-a")
	if !ok || len(uids) != 1 || uids[0] != "u1" {
		t.Fatalf("room-a clients = %v (ok=%v), want [u1]", uids, ok)
	}
	if got := h.GetConnCount(); got != 1 {
		t.Fatalf("conn count = %d, want 1 (replacement is not a new connection)", got)
	}

	// 旧连接退出时不应误删新连接
	h.removeClient(old)
	uids, _ = h.GetRoomClients("room-a")
	if len(uids) != 1 {
		t.Fatalf("old client's unregister removed the replacement: %v", uids)
	}
}

func TestHubBroadcastToRoom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)

	ca := newTestClient(h, "u1", "room-a")
	cb := newTestClient(h, "u2", "room-a")
	other := newTestClient(h, "u3", "room-b")
	h.addClient(ca)
	h.addClient(cb)
	h.addClient(other)

	data := []byte(`[{"type":"danmu"}]`)
	h.BroadcastToRoom("room-a", data)

	// 同房间两个连接都应收到
	for _, c := range []*Client{ca, cb} {
		select {
		case got := <-c.sendCh:
			if string(got) != string(data) {
				t.Fatalf("received %q, want %q", got, data)
			}
		case <-time.After(time.Second):
			t.Fatal("room-a client did not receive broadcast")
		}
	}
	// 其他房间的连接不应收到
	select {
	case <-other.sendCh:
		t.Fatal("client in another room received broadcast")
	case <-time.After(50 * time.Millisecond):
	}

	// 不存在的房间：无 panic、正常返回
	h.BroadcastToRoom("no-such-room", data)
}

func TestHubCloseRoomAndKick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)

	c1 := newTestClient(h, "u1", "room-a")
	c2 := newTestClient(h, "u2", "room-a")
	h.addClient(c1)
	h.addClient(c2)

	if !h.CloseRoom("room-a") {
		t.Fatal("CloseRoom returned false for existing room")
	}
	if h.CloseRoom("room-a") {
		t.Fatal("CloseRoom returned true for missing room")
	}
	if got := h.GetConnCount(); got != 0 {
		t.Fatalf("conn count after close = %d, want 0", got)
	}
	if got := h.GetRoomCount(); got != 0 {
		t.Fatalf("room count after close = %d, want 0", got)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case <-c.ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("closed client ctx not canceled")
		}
	}

	// KickClient
	c3 := newTestClient(h, "u3", "room-b")
	h.addClient(c3)
	if !h.KickClient("room-b", "u3") {
		t.Fatal("KickClient returned false for existing client")
	}
	if h.KickClient("room-b", "u3") {
		t.Fatal("KickClient returned true for already-kicked client")
	}
	if got := h.GetConnCount(); got != 0 {
		t.Fatalf("conn count after kick = %d, want 0", got)
	}
}

// TestHubShardsDisperse 验证房间会分散到多个分片（而不是全部落在同一分片）。
func TestHubShardsDisperse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)

	seen := map[int]bool{}
	for i := 0; i < 512; i++ {
		room := "room-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))
		target := h.shardFor(room)
		idx := 0
		for j, s := range h.shards {
			if s == target {
				idx = j
				break
			}
		}
		seen[idx] = true
	}
	if len(seen) < 8 {
		t.Fatalf("rooms concentrated in too few shards: %d distinct shards", len(seen))
	}
}

func TestHubMsgIDMonotonic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub("srv1", "both", ctx, cancel)
	first := h.nextMsgID()
	second := h.nextMsgID()
	if first == second {
		t.Fatal("msg ids not unique")
	}
	if len(first) < len("srv1-")+1 {
		t.Fatalf("unexpected msg id format: %q", first)
	}
}
