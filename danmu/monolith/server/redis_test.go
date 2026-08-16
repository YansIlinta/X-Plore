package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testRedisHub(t *testing.T, serverID string, sharded bool) (*RedisHub, *Hub) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hub := NewHub(serverID, "both", ctx, cancel)
	return &RedisHub{hub: hub, serverID: serverID, ctx: ctx, shardCount: defaultShardCount, useSharded: sharded}, hub
}

func registerTestClient(h *Hub, uid, roomID string) *Client {
	c := &Client{uid: uid, roomID: roomID, sendCh: make(chan []byte, 16), cancel: func() {}}
	h.addClient(c)
	return c
}

// shardFromChannel 从 "danmu3:mux" 里解析出分片号 3；格式不符返回 -1。
func shardFromChannel(ch string) int {
	if !strings.HasPrefix(ch, shardedChanKey) || !strings.HasSuffix(ch, shardedChanSuffix) {
		return -1
	}
	body := strings.TrimSuffix(strings.TrimPrefix(ch, shardedChanKey), shardedChanSuffix)
	n, err := strconv.Atoi(body)
	if err != nil {
		return -1
	}
	return n
}

// TestRoomChannelSharded 分片频道名：确定性、哈希落片、界内。
func TestRoomChannelSharded(t *testing.T) {
	rh, _ := testRedisHub(t, "srv1", true)
	for _, room := range []string{"room-1", "room-2", "room-3", "room-abc", "房间x"} {
		ch := rh.roomChannel(room)
		if ch != rh.roomChannel(room) {
			t.Fatalf("channel not deterministic for %s", room)
		}
		shard := shardFromChannel(ch)
		if shard < 0 || shard >= defaultShardCount {
			t.Fatalf("channel %q for %s: shard=%d out of range", ch, room, shard)
		}
	}
}

// TestRoomChannelClassic 经典模式频道名 room:{id}。
func TestRoomChannelClassic(t *testing.T) {
	rh, _ := testRedisHub(t, "srv1", false)
	if got := rh.roomChannel("room-9"); got != "room:room-9" {
		t.Fatalf("classic channel = %q, want room:room-9", got)
	}
}

// TestSubscribeChannels 订阅列表：sharded 为 8 个固定复用频道；经典为 room:* pattern。
func TestSubscribeChannels(t *testing.T) {
	rh, _ := testRedisHub(t, "srv1", true)
	chans := rh.subscribeChannels()
	if len(chans) != defaultShardCount {
		t.Fatalf("sharded subscribe channels = %d, want %d", len(chans), defaultShardCount)
	}
	for i, ch := range chans {
		if shard := shardFromChannel(ch); shard != i {
			t.Fatalf("channels[%d] = %q (shard %d), want shard %d", i, ch, shard, i)
		}
	}

	rh2, _ := testRedisHub(t, "srv1", false)
	classic := rh2.subscribeChannels()
	if len(classic) != 1 || classic[0] != "room:*" {
		t.Fatalf("classic subscribe channels = %v, want [room:*]", classic)
	}
}

// TestExtractFirstRoomID 从 payload 扫出第一条消息的 room_id。
func TestExtractFirstRoomID(t *testing.T) {
	cases := []struct {
		payload string
		want    string
	}{
		{`[{"type":"danmu","room_id":"room-1","uid":"u"}]`, "room-1"},
		{`[{"msg_id":"m1","room_id":"r2","content":"x"},{"room_id":"r3"}]`, "r2"},
		{`[{"type":"system"}]`, ""},
		{`garbage`, ""},
	}
	for _, c := range cases {
		if got := extractFirstRoomID([]byte(c.payload)); got != c.want {
			t.Fatalf("extractFirstRoomID(%q) = %q, want %q", c.payload, got, c.want)
		}
	}
}

// TestHandleIncomingShardedMux sharded 复用频道：房间从 payload 取，本机持有则投递。
func TestHandleIncomingShardedMux(t *testing.T) {
	rh, hub := testRedisHub(t, "srv1", true)
	client := registerTestClient(hub, "u1", "r1")

	payload := `[{"type":"danmu","msg_id":"srv2-1","room_id":"r1","uid":"u2","content":"hi","source_server":"srv2"}]`
	rh.handleIncoming("danmu3:mux", payload)

	select {
	case got := <-client.sendCh:
		if string(got) != payload {
			t.Fatalf("delivered %s, want %s", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message not delivered via sharded mux channel")
	}
}

// TestHandleIncomingDropsForeignRoom 本机不持有房间 → 丢弃。
func TestHandleIncomingDropsForeignRoom(t *testing.T) {
	rh, hub := testRedisHub(t, "srv1", true)
	registerTestClient(hub, "u1", "r1")
	payload := `[{"type":"danmu","room_id":"other-room","content":"x"}]`
	rh.handleIncoming("danmu0:mux", payload) // 应静默丢弃，不 panic
}

// TestHandleIncomingLoopAvoid 本机发出的消息（SourceServer==本机）跳过。
func TestHandleIncomingLoopAvoid(t *testing.T) {
	rh, hub := testRedisHub(t, "srv1", true)
	client := registerTestClient(hub, "u1", "r1")
	payload := `[{"type":"danmu","msg_id":"srv1-9","room_id":"r1","uid":"u1","content":"own","source_server":"srv1"}]`
	rh.handleIncoming("danmu1:mux", payload)
	select {
	case got := <-client.sendCh:
		t.Fatalf("loop-back message delivered: %s", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestHandleIncomingClassicChannel 经典 room:{id} 频道路径仍可用。
func TestHandleIncomingClassicChannel(t *testing.T) {
	rh, hub := testRedisHub(t, "srv1", false)
	client := registerTestClient(hub, "u1", "r1")
	payload := `[{"type":"danmu","msg_id":"srv2-2","room_id":"r1","uid":"u2","content":"hi","source_server":"srv2"}]`
	rh.handleIncoming("room:r1", payload)
	select {
	case got := <-client.sendCh:
		if string(got) != payload {
			t.Fatalf("delivered %s, want %s", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message not delivered via classic channel")
	}
}
