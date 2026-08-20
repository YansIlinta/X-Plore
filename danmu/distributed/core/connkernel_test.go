package core

// Phase 1 验收测试——Connection Kernel（ConnectionManager / SessionIndex /
// SubscriptionIndex）。
//
// 覆盖执行 Prompt「Phase 1 验收测试」要求：
//
//	1. Multi Connection：同一用户 device A + device B 同时在线
//	2. Same Device Multi Connection：同一用户同一设备两个 tab 的行为（默认并存）
//	3. Channel Subscription：两用户订阅同一 channel 都能收到
//	4. Unsubscribe：退订后不再收到
//	5. Disconnect Cleanup：断连后三个索引无脏数据
//	6. Slow Consumer：既有 backpressure（sendCh 满即丢）不回归
//
// 并发与计数一致性叠加在既有 TestHubCounterConsistency / TestWritePumpCloseRace 上。

import (
	"context"
	"testing"
)

// mkHub 构造一个默认 multi-device 策略的 Hub。
func mkHub(t *testing.T) *Hub {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewHub("test-gw", ctx)
}

// addConn 造一条连接并登记：可指定 device / room / uid。
// 返回该连接，便于后续断言 TrySend / Close。
func addConn(t *testing.T, h *Hub, uid, device, room string) *Client {
	t.Helper()
	c := &Client{UID: uid, RoomID: room, DeviceID: device}
	c.initKernelClient(h)
	h.AddClient(c)
	return c
}

// initKernelClient 给裸 Client 补上 sendCh / ctx-cancel，使 TrySend / Close 可断言。
func (c *Client) initKernelClient(h *Hub) {
	if c.hub == nil {
		c.hub = h
	}
	if c.sendCh == nil {
		c.sendCh = make(chan []byte, sendChSize)
	}
	if c.ctx == nil {
		c.ctx, c.cancel = context.WithCancel(h.Context())
	}
}

// ---------------------------------------------------------------------------
// 1. Multi Connection：同一用户 device A + device B 同时在线（US-01）

func TestKernelMultiConnection(t *testing.T) {
	h := mkHub(t)
	ca := addConn(t, h, "u1", "device-A", "r1")
	cb := addConn(t, h, "u1", "device-B", "r1")

	if got, want := h.OnlineCount(), int64(2); got != want {
		t.Fatalf("OnlineCount=%d want %d（两个设备应并存）", got, want)
	}
	if got, want := h.OnlineUserCount(), int64(1); got != want {
		t.Fatalf("OnlineUserCount=%d want %d", got, want)
	}
	if got, want := h.OnlineDeviceCount(), int64(2); got != want {
		t.Fatalf("OnlineDeviceCount=%d want %d", got, want)
	}

	// 两条连接都能被 GetUserConnections 取到（User target 的本机基础）。
	conns := h.GetUserConnections("u1")
	if len(conns) != 2 {
		t.Fatalf("GetUserConnections(u1)=%d want 2", len(conns))
	}
	// 按设备可精确命中。
	if got := h.GetDeviceConnections("u1", "device-A"); len(got) != 1 {
		t.Fatalf("GetDeviceConnections(A)=%d want 1", len(got))
	}
	// 两条连接都能收到 PushUser。
	payload := []byte(`[{"type":"danmu"}]`)
	if got := h.PushUser("u1", payload); got != 2 {
		t.Fatalf("PushUser delivered=%d want 2", got)
	}
	select {
	case <-ca.sendCh:
	default:
		t.Error("device-A 应收到消息")
	}
	select {
	case <-cb.sendCh:
	default:
		t.Error("device-B 应收到消息")
	}
}

// ---------------------------------------------------------------------------
// 2. Same Device Multi Connection：同一用户同一设备的两个 tab 默认并存。
//    行为定义：多设备策略下，同一 (user, device) 的多个连接同时存在，
//    PushUser 给每个连接各投递一份；两条连接的 ConnID 各不相同。

func TestKernelSameDeviceMultiConnection(t *testing.T) {
	h := mkHub(t)
	tab1 := addConn(t, h, "u1", "device-A", "r1")
	tab2 := addConn(t, h, "u1", "device-A", "r1")

	if got, want := h.OnlineCount(), int64(2); got != want {
		t.Fatalf("OnlineCount=%d want %d（同设备两个 tab 并存）", got, want)
	}
	if got, want := h.OnlineDeviceCount(), int64(1); got != want {
		t.Fatalf("OnlineDeviceCount=%d want 1（仍是同一个设备）", got)
	}
	if tab1.ConnectionID == tab2.ConnectionID {
		t.Fatal("两个 tab 的 ConnectionID 必须不同")
	}
	if got := h.GetDeviceConnections("u1", "device-A"); len(got) != 2 {
		t.Fatalf("GetDeviceConnections(A)=%d want 2", len(got))
	}
	if got := h.PushUser("u1", []byte(`[{"ok":1}]`)); got != 2 {
		t.Fatalf("PushUser delivered=%d want 2", got)
	}
}

// ---------------------------------------------------------------------------
// 3. Channel Subscription：两用户订阅同一 channel 都能收到。

func TestKernelChannelSubscription(t *testing.T) {
	h := mkHub(t)
	c1 := addConn(t, h, "u1", "device-A", "live:100")
	c2 := addConn(t, h, "u2", "device-A", "live:100")

	subs := h.subs.GetSubscribers("danmu:room:live:100")
	if len(subs) != 2 {
		t.Fatalf("subscribers=%d want 2", len(subs))
	}
	if got := h.BroadcastToRoom("live:100", []byte(`[{"type":"danmu"}]`)); got != 2 {
		t.Fatalf("BroadcastToRoom delivered=%d want 2", got)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case <-c.sendCh:
		default:
			t.Error("订阅者应收到 channel 消息")
		}
	}
}

// ---------------------------------------------------------------------------
// 4. Unsubscribe：退订后不再收到（断连清理的单一退订语义）。

func TestKernelUnsubscribe(t *testing.T) {
	h := mkHub(t)
	c1 := addConn(t, h, "u1", "device-A", "r1")
	c2 := addConn(t, h, "u2", "device-A", "r1")

	h.subs.Unsubscribe("danmu:room:r1", c1)
	if got := h.BroadcastToRoom("r1", []byte(`[{"type":"danmu"}]`)); got != 1 {
		t.Fatalf("退订一个后 delivered=%d want 1", got)
	}
	select {
	case <-c2.sendCh:
	default:
		t.Error("仍在订阅的 c2 应收到")
	}
	select {
	case <-c1.sendCh:
		t.Error("已退订的 c1 不应再收到")
	default:
	}
}

// ---------------------------------------------------------------------------
// 5. Disconnect Cleanup：连接断开后 ConnectionManager / SessionIndex /
//    SubscriptionIndex 三处都不残留脏数据。

func TestKernelDisconnectCleanup(t *testing.T) {
	h := mkHub(t)
	// 一个用户两个设备，一个用户一个设备，跨两个房间。
	c1 := addConn(t, h, "u1", "device-A", "r1")
	_ = addConn(t, h, "u1", "device-B", "r1")
	_ = addConn(t, h, "u2", "device-A", "r1")
	_ = addConn(t, h, "u2", "device-A", "r2")

	if got, want := h.OnlineCount(), int64(4); got != want {
		t.Fatalf("setup OnlineCount=%d want %d", got, want)
	}
	if got, want := h.RoomCountFast(), int64(2); got != want {
		t.Fatalf("setup RoomCountFast=%d want %d", got, want)
	}

	// 断开 c1（u1/device-A/r1）。
	h.RemoveClient(c1)

	// ConnectionManager：总数减一。
	if got, want := h.OnlineCount(), int64(3); got != want {
		t.Fatalf("cleanup OnlineCount=%d want %d", got, want)
	}
	if got := h.connMan.Get(c1.ConnectionID); got != nil {
		t.Error("ConnectionManager 不应残留已断连接")
	}
	// SessionIndex：u1/device-A 整条链路消失；u1 只剩 device-B。
	if got := h.GetDeviceConnections("u1", "device-A"); len(got) != 0 {
		t.Errorf("SessionIndex 残留 u1/device-A: %d", len(got))
	}
	if got := h.GetDeviceConnections("u1", "device-B"); len(got) != 1 {
		t.Errorf("u1/device-B 应保留 1 条，got %d", len(got))
	}
	// SubscriptionIndex：r1 只剩 2 个订阅者（u1/B + u2/A），r2 保持 1 个。
	if got := len(h.subs.GetSubscribers("danmu:room:r1")); got != 2 {
		t.Errorf("r1 订阅者残留=%d want 2", got)
	}
	if got := len(h.subs.GetSubscribers("danmu:room:r2")); got != 1 {
		t.Errorf("r2 订阅者残留=%d want 1", got)
	}
	// 用户/设备计数：u1 仍在（device-B），u2 仍在。
	if got, want := h.OnlineUserCount(), int64(2); got != want {
		t.Errorf("OnlineUserCount=%d want %d", got, want)
	}
	if got, want := h.OnlineDeviceCount(), int64(2); got != want {
		t.Errorf("OnlineDeviceCount=%d want %d（u1-B + u2-A）", got, want)
	}

	// 最后一位用户全断开 → 用户计数归零。
	h.RemoveClient(&Client{hub: h, UID: "u1", RoomID: "r1", DeviceID: "device-B", ConnectionID: h.GetDeviceConnections("u1", "device-B")[0].ConnectionID})
	h.RemoveClient(&Client{hub: h, UID: "u2", RoomID: "r1", DeviceID: "device-A", ConnectionID: h.GetDeviceConnections("u2", "device-A")[0].ConnectionID})
	h.RemoveClient(&Client{hub: h, UID: "u2", RoomID: "r2", DeviceID: "device-A", ConnectionID: h.GetDeviceConnections("u2", "device-A")[0].ConnectionID})
	if got, want := h.OnlineCount(), int64(0); got != want {
		t.Errorf("全断后 OnlineCount=%d want 0", got)
	}
	if got, want := h.RoomCountFast(), int64(0); got != want {
		t.Errorf("全断后 RoomCountFast=%d want 0", got)
	}
	if got := h.OnlineUserCount(); got != 0 {
		t.Errorf("全断后 OnlineUserCount=%d want 0", got)
	}
	if got := h.OnlineDeviceCount(); got != 0 {
		t.Errorf("全断后 OnlineDeviceCount=%d want 0", got)
	}
}

// ---------------------------------------------------------------------------
// 6. Slow Consumer：sendCh 满时非阻塞丢弃（既有 backpressure，不回归）。
//    同时校验 PushChannel/PushUser 的丢弃计数路径。

func TestKernelSlowConsumerNoRegression(t *testing.T) {
	h := mkHub(t)
	slow := &Client{UID: "u1", RoomID: "r1", DeviceID: "device-A", sendCh: make(chan []byte, 2)}
	slow.initKernelClient(h)
	h.AddClient(slow)

	// 灌满 sendCh（容量 2）。
	slow.TrySend([]byte(`{1}`))
	slow.TrySend([]byte(`{2}`))

	if slow.TrySend([]byte(`{3}`)) {
		t.Error("sendCh 满后 TrySend 必须返回 false（丢弃，不阻塞）")
	}
	// PushChannel / PushUser 也不应 panic / 阻塞，且丢弃计入 metric（MetricDropped 幂等可加）。
	n := h.BroadcastToRoom("r1", []byte(`{4}`))
	if n != 0 {
		t.Errorf("sendCh 满时 BroadcastToRoom 应投递 0，got %d", n)
	}
	if got := h.PushUser("u1", []byte(`{5}`)); got != 0 {
		t.Errorf("sendCh 满时 PushUser 应投递 0，got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 7. 身份属性：ConnectionID/DeviceID/GatewayID 由 Hub 分配并持久稳定。

func TestKernelConnectionIdentity(t *testing.T) {
	h := mkHub(t)
	c := addConn(t, h, "u1", "", "r1") // 未显式 device → 回落默认
	if c.ConnectionID == "" {
		t.Fatal("ConnectionID 必须由 Hub 分配")
	}
	if c.DeviceID != DefaultDeviceID {
		t.Fatalf("DeviceID=%q want %q（未上报回落默认）", c.DeviceID, DefaultDeviceID)
	}
	if c.GatewayID != "test-gw" {
		t.Fatalf("GatewayID=%q want test-gw", c.GatewayID)
	}
	if got := h.connMan.Get(c.ConnectionID); got != c {
		t.Error("ConnectionManager.Get 应能按 ConnectionID 取回同一连接")
	}
}

// ---------------------------------------------------------------------------
// 8. SingleDevicePerUser policy：显式顶号行为保留（旧顶号语义 → 明确策略）。

func TestKernelSingleDevicePolicy(t *testing.T) {
	h := mkHub(t)
	h.ConnectionPolicy = PolicySingleDevicePerUser

	cold := addConn(t, h, "u1", "device-A", "r1")
	cnew := addConn(t, h, "u1", "device-B", "r1") // 新连接顶掉旧连接

	if got, want := h.OnlineCount(), int64(1); got != want {
		t.Fatalf("SingleDevice 策略下 OnlineCount=%d want %d", got, want)
	}
	if cnew.ConnectionID == cold.ConnectionID {
		t.Fatal("新旧连接 ConnectionID 必须不同")
	}
	select {
	case <-cold.ctx.Done():
	default:
		t.Error("旧连接应被顶号关闭")
	}
	// 新连接仍能收到 PushUser。
	payload := []byte(`[{"ok":1}]`)
	if got := h.PushUser("u1", payload); got != 1 {
		t.Fatalf("SingleDevice 下 PushUser delivered=%d want 1", got)
	}
}

// ---------------------------------------------------------------------------
// 9. Room→Channel 兼容映射：旧 room API 语义（CloseRoom/KickClient）不回归。

func TestKernelRoomChannelCompat(t *testing.T) {
	h := mkHub(t)
	_ = addConn(t, h, "u1", "device-A", "r1")
	_ = addConn(t, h, "u1", "device-B", "r1")
	c2 := addConn(t, h, "u2", "device-A", "r2")

	if !h.HasRoom("r1") || h.HasRoom("nope") {
		t.Error("HasRoom 语义错误")
	}
	// KickClient：多设备语义下踢掉该用户在该房间的全部连接。
	if !h.KickClient("r1", "u1") {
		t.Fatal("KickClient(r1,u1) 应成功")
	}
	if got, want := h.OnlineCount(), int64(1); got != want {
		t.Fatalf("kick 后 OnlineCount=%d want %d（只剩 u2/r2）", got, want)
	}
	if got, want := h.RoomCountFast(), int64(1); got != want {
		t.Fatalf("kick 后 RoomCountFast=%d want %d（r1 空房删除）", got, want)
	}
	if !h.CloseRoom("r2") {
		t.Fatal("CloseRoom(r2) 应成功")
	}
	if got, want := h.OnlineCount(), int64(0); got != want {
		t.Fatalf("close 后 OnlineCount=%d want 0", got)
	}
	if got, want := h.RoomCountFast(), int64(0); got != want {
		t.Fatalf("close 后 RoomCountFast=%d want 0", got)
	}
	select {
	case <-c2.ctx.Done():
	default:
		t.Error("被关房间的连接应被 Close")
	}
}
