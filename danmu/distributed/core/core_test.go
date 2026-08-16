package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSensitiveFilter(t *testing.T) {
	f := NewSensitiveFilter(DefaultSensitiveWords)
	cases := []struct {
		in     string
		masked bool
	}{
		{"你好世界", false},
		{"办假证快来", true},
		{"正常内容加微信代刷单结尾", true},
		{"", false},
	}
	for _, c := range cases {
		out := f.Filter(c.in)
		has := strings.Contains(out, "*")
		if has != c.masked {
			t.Errorf("Filter(%q)=%q masked=%v want %v", c.in, out, has, c.masked)
		}
		if !c.masked && out != c.in {
			t.Errorf("Filter(%q) 改动了未命中文本: %q", c.in, out)
		}
	}
	// 命中词等长打码
	if got := f.Filter("办假证"); got != "***" {
		t.Errorf("Filter(办假证)=%q want ***", got)
	}
}

func TestTokenIssuerRoundTrip(t *testing.T) {
	ti := NewTokenIssuer("secret-abc")
	tok, exp := ti.Issue("u1", "room-1", time.Minute)
	got, err := ti.Verify(tok, "u1", "room-1")
	if err != nil {
		t.Fatalf("Verify 失败: %v", err)
	}
	if got.Unix() != exp.Unix() {
		t.Errorf("过期时间不一致: %v vs %v", got, exp)
	}
	// 绑定 uid/room 不匹配应拒绝
	if _, err := ti.Verify(tok, "u2", "room-1"); err == nil {
		t.Error("uid 不匹配应拒绝")
	}
	if _, err := ti.Verify(tok, "u1", "room-2"); err == nil {
		t.Error("room 不匹配应拒绝")
	}
	// 篡改签名应拒绝
	if _, err := ti.Verify(tok+"x", "u1", "room-1"); err == nil {
		t.Error("篡改令牌应拒绝")
	}
	// 另一个密钥签发的不认
	other := NewTokenIssuer("secret-xyz")
	if _, err := ti.Verify(mustToken(other, "u1", "room-1"), "u1", "room-1"); err == nil {
		t.Error("异密钥令牌应拒绝")
	}
}

func mustToken(ti *TokenIssuer, uid, room string) string {
	tok, _ := ti.Issue(uid, room, time.Minute)
	return tok
}

func TestTokenBucket(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 5 突发
	allowed := 0
	for i := 0; i < 5; i++ {
		if tb.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("突发应放行 5，实际 %d", allowed)
	}
	if tb.Allow() {
		t.Error("突发耗尽后应拒绝")
	}
}

func TestHubCounterConsistency(t *testing.T) {
	// 回归：CloseRoom/KickClient 删分片条目时必须同步递减原子计数，
	// 否则 OnlineCount/RoomCountFast 永久偏高（旧实现会泄漏计数）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("t1", ctx)

	add := func(uid, room string) {
		c := NewClient(hub, nil, uid, room, ctx)
		hub.AddClient(c)
	}
	add("u1", "r1")
	add("u2", "r1")
	add("u3", "r2")
	if got, want := hub.OnlineCount(), int64(3); got != want {
		t.Fatalf("OnlineCount=%d want %d", got, want)
	}
	if got, want := hub.RoomCountFast(), int64(2); got != want {
		t.Fatalf("RoomCountFast=%d want %d", got, want)
	}
	if got, want := hub.GetConnCount(), 3; got != want {
		t.Fatalf("GetConnCount=%d want %d", got, want)
	}

	// KickClient：删 1 连接，r2 房间空 → 房间数也减
	if !hub.KickClient("r2", "u3") {
		t.Fatal("KickClient(r2,u3) 应成功")
	}
	if got, want := hub.OnlineCount(), int64(2); got != want {
		t.Errorf("kick 后 OnlineCount=%d want %d", got, want)
	}
	if got, want := hub.RoomCountFast(), int64(1); got != want {
		t.Errorf("kick 后 RoomCountFast=%d want %d", got, want)
	}
	if got, want := hub.GetConnCount(), 2; got != want {
		t.Errorf("kick 后 GetConnCount=%d want %d", got, want)
	}

	// CloseRoom：整个房间关闭 → 连接数与房间数同步减
	if !hub.CloseRoom("r1") {
		t.Fatal("CloseRoom(r1) 应成功")
	}
	if got, want := hub.OnlineCount(), int64(0); got != want {
		t.Errorf("close 后 OnlineCount=%d want %d", got, want)
	}
	if got, want := hub.RoomCountFast(), int64(0); got != want {
		t.Errorf("close 后 RoomCountFast=%d want %d", got, want)
	}
	if got, want := hub.GetConnCount(), 0; got != want {
		t.Errorf("close 后 GetConnCount=%d want %d", got, want)
	}

	// 被关连接的 ReadPump 清理路径（RemoveClient）不应重复递减
	for _, uid := range []string{"u1", "u2", "u3"} {
		hub.RemoveClient(&Client{hub: hub, UID: uid, RoomID: "r1"})
		hub.RemoveClient(&Client{hub: hub, UID: uid, RoomID: "r2"})
	}
	if got := hub.OnlineCount(); got != 0 {
		t.Errorf("RemoveClient 空跑后 OnlineCount=%d want 0", got)
	}

	// 顶号：替换不算新增，计数保持不变
	add("u1", "r1")
	if got, want := hub.OnlineCount(), int64(1); got != want {
		t.Errorf("顶号后 OnlineCount=%d want %d", got, want)
	}
	if got, want := hub.RoomCountFast(), int64(1); got != want {
		t.Errorf("顶号后 RoomCountFast=%d want %d", got, want)
	}
	// 被顶旧连接的 RemoveClient 也不该递减
	hub.RemoveClient(&Client{hub: hub, UID: "u1", RoomID: "r1"})
	if got := hub.OnlineCount(); got != 1 {
		t.Errorf("被顶旧连接 RemoveClient 后 OnlineCount=%d want 1", got)
	}
}

// TestWritePumpCloseRace 验证 WritePump 读关闭状态与 Close 并发写之间没有数据竞态
// （-race 下运行）。覆盖两种交错：
//   A. 多个管理面并发 Close（closeOnce 串行化，atomic 发布关闭状态）
//   B. 非 Close 路径先 cancel（模拟 ReadPump defer / 父 ctx 取消唤醒 WritePump），
//      同时管理面并发 Close——这是历史上真实存在的竞态窗口（裸字段读写无同步）。
func TestWritePumpCloseRace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("t-race", ctx)

	for iter := 0; iter < 100; iter++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		c := NewClient(hub, conn, "u1", "r1", ctx)
		go c.WritePump()

		if iter%2 == 0 {
			// 场景 A：管理面并发 Close
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c.Close(4001, "race-test")
				}()
			}
			wg.Wait()
		} else {
			// 场景 B：先由非 Close 路径 cancel（ReadPump defer 语义），再并发 Close
			c.cancel()
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c.Close(4001, "race-test")
				}()
			}
			wg.Wait()
		}

		// 等 WritePump 退出（读到关闭状态后写 CloseMessage）
		select {
		case <-c.ctx.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("WritePump 未在 2s 内退出")
		}
	}
}
