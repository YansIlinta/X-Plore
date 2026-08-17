package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestWordBankBlock 房间词库 block 模式打码。
func TestWordBankBlock(t *testing.T) {
	wb := NewWordBank("")
	if err := wb.Set("r1", WordEntry{Word: "敏感词", Mode: "block"}); err != nil {
		t.Fatal(err)
	}
	masked, flagged := wb.Apply("r1", "这是敏感词内容")
	if flagged {
		t.Fatal("block should not flag")
	}
	if !strings.Contains(masked, "***") || strings.Contains(masked, "敏感词") {
		t.Fatalf("block mask failed: %q", masked)
	}
	// 无房间配置：原样返回
	out, _ := wb.Apply("other", "这是敏感词内容")
	if out != "这是敏感词内容" {
		t.Fatalf("other room should be untouched: %q", out)
	}
}

// TestWordBankFlag 房间词库 flag 模式放行但标记。
func TestWordBankFlag(t *testing.T) {
	wb := NewWordBank("")
	if err := wb.Set("r1", WordEntry{Word: "广告", Mode: "flag"}); err != nil {
		t.Fatal(err)
	}
	out, flagged := wb.Apply("r1", "这是广告内容")
	if !flagged {
		t.Fatal("flag should be true")
	}
	if out != "这是广告内容" {
		t.Fatalf("flag should not mask: %q", out)
	}
}

// TestWordBankRegex 正则 block 打码（Go RE2 安全）。
func TestWordBankRegex(t *testing.T) {
	wb := NewWordBank("")
	if err := wb.Set("r1", WordEntry{Word: `微\s*信`, Mode: "block", IsRegex: true}); err != nil {
		t.Fatal(err)
	}
	masked, _ := wb.Apply("r1", "加我微 信")
	if !strings.Contains(masked, "***") {
		t.Fatalf("regex block failed: %q", masked)
	}
}

// TestWordBankRemove 删除词条后不再命中。
func TestWordBankRemove(t *testing.T) {
	wb := NewWordBank("")
	wb.Set("r1", WordEntry{Word: "x", Mode: "block"})
	wb.Remove("r1", "x")
	out, flagged := wb.Apply("r1", "x")
	if out != "x" || flagged {
		t.Fatalf("after remove: out=%q flagged=%v", out, flagged)
	}
}

// TestSlowMode 慢速模式节流。
func TestSlowMode(t *testing.T) {
	s := NewSlowMode()
	s.SetInterval("r1", 100*time.Millisecond)
	if !s.Allow("r1", "u1") {
		t.Fatal("first allow should pass")
	}
	if s.Allow("r1", "u1") {
		t.Fatal("second within interval should fail")
	}
	if !s.Allow("r1", "u2") {
		t.Fatal("different uid should pass")
	}
	time.Sleep(120 * time.Millisecond)
	if !s.Allow("r1", "u1") {
		t.Fatal("after interval should pass")
	}
	// 关闭
	s.SetInterval("r1", 0)
	if !s.Allow("r1", "u1") {
		t.Fatal("after off should pass")
	}
}

// TestRoomBans 封禁 TTL 与重连拒绝。
func TestRoomBans(t *testing.T) {
	b := NewRoomBans()
	if b.IsBanned("r1", "u1") {
		t.Fatal("not banned initially")
	}
	b.Ban("r1", "u1", 50*time.Millisecond)
	if !b.IsBanned("r1", "u1") {
		t.Fatal("should be banned")
	}
	time.Sleep(70 * time.Millisecond)
	if b.IsBanned("r1", "u1") {
		t.Fatal("ban should expire")
	}
}

// TestAdminWordbankAPI admin 词库 CRUD + 广播裁决。
func TestAdminWordbankAPI(t *testing.T) {
	srv := testServer(t)

	// POST 添加 block 词
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/admin/rooms/room-1/wordbank",
		strings.NewReader(`{"word":"违禁","mode":"block"}`))
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST wordbank = %d", resp.StatusCode)
	}

	// GET 列表
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/api/v1/admin/rooms/room-1/wordbank", nil)
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Items []WordEntry `json:"items"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if len(body.Items) != 1 || body.Items[0].Word != "违禁" {
		t.Fatalf("GET wordbank = %+v", body.Items)
	}

	// WS 发弹幕含违禁词 → B 收到打码
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)
	payload, _ := json.Marshal(map[string]any{"type": "danmu", "content": "这是违禁内容", "client_ts": 1})
	a.WriteMessage(websocket.TextMessage, payload)
	got := readDanmu(t, b)
	if !strings.Contains(got["content"].(string), "***") {
		t.Fatalf("block not applied: %v", got["content"])
	}
}

// TestAdminSlowModeAPI 慢速模式经 admin API 生效。
func TestAdminSlowModeAPI(t *testing.T) {
	srv := testServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/admin/rooms/room-1/slow-mode",
		strings.NewReader(`{"seconds":5}`))
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("slow-mode = %d", resp.StatusCode)
	}

	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	time.Sleep(100 * time.Millisecond)

	// 第一条放行
	a.WriteMessage(websocket.TextMessage, []byte(`{"type":"danmu","content":"first","client_ts":1}`))
	// 第二条应被限流
	a.WriteMessage(websocket.TextMessage, []byte(`{"type":"danmu","content":"second","client_ts":1}`))
	// A 应收到 rate_limited
	deadline := time.Now().Add(2 * time.Second)
	gotLimited := false
	for time.Now().Before(deadline) {
		_, data, err := a.ReadMessage()
		if err != nil {
			break
		}
		if strings.Contains(string(data), "rate_limited") {
			gotLimited = true
			break
		}
	}
	if !gotLimited {
		t.Fatal("expected rate_limited after slow-mode violation")
	}
}

// TestAdminKickBan 踢人 + 封禁：握手 403。
func TestAdminKickBan(t *testing.T) {
	srv := testServer(t)
	// 先连上再踢
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/admin/rooms/room-1/kick",
		strings.NewReader(`{"uid":"user-a","ban_seconds":60}`))
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("kick = %d", resp.StatusCode)
	}

	// 被踢后连接应关闭
	a.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := a.ReadMessage(); err == nil {
		// 可能还有残留控制消息；再读一次
		if _, _, err2 := a.ReadMessage(); err2 == nil {
			t.Fatal("kicked client still readable")
		}
	}

	// 禁言期内重连应 403
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?uid=user-a&room=room-1&token=danmu-secret-token"
	ws, resp2, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		ws.Close()
		t.Fatal("banned user dial succeeded, want 403")
	}
	if resp2 == nil || resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("banned dial status = %v, want 403", resp2)
	}
}

// TestCrossMachineKick 跨机踢人：A 踢 → B 上同 uid 连接断开（真实 Redis）。
func TestCrossMachineKick(t *testing.T) {
	// 需要真实 Redis；复用 DANMU_TEST_REDIS 或默认 localhost:6379
	addr := "localhost:6379"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hA := NewHub("srvA", "both", ctx, cancel)
	hB := NewHub("srvB", "both", ctx, cancel)
	// B 上有一个连接
	cB := &Client{uid: "u1", roomID: "r1", sendCh: make(chan []byte, 8), hpSendCh: make(chan []byte, 8), cancel: func() {}}
	// 给 cB 一个可观察的 close
	closed := make(chan struct{})
	cB.cancel = func() { close(closed) }
	hB.addClient(cB)

	rA, err := NewRedisHub(addr, "", 0, hA, ctx, defaultShardCount, true)
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer rA.Close()
	rB, err := NewRedisHub(addr, "", 0, hB, ctx, defaultShardCount, true)
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer rB.Close()
	hA.redisHub = rA
	hB.redisHub = rB
	go rA.SubscribeLoop()
	go rB.SubscribeLoop()
	time.Sleep(200 * time.Millisecond)

	// A 侧踢人（本机无该连接）+ 跨机广播
	apiA := NewAPI(hA, "tok")
	apiA.applyKick("r1", "u1", 30)
	apiA.publishCtrl(ctrlMsg{Type: "kick", RoomID: "r1", UID: "u1", BanSeconds: 30, Origin: "srvA"})

	select {
	case <-closed:
		// B 上的连接被踢
	case <-time.After(3 * time.Second):
		t.Fatal("cross-machine kick did not reach hub B")
	}
	// B 侧记录了封禁
	if !hB.bans.IsBanned("r1", "u1") {
		t.Fatal("ban not recorded on hub B")
	}
}
