package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// testServer wire 一个完整的单体 server（无 Redis/Kafka：验证降级本机广播路径），
// 返回 httptest server 与清理函数。
func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hub := NewHub("srv1", "both", ctx, cancel)
	hub.tokenIssuer = NewTokenIssuer("danmu-secret-token")
	go hub.Run()

	wp := NewWorkerPool(hub)
	wp.Start()

	api := NewAPI(hub, "danmu-secret-token")
	mux := http.NewServeMux()
	api.SetupRoutes(mux)

	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		cancel()
		wp.Wait()
	})
	return srv
}

// dialWS 建立 WS 连接，返回连接。
func dialWS(t *testing.T, srv *httptest.Server, uid, room, token string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") +
		fmt.Sprintf("/ws?uid=%s&room=%s&token=%s", uid, room, token)
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if resp != nil {
			t.Fatalf("ws dial %s: %v (status %d)", wsURL, err, resp.StatusCode)
		}
		t.Fatalf("ws dial %s: %v", wsURL, err)
	}
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	return ws
}

// readDanmu 读取下一条非控制消息（跳过 session_token/reauth_ack/rate_limited）。
func readDanmu(t *testing.T, ws *websocket.Conn) map[string]any {
	t.Helper()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var msgs []map[string]any
		if err := json.Unmarshal(data, &msgs); err != nil {
			var single map[string]any
			if err2 := json.Unmarshal(data, &single); err2 != nil {
				continue
			}
			msgs = []map[string]any{single}
		}
		for _, m := range msgs {
			switch m["type"] {
			case "session_token", "reauth_ack", "rate_limited", "replay_done":
				continue
			}
			return m
		}
	}
}

// TestWSE2EBroadcast 核心 happy path：A 发弹幕 → 同房间 B 收到（无中间件降级本机广播）。
func TestWSE2EBroadcast(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	// 握手后 session_token 可能还在路上，等注册完成
	time.Sleep(100 * time.Millisecond)

	msg := map[string]any{"type": "danmu", "content": "你好，世界！", "client_ts": 12345}
	payload, _ := json.Marshal(msg)
	if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := readDanmu(t, b)
	if got["content"] != "你好，世界！" {
		t.Fatalf("received content = %q, want 你好，世界！", got["content"])
	}
	if got["uid"] != "user-a" {
		t.Fatalf("received uid = %v, want user-a", got["uid"])
	}
	if got["room_id"] != "room-1" {
		t.Fatalf("received room_id = %v, want room-1", got["room_id"])
	}
	if id, _ := got["msg_id"].(string); !strings.HasPrefix(id, "srv1-") {
		t.Fatalf("msg_id = %v, want srv1-* prefix", got["msg_id"])
	}
	if ts, _ := got["client_ts"].(float64); ts != 12345 {
		t.Fatalf("client_ts = %v, want 12345", got["client_ts"])
	}
}

// TestWSSensitiveFilter 敏感词过滤在广播前生效。
func TestWSSensitiveFilter(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)

	payload, _ := json.Marshal(map[string]any{"type": "danmu", "content": "来办假证吗", "client_ts": 1})
	if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	got := readDanmu(t, b)
	if !strings.Contains(got["content"].(string), "***") {
		t.Fatalf("sensitive word not masked: %q", got["content"])
	}
}

// TestWSRejectsBadToken 错误 token 握手应被拒绝。
func TestWSRejectsBadToken(t *testing.T) {
	srv := testServer(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?uid=u1&room=r1&token=wrong"
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		ws.Close()
		t.Fatal("dial with bad token succeeded, want rejection")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token status = %v, want 401", resp)
	}
}

// TestWSUnauthenticatedAPI /api/v1 无 token 应 401。
func TestWSUnauthenticatedAPI(t *testing.T) {
	srv := testServer(t)
	resp, err := http.Get(srv.URL + "/api/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stats without token = %d, want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("stats with token = %d, want 200", resp2.StatusCode)
	}
}

// TestBroadcastAPI 管理员广播经 API 下发，房间内 WS 客户端收到 type=broadcast。
func TestBroadcastAPI(t *testing.T) {
	srv := testServer(t)
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/broadcast",
		strings.NewReader(`{"room_id":"room-1","content":"系统公告"}`))
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("broadcast = %d, want 200", resp.StatusCode)
	}

	got := readDanmu(t, b)
	if got["type"] != "broadcast" || got["content"] != "系统公告" {
		t.Fatalf("broadcast message = %v", got)
	}
}

// TestSessionExpiryDisconnects 会话令牌到期未续期 → 服务端主动断开（4008）。
func TestSessionExpiryDisconnects(t *testing.T) {
	orig := sessionTTL
	sessionTTL = time.Second
	defer func() { sessionTTL = orig }()

	srv := testServer(t)
	ws := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer ws.Close()

	// writePump 的 ping ticker 每 30s 才检查一次到期，这里等连接被服务端关闭
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, _, err := ws.ReadMessage()
		if err == nil {
			continue // 控制消息，继续读
		}
		if closeErr, ok := err.(*websocket.CloseError); ok && closeErr.Code == 4008 {
			break // 会话到期断开，符合预期
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection not closed after session expiry: %v", err)
		}
	}
}

// TestRoomListPagination /api/v1/rooms 分页与计数。
func TestRoomListPagination(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	c := dialWS(t, srv, "user-c", "room-2", "danmu-secret-token")
	defer c.Close()
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/rooms", nil)
	req.Header.Set("Authorization", "Bearer danmu-secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Total int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Total != 2 {
		t.Fatalf("rooms total = %d, want 2", body.Total)
	}
}

// TestWSAckAndIdempotentBroadcast 客户端带 msg_id 发送：服务端回 ack；
// TTL 窗口内重复 msg_id 只广播一次（重试仍回 ack）。
func TestWSAckAndIdempotentBroadcast(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)

	payload, _ := json.Marshal(map[string]any{
		"type": "danmu", "msg_id": "client-abc-1", "content": "幂等测试", "client_ts": 1,
	})
	if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	// A 收到 ack
	gotAck := readDanmu(t, a)
	if gotAck["type"] != "ack" || gotAck["msg_id"] != "client-abc-1" {
		t.Fatalf("ack = %v, want {type:ack msg_id:client-abc-1}", gotAck)
	}
	// B 收到一次广播
	got := readDanmu(t, b)
	if got["content"] != "幂等测试" {
		t.Fatalf("B received %v", got)
	}

	// A 重试同 msg_id：再回 ack，但不再广播。
	// 注意：A 自己消息的广播回声（danmu）可能排在 ack 前，需跳过直到 ack。
	if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("resend: %v", err)
	}
	var gotAck2 map[string]any
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m := readDanmu(t, a)
		if m["type"] == "ack" {
			gotAck2 = m
			break
		}
	}
	if gotAck2 == nil || gotAck2["msg_id"] != "client-abc-1" {
		t.Fatalf("dup ack = %v, want ack for client-abc-1", gotAck2)
	}
	// B 短窗内不应再收到该消息的广播
	b.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, data, err := b.ReadMessage(); err == nil {
		var msgs []map[string]any
		_ = json.Unmarshal(data, &msgs)
		for _, m := range msgs {
			if m["msg_id"] == "client-abc-1" {
				t.Fatalf("duplicate broadcast delivered: %v", m)
			}
		}
	}
}

// TestWSHighPriorityPassthrough 高优先级与置顶字段透传广播。
func TestWSHighPriorityPassthrough(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)

	pinUntil := time.Now().Add(5 * time.Second).UnixMilli()
	payload, _ := json.Marshal(map[string]any{
		"type": "danmu", "msg_id": "sc-1", "content": "醒目留言",
		"priority": 1, "pin_until": pinUntil, "client_ts": 1,
	})
	if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := readDanmu(t, b)
	if got["content"] != "醒目留言" {
		t.Fatalf("B received %v", got)
	}
	if p, _ := got["priority"].(float64); p != 1 {
		t.Fatalf("priority = %v, want 1", got["priority"])
	}
	if pu, _ := got["pin_until"].(float64); int64(pu) != pinUntil {
		t.Fatalf("pin_until = %v, want %d", got["pin_until"], pinUntil)
	}
}

// TestWSReconnectReplay 重连补发：B 带 after_seq=0 进房，应补收到此前 A 发的全部消息
// （replay 帧带 seq 字段，帧尾 replay_done.recovered 计数）。
func TestWSReconnectReplay(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	time.Sleep(100 * time.Millisecond)

	// A 发 3 条，等 worker 批量 flush + 热历史落定
	for _, content := range []string{"one", "two", "three"} {
		payload, _ := json.Marshal(map[string]any{"type": "danmu", "content": content, "client_ts": 1})
		if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	time.Sleep(150 * time.Millisecond)

	// B 首次进房（after_seq=0）→ 拉最近 N 条
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	if err := b.WriteMessage(websocket.TextMessage, []byte(`{"type":"reconnect","after_seq":0}`)); err != nil {
		t.Fatalf("reconnect send: %v", err)
	}

	got := make(map[string]bool)
	recovered := -1
	deadline := time.Now().Add(5 * time.Second)
	for (len(got) < 3 || recovered < 0) && time.Now().Before(deadline) {
		_, data, err := b.ReadMessage()
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var msgs []map[string]any
		if err := json.Unmarshal(data, &msgs); err != nil {
			continue
		}
		for _, m := range msgs {
			switch m["type"] {
			case "danmu":
				if content, ok := m["content"].(string); ok {
					got[content] = true
				}
				if _, ok := m["seq"].(float64); !ok {
					t.Fatalf("replay danmu missing seq: %v", m)
				}
			case "replay_done":
				recovered = int(m["recovered"].(float64))
			}
		}
	}
	if recovered != 3 {
		t.Fatalf("recovered = %d, want 3", recovered)
	}
	if !got["one"] || !got["two"] || !got["three"] {
		t.Fatalf("missing replay messages: %v", got)
	}
}

// TestWSReconnectReplayGapOnly 重连带 after_seq 只补缺口，不重发已收到的消息。
func TestWSReconnectReplayGapOnly(t *testing.T) {
	srv := testServer(t)
	a := dialWS(t, srv, "user-a", "room-1", "danmu-secret-token")
	defer a.Close()
	b := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b.Close()
	time.Sleep(100 * time.Millisecond)

	// A 发 2 条，B 实时收到并记下最大 seq
	var lastSeq uint64
	for _, content := range []string{"first", "second"} {
		payload, _ := json.Marshal(map[string]any{"type": "danmu", "content": content, "client_ts": 1})
		if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Fatalf("send: %v", err)
		}
		got := readDanmu(t, b)
		if seq, ok := got["seq"].(float64); ok && uint64(seq) > lastSeq {
			lastSeq = uint64(seq)
		}
	}

	// B 断开，A 再发 2 条（B 缺席）
	b.Close()
	for _, content := range []string{"third", "fourth"} {
		payload, _ := json.Marshal(map[string]any{"type": "danmu", "content": content, "client_ts": 1})
		if err := a.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	time.Sleep(150 * time.Millisecond)

	// B 重连（同 uid），带 after_seq=lastSeq，只应补收 2 条
	b2 := dialWS(t, srv, "user-b", "room-1", "danmu-secret-token")
	defer b2.Close()
	payload, _ := json.Marshal(map[string]any{"type": "reconnect", "after_seq": lastSeq})
	if err := b2.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("reconnect send: %v", err)
	}

	got := make(map[string]bool)
	recovered := -1
	deadline := time.Now().Add(5 * time.Second)
	for (len(got) < 2 || recovered < 0) && time.Now().Before(deadline) {
		_, data, err := b2.ReadMessage()
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var msgs []map[string]any
		if err := json.Unmarshal(data, &msgs); err != nil {
			continue
		}
		for _, m := range msgs {
			switch m["type"] {
			case "danmu":
				if content, ok := m["content"].(string); ok {
					got[content] = true
				}
			case "replay_done":
				recovered = int(m["recovered"].(float64))
			}
		}
	}
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2 (only the gap)", recovered)
	}
	if !got["third"] || !got["fourth"] {
		t.Fatalf("missing gap messages: %v", got)
	}
	if got["first"] || got["second"] {
		t.Fatalf("replayed already-seen messages: %v", got)
	}
}
