package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMsgIDSetTTL 幂等窗口：首次 true、窗口内重复 false、过期后重新接受。
func TestMsgIDSetTTL(t *testing.T) {
	s := NewMsgIDSet(40 * time.Millisecond)
	if !s.MarkSeen("r1", "m1") {
		t.Fatal("first MarkSeen should be true")
	}
	if s.MarkSeen("r1", "m1") {
		t.Fatal("dup within TTL should be false")
	}
	if !s.MarkSeen("r1", "m2") {
		t.Fatal("different id should be true")
	}
	if !s.MarkSeen("r2", "m1") {
		t.Fatal("same id in different room should be true")
	}
	time.Sleep(60 * time.Millisecond)
	if !s.MarkSeen("r1", "m1") {
		t.Fatal("after TTL expiry should be true again")
	}
}

// TestMsgIDSetSweep 过期条目与空房间被清理。
func TestMsgIDSetSweep(t *testing.T) {
	s := NewMsgIDSet(20 * time.Millisecond)
	s.MarkSeen("r1", "m1")
	time.Sleep(40 * time.Millisecond)
	s.Sweep(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rooms["r1"]; exists {
		t.Fatal("sweep should remove expired room")
	}
}

// TestHighPrioritySurvivesFullSendCh 普通通道满时普通消息被丢、高优先级走独立通道送达。
func TestHighPrioritySurvivesFullSendCh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("srv1", "both", ctx, cancel)

	client := &Client{uid: "u1", roomID: "r1", sendCh: make(chan []byte, sendChSize), hpSendCh: make(chan []byte, hpSendChSize), cancel: func() {}}
	// 塞满普通通道
	for i := 0; i < sendChSize; i++ {
		client.sendCh <- []byte("filler")
	}
	hub.addClient(client)

	hub.BroadcastToRoom("r1", []byte("[normal]"))
	hub.BroadcastToRoomHigh("r1", []byte("[high]"))

	// 普通消息应被静默丢弃（通道已满，无人消费）
	select {
	case got := <-client.sendCh:
		if string(got) == "[normal]" {
			t.Fatal("normal message should have been dropped (channel full)")
		}
	case <-time.After(50 * time.Millisecond):
	}

	// 高优先级消息应经独立通道送达
	select {
	case got := <-client.hpSendCh:
		if string(got) != "[high]" {
			t.Fatalf("high channel got %s, want [high]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("high priority message not delivered via hpSendCh")
	}
}

// TestHighPriorityFullChannelCountsDrop 高优通道也满时：显式计数（不 panic、不静默）。
func TestHighPriorityFullChannelCountsDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("srv1", "both", ctx, cancel)

	client := &Client{uid: "u1", roomID: "r1", sendCh: make(chan []byte, sendChSize), hpSendCh: make(chan []byte, hpSendChSize), cancel: func() {}}
	for i := 0; i < hpSendChSize; i++ {
		client.hpSendCh <- []byte("filler")
	}
	hub.addClient(client)

	hub.BroadcastToRoomHigh("r1", []byte("[overflow]"))
	// 通道满 → 不被投递（计数器递增由 prometheus 承接，此处仅验证不阻塞不 panic）
	if len(client.hpSendCh) != hpSendChSize {
		t.Fatalf("hpSendCh len = %d, want still full", len(client.hpSendCh))
	}
}

// TestWorkerPartitionsPriority worker flush 按优先级拆两路：高优走 hpSendCh。
func TestWorkerPartitionsPriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub("srv1", "both", ctx, cancel)

	client := &Client{uid: "u1", roomID: "r1", sendCh: make(chan []byte, sendChSize), hpSendCh: make(chan []byte, hpSendChSize), cancel: func() {}}
	hub.addClient(client)

	wp := NewWorkerPool(hub)
	wp.Start() // 统一走 Start：内部 wg.Add 与 worker 的 Done 配对

	normal := acquireMessage()
	normal.Type = "danmu"
	normal.MsgID = "n1"
	normal.RoomID = "r1"
	normal.UID = "u2"
	normal.Content = "normal"
	normal.ServerTS = time.Now().UnixMilli()
	high := acquireMessage()
	high.Type = "danmu"
	high.MsgID = "h1"
	high.RoomID = "r1"
	high.UID = "u2"
	high.Content = "high"
	high.Priority = 1
	high.ServerTS = time.Now().UnixMilli()

	hub.msgQueue <- normal
	hub.msgQueue <- high

	gotNormal := ""
	gotHigh := ""
	for i := 0; i < 2; i++ {
		select {
		case data := <-client.sendCh:
			gotNormal = string(data)
		case data := <-client.hpSendCh:
			gotHigh = string(data)
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting broadcasts")
		}
	}
	if !strings.Contains(gotNormal, "normal") {
		t.Fatalf("normal broadcast missing, got %q (high=%q)", gotNormal, gotHigh)
	}
	if !strings.Contains(gotHigh, "high") {
		t.Fatalf("high broadcast missing, got %q (normal=%q)", gotHigh, gotNormal)
	}
	if strings.Contains(gotNormal, "high") || strings.Contains(gotHigh, "normal") {
		t.Fatalf("batches not partitioned: normal=%q high=%q", gotNormal, gotHigh)
	}
	cancel()
}
