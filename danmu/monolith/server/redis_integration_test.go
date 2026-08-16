//go:build integration

package main

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestShardedPubSubIntegration 需要真实 Redis ≥7（支持 SPUBLISH/SSUBSCRIBE），
// 地址用环境变量 DANMU_TEST_REDIS 指定（如 localhost:6379）。
// 两个 hub 各自持有房间 r1：A 发布 → B 收到完整载荷、A 不回环。
func TestShardedPubSubIntegration(t *testing.T) {
	addr := os.Getenv("DANMU_TEST_REDIS")
	if addr == "" {
		t.Skip("DANMU_TEST_REDIS not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hA := NewHub("srvA", "both", ctx, cancel)
	hB := NewHub("srvB", "both", ctx, cancel)
	cA := registerTestClient(hA, "uA", "r1")
	cB := registerTestClient(hB, "uB", "r1")

	rA, err := NewRedisHub(addr, "", 0, hA, ctx, defaultShardCount, true)
	if err != nil {
		t.Fatalf("hub A: %v", err)
	}
	defer rA.Close()
	rB, err := NewRedisHub(addr, "", 0, hB, ctx, defaultShardCount, true)
	if err != nil {
		t.Fatalf("hub B: %v", err)
	}
	defer rB.Close()

	go rA.SubscribeLoop()
	go rB.SubscribeLoop()
	time.Sleep(200 * time.Millisecond) // 等订阅建立

	payload := `[{"type":"danmu","msg_id":"srvA-1","room_id":"r1","uid":"uA","content":"跨机","source_server":"srvA"}]`
	if err := rA.PublishBatch("r1", []byte(payload)); err != nil {
		t.Fatalf("spublish: %v", err)
	}

	select {
	case got := <-cB.sendCh:
		if string(got) != payload {
			t.Fatalf("B received %s, want %s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B did not receive cross-machine batch")
	}

	// A 不得通过自己的订阅收到自己发布的消息（SourceServer 回环跳过）
	select {
	case got := <-cA.sendCh:
		t.Fatalf("A received own message (loop-back): %s", got)
	case <-time.After(500 * time.Millisecond):
	}
}
