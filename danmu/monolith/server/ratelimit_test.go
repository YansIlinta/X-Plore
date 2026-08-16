package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketBurstThenRefill(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 5 令牌容量，10/s 补充

	// 突发消费 5 个令牌
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("burst token %d rejected, want allowed", i)
		}
	}
	if tb.Allow() {
		t.Fatal("6th token within burst accepted, want rejected")
	}

	// 1 秒后应有 ~10 个令牌补充（封顶 5）
	time.Sleep(1100 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("token not refilled after 1s")
	}
}

func TestTokenBucketLongIdleCapsAtCapacity(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	time.Sleep(300 * time.Millisecond) // 空闲 300ms，理论补充 30 个，应封顶 10
	allowed := 0
	for i := 0; i < 20; i++ {
		if tb.Allow() {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("allowed %d after long idle, want capacity 10", allowed)
	}
}

func TestTokenBucketConcurrent(t *testing.T) {
	tb := NewTokenBucket(1000, 100)
	var wg sync.WaitGroup
	var allowed, rejected atomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if tb.Allow() {
					allowed.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	total := allowed.Load() + rejected.Load()
	if total != 3200 {
		t.Fatalf("total outcomes = %d, want 3200", total)
	}
	// 3200 次并发消费不可能全部放行（容量 100、补充有限）
	if allowed.Load() == 3200 {
		t.Fatal("all concurrent calls allowed, rate limit not enforced")
	}
}
