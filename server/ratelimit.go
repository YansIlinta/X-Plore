package main

import (
	"sync/atomic"
	"time"
)

// TokenBucket 无锁令牌桶限流器，使用 atomic + CAS 实现
// 每个连接（按 IP/房间）持有一个实例
type TokenBucket struct {
	tokens     atomic.Int64 // 当前令牌数 * 1000（用定点数避免浮点）
	lastRefill atomic.Int64 // 上次补充时间（UnixNano）
	rate       int64        // 每秒补充令牌数
	capacity   int64        // 桶容量
}

func NewTokenBucket(rate, capacity int64) *TokenBucket {
	tb := &TokenBucket{
		rate:     rate,
		capacity: capacity,
	}
	tb.tokens.Store(capacity * 1000)
	tb.lastRefill.Store(time.Now().UnixNano())
	return tb
}

// Allow 尝试消费一个令牌，返回是否允许。无锁 CAS 实现。
func (tb *TokenBucket) Allow() bool {
	for {
		now := time.Now().UnixNano()
		last := tb.lastRefill.Load()
		elapsed := now - last
		if elapsed < 0 {
			elapsed = 0
		}

		// 计算应补充的令牌数（定点数 *1000）
		refill := (elapsed * tb.rate * 1000) / int64(time.Second)

		oldTokens := tb.tokens.Load()
		newTokens := oldTokens + refill
		maxTokens := tb.capacity * 1000
		if newTokens > maxTokens {
			newTokens = maxTokens
		}

		// 消费一个令牌
		if newTokens < 1000 {
			return false
		}
		newTokens -= 1000

		// CAS 更新
		if tb.tokens.CompareAndSwap(oldTokens, newTokens) {
			tb.lastRefill.Store(now)
			return true
		}
		// CAS 失败，重试
	}
}
