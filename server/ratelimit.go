package main

import (
	"sync/atomic"
	"time"
)

// TokenBucket 无锁令牌桶限流器，使用 atomic + CAS 实现
// 每个连接（按 IP/房间）持有一个实例
//
// tokens 和 lastRefill 打包进同一个 uint64（state）做单次 CAS：
// 高32位 = 当前令牌数*1000（定点数，int32 范围足够容纳 capacity*1000），
// 低32位 = 相对 createdAt 的毫秒偏移（uint32，wraparound 安全——Allow()
// 调用间隔远小于 2^31ms≈24.8天，且 TokenBucket 生命周期等于单个连接，不会长期存活）。
// 拆成两个独立 atomic 分别 CAS 会导致并发 goroutine 各自成功但用了不一致的
// (tokens, lastRefill) 组合，造成补充计算偏移；打包成单个 CAS 后更新是原子的。
type TokenBucket struct {
	state     atomic.Uint64
	createdAt time.Time
	rate      int64 // 每秒补充令牌数
	capacity  int64 // 桶容量
}

func NewTokenBucket(rate, capacity int64) *TokenBucket {
	tb := &TokenBucket{
		createdAt: time.Now(),
		rate:      rate,
		capacity:  capacity,
	}
	tb.state.Store(packState(capacity*1000, 0))
	return tb
}

func packState(tokensFixed int64, elapsedMS uint32) uint64 {
	return uint64(uint32(tokensFixed))<<32 | uint64(elapsedMS)
}

func unpackState(s uint64) (tokensFixed int64, elapsedMS uint32) {
	tokensFixed = int64(int32(s >> 32))
	elapsedMS = uint32(s)
	return
}

// Allow 尝试消费一个令牌，返回是否允许。无锁单次 CAS 实现。
func (tb *TokenBucket) Allow() bool {
	maxTokens := tb.capacity * 1000

	for {
		old := tb.state.Load()
		oldTokens, oldElapsedMS := unpackState(old)

		nowElapsedMS := uint32(time.Since(tb.createdAt).Milliseconds())
		deltaMS := nowElapsedMS - oldElapsedMS // uint32 减法天然处理 wraparound

		// 应补充的令牌数（定点数 *1000）：rate(个/秒) * deltaMS(毫秒) = rate * deltaMS / 1000 * 1000
		refill := int64(deltaMS) * tb.rate

		newTokens := oldTokens + refill
		if newTokens > maxTokens {
			newTokens = maxTokens
		}

		if newTokens < 1000 {
			return false
		}
		newTokens -= 1000

		newState := packState(newTokens, nowElapsedMS)
		if tb.state.CompareAndSwap(old, newState) {
			return true
		}
		// CAS 失败，重试
	}
}
