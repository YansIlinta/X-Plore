package core

import (
	"sync/atomic"
	"time"
)

// TokenBucket 无锁令牌桶：tokens(定点*1000) 与相对创建时刻的毫秒偏移打包进一个
// uint64，做单次 CAS 更新，避免拆两个 atomic 各自成功却用了不一致组合的问题。
type TokenBucket struct {
	state     atomic.Uint64
	createdAt time.Time
	rate      int64
	capacity  int64
}

func NewTokenBucket(rate, capacity int64) *TokenBucket {
	tb := &TokenBucket{createdAt: time.Now(), rate: rate, capacity: capacity}
	tb.state.Store(packState(capacity*1000, 0))
	return tb
}

func packState(tokensFixed int64, elapsedMS uint32) uint64 {
	return uint64(uint32(tokensFixed))<<32 | uint64(elapsedMS)
}

func unpackState(s uint64) (tokensFixed int64, elapsedMS uint32) {
	return int64(int32(s >> 32)), uint32(s)
}

// Allow 尝试消费一个令牌，无锁单次 CAS。
func (tb *TokenBucket) Allow() bool {
	maxTokens := tb.capacity * 1000
	for {
		old := tb.state.Load()
		oldTokens, oldElapsedMS := unpackState(old)
		nowElapsedMS := uint32(time.Since(tb.createdAt).Milliseconds())
		deltaMS := nowElapsedMS - oldElapsedMS // uint32 减法天然处理 wraparound
		newTokens := oldTokens + int64(deltaMS)*tb.rate
		if newTokens > maxTokens {
			newTokens = maxTokens
		}
		if newTokens < 1000 {
			return false
		}
		newTokens -= 1000
		if tb.state.CompareAndSwap(old, packState(newTokens, nowElapsedMS)) {
			return true
		}
	}
}
