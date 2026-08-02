// Package breaker 实现三态熔断器：Closed → Open → HalfOpen。
//
// 超时保护单次调用，熔断保护整个系统：下游已经死透时，
// 与其让每个请求傻等满超时，不如本地秒拒（fail fast），防止雪崩。
//
// 失败统计用最简单的"连续失败次数"（gobreaker 默认同款）；
// 工业实现会换成滑动窗口错误率（Hystrix/Sentinel），骨架不变。
package breaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	Closed   State = iota // 正常放行，统计连续失败
	Open                  // 全部秒拒，冷却计时中
	HalfOpen              // 只放 1 个探针，其余照拒
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half-open"
	}
}

// ErrOpen 熔断中，请求未发出就被本地拒绝。
var ErrOpen = errors.New("breaker: circuit open")

type Breaker struct {
	threshold int           // 连续失败多少次后熔断
	cooldown  time.Duration // Open 状态的冷却时长

	mu       sync.Mutex
	state    State
	failures int       // Closed 状态下的连续失败计数
	openedAt time.Time // 进入 Open 的时刻
	probing  bool      // HalfOpen 时探针是否已放出
}

func New(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown, state: Closed}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow 在发请求前调用：nil = 放行；ErrOpen = 快速失败，别发了。
// 状态迁移 Open→HalfOpen 发生在这里（冷却期满后的第一个请求变成探针）。
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return nil
	case Open:
		if time.Since(b.openedAt) < b.cooldown {
			return ErrOpen
		}
		// 冷却期满：转半开，本请求就是那个探针
		b.state = HalfOpen
		b.probing = true
		return nil
	default: // HalfOpen
		if b.probing {
			return ErrOpen // 探针还在路上，其余请求继续秒拒
		}
		b.probing = true
		return nil
	}
}

// Success 调用成功后上报。
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	if b.state == HalfOpen {
		// 探针成功 → 下游恢复了，闭合电路
		b.state = Closed
		b.probing = false
	}
}

// Failure 调用失败（连接级错误/超时）后上报。
// 注意：业务错误不应上报到这里——下游能返回业务错误说明它活得好好的。
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		// 探针失败 → 下游还没好，重新熔断并重置冷却计时
		b.state = Open
		b.openedAt = time.Now()
		b.probing = false
	case Closed:
		b.failures++
		if b.failures >= b.threshold {
			b.state = Open
			b.openedAt = time.Now()
			b.failures = 0
		}
	}
}
