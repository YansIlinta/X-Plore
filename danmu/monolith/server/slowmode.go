package main

import (
	"sync"
	"time"
)

// SlowMode 房间慢速模式：roomID -> 每条弹幕的最小发送间隔（0=关闭）。
// 按「房间 × 用户」记录上次发送时间，间隔内再发返回 rate_limited。
// 内存结构：config 为房间间隔配置；last 为 房间×用户 -> 上次发送时间。
type SlowMode struct {
	mu     sync.Mutex
	config map[string]time.Duration
	last   map[string]map[string]time.Time
}

func NewSlowMode() *SlowMode {
	return &SlowMode{
		config: make(map[string]time.Duration),
		last:   make(map[string]map[string]time.Time),
	}
}

// SetInterval 设置房间慢速模式间隔（0 = 关闭）。
func (s *SlowMode) SetInterval(roomID string, d time.Duration) {
	s.mu.Lock()
	if d <= 0 {
		delete(s.config, roomID)
	} else {
		s.config[roomID] = d
	}
	s.mu.Unlock()
}

// Interval 查询房间当前间隔（0 = 关闭）。
func (s *SlowMode) Interval(roomID string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config[roomID]
}

// Allow 放行检查：未开慢速模式或距上次发送已超过间隔 → 放行并记录。
func (s *SlowMode) Allow(roomID, uid string) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	interval := s.config[roomID]
	if interval <= 0 {
		return true
	}
	users := s.last[roomID]
	if users == nil {
		users = make(map[string]time.Time)
		s.last[roomID] = users
	}
	lastAt, seen := users[uid]
	if !seen || now.Sub(lastAt) >= interval {
		users[uid] = now
		return true
	}
	return false
}
