package main

import (
	"sync"
	"time"
)

// RoomBans 房间级封禁：roomID -> uid -> 解禁时间（TTL）。
// 踢人时可带 ban_seconds；禁言期内重连会被握手阶段拒绝（403）。
// 跨机语义：管理员踢人/禁言通过 Redis 控制频道广播，所有 server 本地记录并执行。
type RoomBans struct {
	mu    sync.Mutex
	rooms map[string]map[string]time.Time
}

func NewRoomBans() *RoomBans {
	return &RoomBans{rooms: make(map[string]map[string]time.Time)}
}

// Ban 封禁 room 中的 uid，ttl 秒后自动解禁（ttl<=0 表示仅踢不封）。
// 返回是否本次新封禁。
func (b *RoomBans) Ban(roomID, uid string, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	until := time.Now().Add(ttl)
	b.mu.Lock()
	defer b.mu.Unlock()
	users := b.rooms[roomID]
	if users == nil {
		users = make(map[string]time.Time)
		b.rooms[roomID] = users
	}
	if exp, banned := users[uid]; banned && exp.After(time.Now()) {
		return false
	}
	users[uid] = until
	return true
}

// IsBanned 查询是否在禁言期内（惰性清理过期条目）。
func (b *RoomBans) IsBanned(roomID, uid string) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	users := b.rooms[roomID]
	if users == nil {
		return false
	}
	exp, banned := users[uid]
	if !banned {
		return false
	}
	if exp.Before(now) {
		delete(users, uid)
		return false
	}
	return true
}

// Clear 删除房间全部封禁（关房时调用）。
func (b *RoomBans) Clear(roomID string) {
	b.mu.Lock()
	delete(b.rooms, roomID)
	b.mu.Unlock()
}
