package main

import (
	"sync"
	"time"
)

// MsgIDSet 房间级客户端 msg_id 幂等集合：短 TTL 内抑制客户端重试造成的重复广播。
// 语义：MarkSeen 首次见到返回 true（应广播），TTL 内再次见到返回 false（跳过广播，
// 但客户端仍会收到 ack 作为确认）。过期条目由访问时的惰性清理 + 周期 Sweep 回收。
type MsgIDSet struct {
	ttl   time.Duration
	mu    sync.Mutex
	rooms map[string]*roomMsgIDs
}

type roomMsgIDs struct {
	entries map[string]time.Time
}

func NewMsgIDSet(ttl time.Duration) *MsgIDSet {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &MsgIDSet{ttl: ttl, rooms: make(map[string]*roomMsgIDs)}
}

// MarkSeen 幂等标记：首次出现返回 true；TTL 内重复出现返回 false。
func (s *MsgIDSet) MarkSeen(roomID, msgID string) bool {
	if msgID == "" {
		return true // 无 msg_id 不参与幂等
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rooms[roomID]
	if r == nil {
		r = &roomMsgIDs{entries: make(map[string]time.Time)}
		s.rooms[roomID] = r
	}
	if exp, seen := r.entries[msgID]; seen && now.Before(exp) {
		return false
	}
	r.entries[msgID] = now.Add(s.ttl)
	if len(r.entries)%256 == 0 {
		s.sweepLocked(roomID, r, now) // 周期性惰性清理过期条目
	}
	return true
}

// Sweep 清理所有房间的过期条目与空房间。
func (s *MsgIDSet) Sweep(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.rooms {
		s.sweepLocked(id, r, now)
		if len(r.entries) == 0 {
			delete(s.rooms, id)
		}
	}
}

func (s *MsgIDSet) sweepLocked(roomID string, r *roomMsgIDs, now time.Time) {
	for id, exp := range r.entries {
		if !now.Before(exp) {
			delete(r.entries, id)
		}
	}
}

// Clear 删除房间（关房时调用）。
func (s *MsgIDSet) Clear(roomID string) {
	s.mu.Lock()
	delete(s.rooms, roomID)
	s.mu.Unlock()
}
