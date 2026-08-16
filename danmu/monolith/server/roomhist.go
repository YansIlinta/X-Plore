package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RoomHist 所有房间的短期热历史（ring buffer + TTL），用于断线重连/进房补发
// 最近 N 条消息。与 ClickHouse 冷历史分层：热层只保留最近 N 条、有 TTL，
// 冷层全量落库（consumer → ClickHouse → /api/v1/history）。
//
// 一致性语义（有意放宽，弹幕允许丢与乱序）：
//   - seq 由「源机」在 worker 批量 flush 时按房间打号，跨机消息经 Redis 到达时
//     其它 server 采纳 payload 里的 seq（只取最大值），因此单房间序号在跨机
//     并发发送时可能重复/乱序——seq 只作为补发缺口提示，去重仍以 msg_id 为准。
//   - 写入顺序是「先入热历史、再实时广播」：补发可见的消息一定不会漏出实时路径
//     （顺序相反时，注册窗口内会存在既没实时投递又没进热历史快照的漏补）。
type RoomHist struct {
	mu    sync.RWMutex
	max   int
	ttl   time.Duration
	rooms map[string]*histRoom
}

// histEntry 热历史单条。Message 为值拷贝：源 Message 对象随后会归还 sync.Pool，
// 值拷贝持有的字符串头仍引用底层字节（Go 字符串不可变），因此安全。
type histEntry struct {
	seq uint64
	msg Message
}

// histRoom 单个房间的热历史环形缓冲
type histRoom struct {
	mu      sync.Mutex
	entries []histEntry
	head    int // entries 写满后环形写入的队首下标；未满时恒为 0
	count   int
	lastSeq uint64
	lastAt  time.Time
}

// reset 清空（保留 entries 底层数组复用，不释放）
func (r *histRoom) reset() {
	r.head = 0
	r.count = 0
	r.lastSeq = 0
}

// NewRoomHist 创建热历史。max<=0 取默认 100，ttl<=0 取默认 5 分钟。
func NewRoomHist(max int, ttl time.Duration) *RoomHist {
	if max <= 0 {
		max = 100
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &RoomHist{max: max, ttl: ttl, rooms: make(map[string]*histRoom)}
}

func (h *RoomHist) getOrCreate(roomID string) *histRoom {
	h.mu.RLock()
	r := h.rooms[roomID]
	h.mu.RUnlock()
	if r != nil {
		return r
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if r = h.rooms[roomID]; r == nil {
		r = &histRoom{}
		h.rooms[roomID] = r
	}
	return r
}

// AppendBatch 追加一批已打序号的消息。Seq==0（未打号）的消息跳过。
// 若距上次写入超过 TTL，先重置——过期房间视为全新会话，补发不跨越。
func (h *RoomHist) AppendBatch(roomID string, msgs []*Message) {
	r := h.getOrCreate(roomID)
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count > 0 && now.Sub(r.lastAt) > h.ttl {
		r.reset()
	}
	for _, m := range msgs {
		if m == nil || m.Seq == 0 {
			continue
		}
		if r.count < h.max {
			// 未满：优先原地复用底层数组（reset 后 count 归零但 entries 仍有
			// 旧长度，不能无条件 append 再拿 len 当 count——会把旧数据算回来）
			if r.count == len(r.entries) {
				r.entries = append(r.entries, histEntry{seq: m.Seq, msg: *m})
			} else {
				r.entries[r.count] = histEntry{seq: m.Seq, msg: *m}
			}
			r.count++
		} else {
			r.entries[r.head] = histEntry{seq: m.Seq, msg: *m}
			r.head = (r.head + 1) % h.max
		}
		if m.Seq > r.lastSeq {
			r.lastSeq = m.Seq
		}
	}
	r.lastAt = now
}

// ReplayFrom 返回 seq > afterSeq 的消息（按 seq 升序，最多 limit 条；limit<=0 用
// h.max 且超出时保留最新部分），以及该房间当前最新 seq。房间不存在或 TTL 已过期
// 返回空。
func (h *RoomHist) ReplayFrom(roomID string, afterSeq uint64, limit int) ([]Message, uint64) {
	if limit <= 0 {
		limit = h.max
	}
	h.mu.RLock()
	r := h.rooms[roomID]
	h.mu.RUnlock()
	if r == nil {
		return nil, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count > 0 && time.Since(r.lastAt) > h.ttl {
		r.reset()
		return nil, 0
	}
	out := make([]Message, 0, r.count)
	for i := 0; i < r.count; i++ {
		e := r.entries[(r.head+i)%h.max]
		if e.seq > afterSeq {
			out = append(out, e.msg)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, r.lastSeq
}

// Clear 删除房间热历史（关房时调用，配合 roomSeqs 计数器一起清理）。
func (h *RoomHist) Clear(roomID string) {
	h.mu.Lock()
	delete(h.rooms, roomID)
	h.mu.Unlock()
}

// SweepLoop 周期性清理 TTL 过期房间，防止房间 churn 导致热历史无限增长。
// interval<=0 取默认 1 分钟。
func (h *RoomHist) SweepLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.mu.Lock()
			for id, r := range h.rooms {
				r.mu.Lock()
				if r.count > 0 && time.Since(r.lastAt) > h.ttl {
					r.reset()
				}
				if r.count == 0 {
					delete(h.rooms, id)
				}
				r.mu.Unlock()
			}
			h.mu.Unlock()
		}
	}
}
