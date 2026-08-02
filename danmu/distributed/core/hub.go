package core

import (
	"context"
	"sync"
)

const numShards = 256

type roomShard struct {
	mu    sync.RWMutex
	rooms map[string]map[string]*Client
}

// Hub 管理本机所有房间-连接映射，按 roomID 哈希分 256 片，各片独立 RWMutex。
//
// 相对原单体的改动（REVIEW round-2 D1）：去掉 register/unregister channel 和单个
// Run goroutine——AddClient/RemoveClient 本就持分片锁、并发安全，没必要再用一个
// 全局 goroutine 串行化，那是万级并发建连时延迟尖刺的根因。
type Hub struct {
	shards   [numShards]*roomShard
	ServerID string

	// TokenIssuer 用于校验 reauth 令牌（会话续期）。
	TokenIssuer *TokenIssuer
	// Uplink 由 comet 注入：readPump 收到一条弹幕就回调它（转发给 Logic）。
	// offsetMS 为点播进度（毫秒），直播可传 0。
	Uplink func(uid, roomID, content string, clientTS, clientTSNano, offsetMS int64)

	ctx context.Context
}

func NewHub(serverID string, ctx context.Context) *Hub {
	h := &Hub{ServerID: serverID, ctx: ctx}
	for i := range h.shards {
		h.shards[i] = &roomShard{rooms: make(map[string]map[string]*Client)}
	}
	return h
}

func fnv32(s string) uint32 {
	const offset32, prime32 = 2166136261, 16777619
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

func (h *Hub) shardFor(roomID string) *roomShard {
	return h.shards[fnv32(roomID)%numShards]
}

// Context 返回 Hub 的根 context，供 comet 派生每连接的子 context。
func (h *Hub) Context() context.Context { return h.ctx }

// AddClient 直接在分片锁下登记连接（无中间 goroutine）。
func (h *Hub) AddClient(c *Client) {
	shard := h.shardFor(c.RoomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	room, ok := shard.rooms[c.RoomID]
	if !ok {
		room = make(map[string]*Client)
		shard.rooms[c.RoomID] = room
	}
	if old, exists := room[c.UID]; exists {
		old.cancel() // 同 uid 顶号，关旧连接
	}
	room[c.UID] = c
	MetricConnInc()
}

func (h *Hub) RemoveClient(c *Client) {
	shard := h.shardFor(c.RoomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	room, ok := shard.rooms[c.RoomID]
	if !ok {
		return
	}
	if existing, exists := room[c.UID]; exists && existing == c {
		delete(room, c.UID)
		if len(room) == 0 {
			delete(shard.rooms, c.RoomID)
		}
	}
}

// BroadcastToRoom 向房间所有连接非阻塞下发；sendCh 满则丢弃并计数。
// 持锁约束：持 RLock 只做 channel send，不发 RPC。
func (h *Hub) BroadcastToRoom(roomID string, data []byte) int {
	shard := h.shardFor(roomID)
	shard.mu.RLock()
	room, ok := shard.rooms[roomID]
	if !ok {
		shard.mu.RUnlock()
		return 0
	}
	clients := make([]*Client, 0, len(room))
	for _, c := range room {
		clients = append(clients, c)
	}
	shard.mu.RUnlock()

	delivered, dropped := 0, 0
	for _, c := range clients {
		select {
		case c.sendCh <- data:
			delivered++
		default:
			dropped++
		}
	}
	if dropped > 0 {
		MetricDropped(dropped)
	}
	return delivered
}

// HasRoom 本机是否持有该房间（廉价 RLock 读）。
func (h *Hub) HasRoom(roomID string) bool {
	shard := h.shardFor(roomID)
	shard.mu.RLock()
	_, ok := shard.rooms[roomID]
	shard.mu.RUnlock()
	return ok
}

type RoomInfo struct {
	RoomID      string `json:"room_id"`
	OnlineCount int    `json:"online_count"`
	IsActive    bool   `json:"is_active"`
}

func (h *Hub) GetRoomList() []RoomInfo {
	var rooms []RoomInfo
	for _, shard := range h.shards {
		shard.mu.RLock()
		for id, clients := range shard.rooms {
			rooms = append(rooms, RoomInfo{RoomID: id, OnlineCount: len(clients), IsActive: true})
		}
		shard.mu.RUnlock()
	}
	return rooms
}

func (h *Hub) GetRoomClients(roomID string) ([]string, bool) {
	shard := h.shardFor(roomID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	room, ok := shard.rooms[roomID]
	if !ok {
		return nil, false
	}
	uids := make([]string, 0, len(room))
	for uid := range room {
		uids = append(uids, uid)
	}
	return uids, true
}

func (h *Hub) CloseRoom(roomID string) bool {
	shard := h.shardFor(roomID)
	shard.mu.Lock()
	room, ok := shard.rooms[roomID]
	if !ok {
		shard.mu.Unlock()
		return false
	}
	clients := make([]*Client, 0, len(room))
	for _, c := range room {
		clients = append(clients, c)
	}
	delete(shard.rooms, roomID)
	shard.mu.Unlock()
	for _, c := range clients {
		c.Close(4001, "room closed")
	}
	return true
}

func (h *Hub) KickClient(roomID, uid string) bool {
	shard := h.shardFor(roomID)
	shard.mu.Lock()
	room, ok := shard.rooms[roomID]
	if !ok {
		shard.mu.Unlock()
		return false
	}
	c, exists := room[uid]
	if !exists {
		shard.mu.Unlock()
		return false
	}
	delete(room, uid)
	if len(room) == 0 {
		delete(shard.rooms, roomID)
	}
	shard.mu.Unlock()
	c.Close(4001, "kicked")
	return true
}

func (h *Hub) GetConnCount() int {
	count := 0
	for _, shard := range h.shards {
		shard.mu.RLock()
		for _, room := range shard.rooms {
			count += len(room)
		}
		shard.mu.RUnlock()
	}
	return count
}

func (h *Hub) GetRoomCount() int {
	count := 0
	for _, shard := range h.shards {
		shard.mu.RLock()
		count += len(shard.rooms)
		shard.mu.RUnlock()
	}
	return count
}
