package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

const msgQueueSize = 100000

// numShards 房间分片数。房间通过 roomID 哈希落到某个分片，
// 分片各自持有独立的 RWMutex，避免万级房间场景下所有房间共享一把全局锁——
// 任意房间的 join/leave（Lock）会阻塞所有房间广播的 RLock。
const numShards = 256

// roomShard 单个分片：一部分房间及其连接，由独立的 RWMutex 保护
type roomShard struct {
	mu    sync.RWMutex
	rooms map[string]map[string]*Client
}

// Hub 管理所有房间的连接，内部按 roomID 哈希分片存储，分散锁竞争
type Hub struct {
	shards   [numShards]*roomShard
	serverID string

	register   chan *Client
	unregister chan *Client
	msgQueue   chan *Message // 进程内消息队列，带缓冲 channel 做削峰

	redisHub    *RedisHub
	kafkaProd   *KafkaProducer
	mqMode      string // "redis" | "kafka" | "both"
	filter      *SensitiveFilter
	tokenIssuer *TokenIssuer

	msgIDCounter atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// nextMsgID 生成本机唯一的消息ID：serverID + 单调递增计数器，
// 用于客户端在 Redis/Kafka 双路径都可能到达时做去重
func (h *Hub) nextMsgID() string {
	return h.serverID + "-" + strconv.FormatUint(h.msgIDCounter.Add(1), 10)
}

func NewHub(serverID, mqMode string, ctx context.Context, cancel context.CancelFunc) *Hub {
	h := &Hub{
		serverID:   serverID,
		register:   make(chan *Client, 1024),
		unregister: make(chan *Client, 1024),
		msgQueue:   make(chan *Message, msgQueueSize),
		mqMode:     mqMode,
		filter:     NewSensitiveFilter(defaultSensitiveWords),
		ctx:        ctx,
		cancel:     cancel,
	}
	for i := range h.shards {
		h.shards[i] = &roomShard{rooms: make(map[string]map[string]*Client)}
	}
	return h
}

// shardFor 返回 roomID 所属的分片
func (h *Hub) shardFor(roomID string) *roomShard {
	return h.shards[fnv32(roomID)%numShards]
}

// fnv32 FNV-1a 哈希，用于房间到分片的映射
func fnv32(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// Run 主循环，处理连接注册/注销
func (h *Hub) Run() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case client := <-h.register:
			h.addClient(client)
		case client := <-h.unregister:
			h.removeClient(client)
		}
	}
}

func (h *Hub) addClient(c *Client) {
	shard := h.shardFor(c.roomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	room, ok := shard.rooms[c.roomID]
	if !ok {
		room = make(map[string]*Client)
		shard.rooms[c.roomID] = room
	}
	// 如果已有同 uid 连接，先关闭旧的
	if old, exists := room[c.uid]; exists {
		old.cancel()
	}
	room[c.uid] = c
	metricConnectionsTotal.WithLabelValues(c.roomID).Inc()
	log.Printf("[Hub] client joined: uid=%s room=%s, room_size=%d", c.uid, c.roomID, len(room))
}

func (h *Hub) removeClient(c *Client) {
	shard := h.shardFor(c.roomID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	room, ok := shard.rooms[c.roomID]
	if !ok {
		return
	}
	if existing, exists := room[c.uid]; exists && existing == c {
		delete(room, c.uid)
		if len(room) == 0 {
			delete(shard.rooms, c.roomID)
		}
	}
}

// BroadcastToRoom 向指定房间的所有连接发送消息
// 使用分片 RLock，只阻塞同分片内的房间增删，不影响其他分片的广播/注册
// 持锁约束：此处持 RLock，只做 channel send（非阻塞），不发 RPC
func (h *Hub) BroadcastToRoom(roomID string, data []byte) {
	shard := h.shardFor(roomID)
	shard.mu.RLock()
	room, ok := shard.rooms[roomID]
	if !ok {
		shard.mu.RUnlock()
		return
	}
	// 复制 client 指针列表，尽快释放锁
	clients := make([]*Client, 0, len(room))
	for _, c := range room {
		clients = append(clients, c)
	}
	shard.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.sendCh <- data:
		default:
		}
	}
}

// BroadcastToAll 广播到所有房间
func (h *Hub) BroadcastToAll(data []byte) {
	for _, roomID := range h.allRoomIDs() {
		h.BroadcastToRoom(roomID, data)
	}
}

// allRoomIDs 汇总所有分片的房间 ID，逐分片加锁，非全局快照
func (h *Hub) allRoomIDs() []string {
	var roomIDs []string
	for _, shard := range h.shards {
		shard.mu.RLock()
		for id := range shard.rooms {
			roomIDs = append(roomIDs, id)
		}
		shard.mu.RUnlock()
	}
	return roomIDs
}

// GetRoomList 获取房间列表
func (h *Hub) GetRoomList() []RoomInfo {
	var rooms []RoomInfo
	for _, shard := range h.shards {
		shard.mu.RLock()
		for id, clients := range shard.rooms {
			rooms = append(rooms, RoomInfo{
				RoomID:      id,
				OnlineCount: len(clients),
				IsActive:    true,
			})
		}
		shard.mu.RUnlock()
	}
	return rooms
}

// GetRoomClients 获取房间内的 uid 列表
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

// CloseRoom 关闭房间，踢出所有连接
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

// KickClient 踢出指定用户
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

// GetConnCount 获取总连接数
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

// GetRoomCount 获取房间数
func (h *Hub) GetRoomCount() int {
	count := 0
	for _, shard := range h.shards {
		shard.mu.RLock()
		count += len(shard.rooms)
		shard.mu.RUnlock()
	}
	return count
}

// RoomInfo 房间信息
type RoomInfo struct {
	RoomID      string `json:"room_id"`
	OnlineCount int    `json:"online_count"`
	IsActive    bool   `json:"is_active"`
}
