package main

import (
	"context"
	"log"
	"sync"
)

const msgQueueSize = 100000

// Hub 管理所有房间的连接
// rooms 用 sync.RWMutex 保护：广播用 RLock，增删连接用 Lock
type Hub struct {
	// rooms: map[roomId]map[uid]*Client
	rooms    map[string]map[string]*Client
	mu       sync.RWMutex
	serverID string

	register   chan *Client
	unregister chan *Client
	msgQueue   chan *Message // 进程内消息队列，带缓冲 channel 做削峰

	redisHub  *RedisHub
	kafkaProd *KafkaProducer
	mqMode    string // "redis" | "kafka" | "both"

	ctx    context.Context
	cancel context.CancelFunc
}

func NewHub(serverID, mqMode string, ctx context.Context, cancel context.CancelFunc) *Hub {
	return &Hub{
		rooms:      make(map[string]map[string]*Client),
		serverID:   serverID,
		register:   make(chan *Client, 1024),
		unregister: make(chan *Client, 1024),
		msgQueue:   make(chan *Message, msgQueueSize),
		mqMode:     mqMode,
		ctx:        ctx,
		cancel:     cancel,
	}
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
	h.mu.Lock()
	defer h.mu.Unlock()

	room, ok := h.rooms[c.roomID]
	if !ok {
		room = make(map[string]*Client)
		h.rooms[c.roomID] = room
	}
	// 如果已有同 uid 连接，先关闭旧的
	if old, exists := room[c.uid]; exists {
		old.cancel()
	}
	room[c.uid] = c
	log.Printf("[Hub] client joined: uid=%s room=%s, room_size=%d", c.uid, c.roomID, len(room))
}

func (h *Hub) removeClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	room, ok := h.rooms[c.roomID]
	if !ok {
		return
	}
	if existing, exists := room[c.uid]; exists && existing == c {
		delete(room, c.uid)
		if len(room) == 0 {
			delete(h.rooms, c.roomID)
		}
	}
}

// BroadcastToRoom 向指定房间的所有连接发送消息
// 使用 RLock 读锁，不阻塞其他广播
// 持锁约束：此处持 RLock，只做 channel send（非阻塞），不发 RPC
func (h *Hub) BroadcastToRoom(roomID string, data []byte) {
	h.mu.RLock()
	room, ok := h.rooms[roomID]
	if !ok {
		h.mu.RUnlock()
		return
	}
	// 复制 client 指针列表，尽快释放锁
	clients := make([]*Client, 0, len(room))
	for _, c := range room {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.sendCh <- data:
		default:
			// sendCh 满（慢客户端），丢弃消息，保护整体不阻塞
		}
	}
}

// BroadcastToAll 广播到所有房间
func (h *Hub) BroadcastToAll(data []byte) {
	h.mu.RLock()
	roomIDs := make([]string, 0, len(h.rooms))
	for id := range h.rooms {
		roomIDs = append(roomIDs, id)
	}
	h.mu.RUnlock()

	for _, roomID := range roomIDs {
		h.BroadcastToRoom(roomID, data)
	}
}

// GetRoomList 获取房间列表
func (h *Hub) GetRoomList() []RoomInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	rooms := make([]RoomInfo, 0, len(h.rooms))
	for id, clients := range h.rooms {
		rooms = append(rooms, RoomInfo{
			RoomID:      id,
			OnlineCount: len(clients),
			IsActive:    true,
		})
	}
	return rooms
}

// GetRoomClients 获取房间内的 uid 列表
func (h *Hub) GetRoomClients(roomID string) ([]string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	room, ok := h.rooms[roomID]
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
	h.mu.Lock()
	room, ok := h.rooms[roomID]
	if !ok {
		h.mu.Unlock()
		return false
	}
	clients := make([]*Client, 0, len(room))
	for _, c := range room {
		clients = append(clients, c)
	}
	delete(h.rooms, roomID)
	h.mu.Unlock()

	for _, c := range clients {
		c.Close(4001, "room closed")
	}
	return true
}

// KickClient 踢出指定用户
func (h *Hub) KickClient(roomID, uid string) bool {
	h.mu.Lock()
	room, ok := h.rooms[roomID]
	if !ok {
		h.mu.Unlock()
		return false
	}
	c, exists := room[uid]
	if !exists {
		h.mu.Unlock()
		return false
	}
	delete(room, uid)
	if len(room) == 0 {
		delete(h.rooms, roomID)
	}
	h.mu.Unlock()

	c.Close(4001, "kicked")
	return true
}

// GetConnCount 获取总连接数
func (h *Hub) GetConnCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, room := range h.rooms {
		count += len(room)
	}
	return count
}

// GetRoomCount 获取房间数
func (h *Hub) GetRoomCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms)
}

// RoomInfo 房间信息
type RoomInfo struct {
	RoomID      string `json:"room_id"`
	OnlineCount int    `json:"online_count"`
	IsActive    bool   `json:"is_active"`
}
