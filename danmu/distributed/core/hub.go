package core

import (
	"context"
	"strconv"
	"sync/atomic"
)

// Hub 是本机连接平面的门面（comet 注入点）：把 room-centric 的分片房表升级为
// 通用 Connection Kernel（见 connkernel.go），对外保留旧 room API 作为兼容层。
//
// 兼容映射：room 是 channel 的特例，内部一律以
//
//	danmu:room:<roomID>
//
// 作为 SubscriptionIndex 的 channel key；BroadcastToRoom/CloseRoom/KickClient 等
// 旧语义不变地委托到 kernel，因此 comet（PushRoom/localBroadcast）与既有的
// Danmu 行为无需改动、不回归。
//
// 身份：每个 Client 由 Hub.AddClient 分配稳定 ConnectionID，并补充
// DeviceID（客户端未上报则回落 DefaultDeviceID）与 GatewayID（=本机 ServerID）。
// 顶级行为从「uid → 单连接（顶号）」升级为「user / device / connection」多设备并存，
// 由 ConnectionPolicy 显式选择：
//
//	PolicyMultiDevice         默认：同一用户可多设备/多连接并存（US-01）
//	PolicySingleDevicePerUser 显式顶号：新连接顶掉该用户全部旧连接
type Hub struct {
	ServerID string
	// TokenIssuer 用于校验 reauth 令牌（会话续期）。
	TokenIssuer *TokenIssuer
	// Uplink 由 comet 注入：readPump 收到一条弹幕就回调它（转发给 Logic）。
	Uplink func(uid, roomID, content string, clientTS, clientTSNano, offsetMS int64)

	// ConnectionPolicy 决定同一用户的连接并存语义（默认 PolicyMultiDevice）。
	ConnectionPolicy SessionPolicy

	connSeq  atomic.Uint64
	connMan  *ConnectionManager
	sessions *SessionIndex
	subs     *SubscriptionIndex

	ctx context.Context
}

// fnv32 FNV-1a 哈希，用于 key 到分片的映射（内核与 RoomInfo 共用）。
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

func NewHub(serverID string, ctx context.Context) *Hub {
	return &Hub{
		ServerID:         serverID,
		connMan:          NewConnectionManager(),
		sessions:         NewSessionIndex(),
		subs:             NewSubscriptionIndex(),
		ConnectionPolicy: PolicyMultiDevice,
		ctx:              ctx,
	}
}

// Context 返回 Hub 的根 context，供 comet 派生每连接的子 context。
func (h *Hub) Context() context.Context { return h.ctx }

// roomChannel 把 roomID 映射为内部 channel key（Room Broadcast 兼容层）。
func (h *Hub) roomChannel(roomID string) string { return "danmu:room:" + roomID }

// channelRoom 内部 channel key 反向还原 roomID；非 danmu:room: 前缀的 channel 原样返回
// （Phase 2/3 引入通用 Channel 目标后，这里承载无缝扩展）。
func (h *Hub) channelRoom(channel string) string {
	const prefix = "danmu:room:"
	if len(channel) > len(prefix) && channel[:len(prefix)] == prefix {
		return channel[len(prefix):]
	}
	return channel
}

// nextConnectionID 生成本机稳定 ConnectionID：gatewayID + 单调递增序号。
func (h *Hub) nextConnectionID() string {
	return h.ServerID + "-conn-" + strconv.FormatUint(h.connSeq.Add(1), 10)
}

// AddClient 登记一个新连接：分配 ConnectionID / DeviceID / GatewayID，写入
// ConnectionManager + SessionIndex + SubscriptionIndex（订阅其 room 对应的 channel）。
// 默认（PolicyMultiDevice）不做顶号；PolicySingleDevicePerUser 下先顶掉该用户旧连接。
func (h *Hub) AddClient(c *Client) {
	if c.ConnectionID == "" {
		c.ConnectionID = h.nextConnectionID()
	}
	if c.DeviceID == "" {
		c.DeviceID = DefaultDeviceID
	}
	c.GatewayID = h.ServerID

	if h.ConnectionPolicy == PolicySingleDevicePerUser {
		for _, old := range h.sessions.GetUserConnections(c.UID) {
			if old == c {
				continue
			}
			h.RemoveClient(old)
			old.Close(4009, "session replaced by new connection")
		}
	}
	h.connMan.Add(c)
	h.sessions.Add(c)
	h.subs.Subscribe(h.roomChannel(c.RoomID), c)
	MetricConnInc()
}

// RemoveClient 连接断开清理：从三个索引全部移除（幂等；未登记的连接空跑不递减）。
func (h *Hub) RemoveClient(c *Client) {
	if !h.connMan.Remove(c) {
		return // 从未登记（或已被清理），不递减计数
	}
	h.sessions.Remove(c)
	h.subs.RemoveConn(c)
}

// BroadcastToRoom 向房间对应 channel 的所有连接非阻塞下发；sendCh 满则丢弃并计数。
// 语义与旧 Hub 一致。
func (h *Hub) BroadcastToRoom(roomID string, data []byte) int {
	return h.subs.PushChannel(h.roomChannel(roomID), data)
}

// HasRoom 本机是否持有该房间（廉价 RLock 读）。
func (h *Hub) HasRoom(roomID string) bool {
	return h.subs.HasChannel(h.roomChannel(roomID))
}

type RoomInfo struct {
	RoomID      string `json:"room_id"`
	OnlineCount int    `json:"online_count"`
	IsActive    bool   `json:"is_active"`
}

func (h *Hub) GetRoomList() []RoomInfo {
	var rooms []RoomInfo
	for _, channel := range h.subs.ChannelList() {
		subs := h.subs.GetSubscribers(channel)
		rooms = append(rooms, RoomInfo{
			RoomID:      h.channelRoom(channel),
			OnlineCount: len(subs),
			IsActive:    true,
		})
	}
	return rooms
}

func (h *Hub) GetRoomClients(roomID string) ([]string, bool) {
	subs := h.subs.GetSubscribers(h.roomChannel(roomID))
	if len(subs) == 0 {
		return nil, false
	}
	seen := make(map[string]struct{}, len(subs))
	uids := make([]string, 0, len(subs))
	for _, c := range subs {
		if _, dup := seen[c.UID]; dup {
			continue // 同一用户多连接只列一次（旧语义 uid 列表）
		}
		seen[c.UID] = struct{}{}
		uids = append(uids, c.UID)
	}
	return uids, true
}

// CloseRoom 关闭某房间：把该 channel 的订阅连接全部从索引移除并 Close。
// 多设备语义：房间内该连接无论几个设备一起关闭。返回该房间是否存在。
func (h *Hub) CloseRoom(roomID string) bool {
	channel := h.roomChannel(roomID)
	subs := h.subs.GetSubscribers(channel)
	if len(subs) == 0 {
		return false
	}
	for _, c := range subs {
		h.RemoveClient(c)
		c.Close(4001, "room closed")
	}
	return true
}

// KickClient 踢出指定用户在某房间的全部连接（多设备语义）。
func (h *Hub) KickClient(roomID, uid string) bool {
	channel := h.roomChannel(roomID)
	subs := h.subs.GetSubscribers(channel)
	kicked := false
	for _, c := range subs {
		if c.UID != uid {
			continue
		}
		h.RemoveClient(c)
		c.Close(4001, "kicked")
		kicked = true
	}
	return kicked
}

// GetConnCount 总连接数（ConnectionManager 原子计数）。
func (h *Hub) GetConnCount() int { return int(h.connMan.Count()) }

// GetRoomCount 活跃房间数（SubscriptionIndex 原子计数）。
func (h *Hub) GetRoomCount() int { return int(h.subs.Count()) }

// OnlineCount 在线连接数（O(1)，替代分片扫描）。
func (h *Hub) OnlineCount() int64 { return h.connMan.Count() }

// RoomCountFast 活跃房间数（O(1)）。
func (h *Hub) RoomCountFast() int64 { return h.subs.Count() }

// OnlineUserCount / OnlineDeviceCount 在线用户 / 设备数（O(1)，观测接口用）。
func (h *Hub) OnlineUserCount() int64   { return h.sessions.CountUsers() }
func (h *Hub) OnlineDeviceCount() int64 { return h.sessions.CountDevices() }

// --- 多设备查询/定向投递（Phase 1 提供索引查询；Phase 3 的跨机路由在其上叠加） ---

// GetUserConnections 返回某用户本机当前全部连接。
func (h *Hub) GetUserConnections(uid string) []*Client { return h.sessions.GetUserConnections(uid) }

// GetDeviceConnections 返回某用户指定设备的连接。
func (h *Hub) GetDeviceConnections(uid, deviceID string) []*Client {
	return h.sessions.GetDeviceConnections(uid, deviceID)
}

// PushUser 定向推送给某用户的全部连接（本机范围）。
func (h *Hub) PushUser(uid string, data []byte) int {
	delivered := 0
	for _, c := range h.sessions.GetUserConnections(uid) {
		if c.TrySend(data) {
			delivered++
		}
	}
	if delivered == 0 {
		return 0
	}
	MetricMsgOut(delivered)
	return delivered
}

// PushDevice 定向推送给某用户指定设备的连接（本机范围）。
func (h *Hub) PushDevice(uid, deviceID string, data []byte) int {
	delivered := 0
	for _, c := range h.sessions.GetDeviceConnections(uid, deviceID) {
		if c.TrySend(data) {
			delivered++
		}
	}
	if delivered > 0 {
		MetricMsgOut(delivered)
	}
	return delivered
}
