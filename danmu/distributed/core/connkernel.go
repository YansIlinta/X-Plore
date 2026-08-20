// Connection Kernel（Phase 1）
//
// 把原 room-centric 的 Hub 拆成三个各司其职的索引，语义由「uid → 单连接」升级为
// 「user / device / connection」，支持多设备同时在线（PRD US-01）：
//
//	ConnectionManager    connectionID -> *Client      连接注册表
//	SessionIndex         userID -> deviceID -> []*Client  多设备会话索引
//	SubscriptionIndex    channelID -> []*Client       频道订阅索引
//
// 兼容性：
//   - DeviceID 为可选：客户端未上报时回落到 DefaultDeviceID，不强制客户端升级。
//   - room 是 channel 的一个特例，映射成 "danmu:room:<roomID>"；旧 room API 由
//     Hub 以兼容层方式委托到这里，广播语义与非阻塞丢弃（sendCh 满即丢）保持不变。
//   - 顶号行为不再是底层数据结构的限制：由 Hub.ConnectionPolicy 显式选择
//     （默认 PolicyMultiDevice 允许并存；PolicySingleDevicePerUser 保留旧顶号）。
//
// 并发：每个索引内部按 key 哈希分片，各片独立 RWMutex；广播在片锁下只拷贝订阅者
// 指针列表，释放锁后才做 channel send（与旧 Hub 的持锁约束一致，不发 RPC）。
package core

import (
	"sync"
	"sync/atomic"
)

// kernelShards 内核索引的分片数（沿用旧 Hub 的分片粒度，保持注册/广播并发特征）。
const kernelShards = 256

// SessionPolicy 决定同一用户可同时保有的连接语义。
type SessionPolicy int

const (
	// PolicyMultiDevice：默认。同一用户可以多设备、多连接并存（PRD US-01）。
	PolicyMultiDevice SessionPolicy = iota
	// PolicySingleDevicePerUser：旧「顶号」语义——新连接注册时顶掉该用户全部旧连接。
	PolicySingleDevicePerUser
)

// DefaultDeviceID 用于未显式上报设备标识的连接（兼容：不强制客户端升级）。
// 客户端可通过 WS handshake 的 `device` query 参数显式区分 Web/iOS/Android 等。
const DefaultDeviceID = "default"

// ---------------------------------------------------------------------------
// ConnectionManager：connectionID -> *Client（本机连接注册表）

type connShard struct {
	mu    sync.RWMutex
	conns map[string]*Client
}

type ConnectionManager struct {
	shards [kernelShards]*connShard
	count  atomic.Int64
}

func NewConnectionManager() *ConnectionManager {
	m := &ConnectionManager{}
	for i := range m.shards {
		m.shards[i] = &connShard{conns: make(map[string]*Client)}
	}
	return m
}

func (m *ConnectionManager) shardFor(connID string) *connShard {
	return m.shards[fnv32(connID)%kernelShards]
}

// Add 登记连接。若 connID 已存在则替换（不重复计数）。
func (m *ConnectionManager) Add(c *Client) {
	s := m.shardFor(c.ConnectionID)
	s.mu.Lock()
	if _, exists := s.conns[c.ConnectionID]; !exists {
		m.count.Add(1)
	}
	s.conns[c.ConnectionID] = c
	s.mu.Unlock()
}

// Remove 移除连接并递减计数；返回是否真的删除（幂等：connID 不存在返回 false 且不递减）。
func (m *ConnectionManager) Remove(c *Client) bool {
	s := m.shardFor(c.ConnectionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[c.ConnectionID]; !ok {
		return false
	}
	delete(s.conns, c.ConnectionID)
	m.count.Add(-1)
	return true
}

func (m *ConnectionManager) Get(connID string) *Client {
	s := m.shardFor(connID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conns[connID]
}

// Push 非阻塞投递一条已序列化的消息到指定连接；sendCh 满返回 false（drop）。
func (m *ConnectionManager) Push(connID string, data []byte) bool {
	if c := m.Get(connID); c != nil {
		return c.TrySend(data)
	}
	return false
}

// Close 关闭指定连接（code/reason 由 WritePump 以 CloseMessage 下发）。
func (m *ConnectionManager) Close(connID string, code int, reason string) {
	if c := m.Get(connID); c != nil {
		c.Close(code, reason)
	}
}

// Count 当前登记连接数（O(1) 原子计数，替代分片扫描）。
func (m *ConnectionManager) Count() int64 { return m.count.Load() }

// ---------------------------------------------------------------------------
// SessionIndex：userID -> deviceID -> connID -> *Client（多设备会话索引）

type sessionShard struct {
	mu    sync.RWMutex
	users map[string]map[string]map[string]*Client // user -> device -> connID -> client
}

type SessionIndex struct {
	shards [kernelShards]*sessionShard
	// users/devices 在线数（O(1) 观测）。
	userCount   atomic.Int64
	deviceCount atomic.Int64
}

func NewSessionIndex() *SessionIndex {
	s := &SessionIndex{}
	for i := range s.shards {
		s.shards[i] = &sessionShard{users: make(map[string]map[string]map[string]*Client)}
	}
	return s
}

func (s *SessionIndex) shardFor(uid string) *sessionShard {
	return s.shards[fnv32(uid)%kernelShards]
}

// Add 把连接登记进 (user, device) 会话。同一 (user, device) 可容纳多个连接
// （同一设备的多个 tab / 多连接由客户端设备标志之外的 ConnectionID 区分）。
func (s *SessionIndex) Add(c *Client) {
	sh := s.shardFor(c.UID)
	sh.mu.Lock()
	devices := sh.users[c.UID]
	if devices == nil {
		devices = make(map[string]map[string]*Client)
		sh.users[c.UID] = devices
		s.userCount.Add(1)
	}
	conns := devices[c.DeviceID]
	if conns == nil {
		conns = make(map[string]*Client)
		devices[c.DeviceID] = conns
		s.deviceCount.Add(1)
	}
	conns[c.ConnectionID] = c
	sh.mu.Unlock()
}

// Remove 从会话移出连接并维护计数；返回是否真的删除（幂等）。
func (s *SessionIndex) Remove(c *Client) bool {
	sh := s.shardFor(c.UID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	devices, ok := sh.users[c.UID]
	if !ok {
		return false
	}
	conns, ok := devices[c.DeviceID]
	if !ok {
		return false
	}
	if _, ok := conns[c.ConnectionID]; !ok {
		return false
	}
	delete(conns, c.ConnectionID)
	if len(conns) == 0 {
		delete(devices, c.DeviceID)
		s.deviceCount.Add(-1)
	}
	if len(devices) == 0 {
		delete(sh.users, c.UID)
		s.userCount.Add(-1)
	}
	return true
}

// GetUserConnections 返回该用户当前本机的全部连接。
func (s *SessionIndex) GetUserConnections(uid string) []*Client {
	sh := s.shardFor(uid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	devices, ok := sh.users[uid]
	if !ok {
		return nil
	}
	out := make([]*Client, 0, 2)
	for _, conns := range devices {
		for _, c := range conns {
			out = append(out, c)
		}
	}
	return out
}

// GetDeviceConnections 返回该用户指定设备的本机连接。
func (s *SessionIndex) GetDeviceConnections(uid, deviceID string) []*Client {
	sh := s.shardFor(uid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	devices, ok := sh.users[uid]
	if !ok {
		return nil
	}
	conns, ok := devices[deviceID]
	if !ok {
		return nil
	}
	out := make([]*Client, 0, len(conns))
	for _, c := range conns {
		out = append(out, c)
	}
	return out
}

// CountUsers / CountDevices 在线用户 / 设备数（O(1)）。
func (s *SessionIndex) CountUsers() int64   { return s.userCount.Load() }
func (s *SessionIndex) CountDevices() int64 { return s.deviceCount.Load() }

// ---------------------------------------------------------------------------
// SubscriptionIndex：channelID -> connID -> *Client

type subShard struct {
	mu    sync.RWMutex
	chans map[string]map[string]*Client // channel -> connID -> client
}

// memberShard 是 connID -> channel 的反查表分片：与 chans 分片按不同 key 哈希，
// 供断连清理「这个连接订阅了哪些 channel」的 O(1) 读取，无需扫描全部 channel 分片。
type memberShard struct {
	mu     sync.RWMutex
	member map[string]map[string]int // connID -> channel -> membership(1)
}

type SubscriptionIndex struct {
	shards  [kernelShards]*subShard
	members [kernelShards]*memberShard
	// channelCount 原子计数（O(1) 观测，替代分片扫描）。
	channelCount atomic.Int64
}

func NewSubscriptionIndex() *SubscriptionIndex {
	s := &SubscriptionIndex{}
	for i := range s.shards {
		s.shards[i] = &subShard{chans: make(map[string]map[string]*Client)}
		s.members[i] = &memberShard{member: make(map[string]map[string]int)}
	}
	return s
}

func (s *SubscriptionIndex) shardFor(channel string) *subShard {
	return s.shards[fnv32(channel)%kernelShards]
}

func (s *SubscriptionIndex) memberShardFor(connID string) *memberShard {
	return s.members[fnv32(connID)%kernelShards]
}

// Subscribe 登记一个连接对某 channel 的订阅。重复 Subscribe 同一
// (channel, connectionID) 是幂等的，不会累积反向 membership 计数。
func (s *SubscriptionIndex) Subscribe(channel string, c *Client) {
	sh := s.shardFor(channel)
	sh.mu.Lock()
	conns, ok := sh.chans[channel]
	if !ok {
		conns = make(map[string]*Client)
		sh.chans[channel] = conns
		s.channelCount.Add(1)
	}
	_, existed := conns[c.ConnectionID]
	conns[c.ConnectionID] = c
	sh.mu.Unlock()
	if existed {
		return
	}

	ms := s.memberShardFor(c.ConnectionID)
	ms.mu.Lock()
	if ms.member[c.ConnectionID] == nil {
		ms.member[c.ConnectionID] = make(map[string]int)
	}
	ms.member[c.ConnectionID][channel] = 1
	ms.mu.Unlock()
}

// Unsubscribe 移除一个连接对某 channel 的订阅；重复 Unsubscribe 幂等。
func (s *SubscriptionIndex) Unsubscribe(channel string, c *Client) {
	sh := s.shardFor(channel)
	sh.mu.Lock()
	removed := false
	conns, ok := sh.chans[channel]
	if ok {
		if _, exists := conns[c.ConnectionID]; exists {
			delete(conns, c.ConnectionID)
			removed = true
			if len(conns) == 0 {
				delete(sh.chans, channel)
				s.channelCount.Add(-1)
			}
		}
	}
	sh.mu.Unlock()
	if !removed {
		return
	}

	ms := s.memberShardFor(c.ConnectionID)
	ms.mu.Lock()
	if m := ms.member[c.ConnectionID]; m != nil {
		delete(m, channel)
		if len(m) == 0 {
			delete(ms.member, c.ConnectionID)
		}
	}
	ms.mu.Unlock()
}

// RemoveConn 连接断开时从它订阅的全部 channel 移除（disconnect cleanup）。
// 幂等：未订阅该 channel 的连接空跑也不递减。
func (s *SubscriptionIndex) RemoveConn(c *Client) {
	// 从反查表快照连接订阅的 channel 列表（按 connID 分片，非全局扫描）。
	ms := s.memberShardFor(c.ConnectionID)
	ms.mu.RLock()
	var channels []string
	if m := ms.member[c.ConnectionID]; m != nil {
		channels = make([]string, 0, len(m))
		for ch := range m {
			channels = append(channels, ch)
		}
	}
	ms.mu.RUnlock()
	for _, ch := range channels {
		s.Unsubscribe(ch, c)
	}
}

// GetSubscribers 返回某 channel 当前订阅的连接副本（供广播前快照）。
func (s *SubscriptionIndex) GetSubscribers(channel string) []*Client {
	sh := s.shardFor(channel)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	conns, ok := sh.chans[channel]
	if !ok {
		return nil
	}
	out := make([]*Client, 0, len(conns))
	for _, c := range conns {
		out = append(out, c)
	}
	return out
}

// PushChannel 向某 channel 的所有订阅连接非阻塞广播；sendCh 满则丢弃并计数。
// 持锁约束：片锁下只拷贝订阅者列表，释放后才 channel send（不发 RPC）。
func (s *SubscriptionIndex) PushChannel(channel string, data []byte) int {
	subs := s.GetSubscribers(channel)
	delivered, dropped := 0, 0
	for _, c := range subs {
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

// HasChannel 某 channel 是否仍有订阅者（廉价 RLock 读）。
func (s *SubscriptionIndex) HasChannel(channel string) bool {
	sh := s.shardFor(channel)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	conns, ok := sh.chans[channel]
	return ok && len(conns) > 0
}

// ChannelList 返回当前有订阅者的 channel 列表（观测/审计用，非全局快照）。
func (s *SubscriptionIndex) ChannelList() []string {
	var out []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		for ch := range sh.chans {
			out = append(out, ch)
		}
		sh.mu.RUnlock()
	}
	return out
}

// Count 当前活跃 channel 数（O(1) 原子计数）。
func (s *SubscriptionIndex) Count() int64 { return s.channelCount.Load() }
