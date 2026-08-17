// Package core 是 comet/logic/job 三个 goim 式服务共享的连接层与消息模型。
// 相对原单体 server 包，这里应用了 REVIEW round-2 的 D1：连接注册/注销不再走
// 单个 Hub.Run goroutine，而是直接调用分片安全的 AddClient/RemoveClient。
package core

import "sync"

// Message 弹幕消息（服务端下行 / Kafka 载荷）。字段与原单体、consumer 保持一致。
type Message struct {
	Type         string `json:"type"`
	MsgID        string `json:"msg_id,omitempty"`
	RoomID       string `json:"room_id,omitempty"`
	UID          string `json:"uid,omitempty"`
	Content      string `json:"content,omitempty"`
	ClientTS     int64  `json:"client_ts,omitempty"`
	ClientTSNano int64  `json:"client_ts_ns,omitempty"`
	ServerTS     int64  `json:"server_ts,omitempty"`
	SourceServer string `json:"source_server,omitempty"` // origin comet id，仅观测用
	// OffsetMS 点播弹幕：相对视频起点的毫秒；直播可忽略/为 0。
	OffsetMS int64 `json:"offset_ms,omitempty"`
	// Seq 房间内单调递增序号（与单体对齐；单体的 worker flush 打号，
	// 分布式侧重连补发未实现前恒为 0，仅保留字段做跨端 wire 兼容）。
	Seq uint64 `json:"seq,omitempty"`
	// Priority/PinUntil 与单体对齐的字段（0=普通，1=高优先级；置顶截止 unix 毫秒）。
	// 分布式侧目前透传不改造通道，仅保证 wire 兼容。
	Priority int   `json:"priority,omitempty"`
	PinUntil int64 `json:"pin_until,omitempty"`
}

// UpMessage 上行消息（客户端 → comet）。
type UpMessage struct {
	Type         string `json:"type"`
	Content      string `json:"content"`
	ClientTS     int64  `json:"client_ts"`
	ClientTSNano int64  `json:"client_ts_ns,omitempty"`
	Token        string `json:"token,omitempty"`     // type=="reauth" 时携带的新会话令牌
	OffsetMS     int64  `json:"offset_ms,omitempty"` // 点播播放进度
	// MsgID/Priority/PinUntil 与单体对齐（分布式侧目前不消费，仅 wire 兼容）。
	MsgID    string `json:"msg_id,omitempty"`
	Priority int    `json:"priority,omitempty"`
	PinUntil int64  `json:"pin_until,omitempty"`
}

var messagePool = sync.Pool{New: func() any { return &Message{} }}

func AcquireMessage() *Message {
	m := messagePool.Get().(*Message)
	*m = Message{}
	return m
}

func ReleaseMessage(m *Message) { messagePool.Put(m) }
