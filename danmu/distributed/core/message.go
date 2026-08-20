// Package core 是 comet/logic/job 三个 goim 式服务共享的连接层与消息模型。
// 相对原单体 server 包，这里应用了 REVIEW round-2 的 D1：连接注册/注销不再走
// 单个 Hub.Run goroutine，而是直接调用分片安全的 AddClient/RemoveClient。
package core

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

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

// ---------------------------------------------------------------------------
// Phase 2: unified realtime message contract.
//
// These types deliberately coexist with the legacy Message/UpMessage wire
// structs above. Phase 2 establishes the domain contract first; protobuf/Kafka
// migration and cross-Gateway routing happen in subsequent, independently
// verifiable steps so the existing Danmu path remains a regression baseline.

type TargetType string

const (
	TargetUser      TargetType = "USER"
	TargetDevice    TargetType = "DEVICE"
	TargetChannel   TargetType = "CHANNEL"
	TargetBroadcast TargetType = "BROADCAST"
)

type DeliveryClass string

const (
	DeliveryEphemeral DeliveryClass = "EPHEMERAL"
	DeliveryReliable  DeliveryClass = "RELIABLE"
)

type MessageType string

const (
	MessageDanmu MessageType = "DANMU"
)

// MessageEnvelope is the internal contract shared by future user/device/channel
// delivery paths. It is not itself a reliability guarantee: DeliveryReliable
// becomes meaningful only after persistence, idempotency, sequencing, sync and
// client ACK are connected in later phases.
//
// Device IDs are scoped by user in the Phase 1 SessionIndex, so DEVICE targets
// carry both TargetUserID and TargetID (the device ID). This avoids treating a
// device ID as globally unique when the connection kernel does not make that
// guarantee.
type MessageEnvelope struct {
	MessageID       string          `json:"message_id,omitempty"`
	ClientMessageID string          `json:"client_message_id,omitempty"`
	SenderID        string          `json:"sender_id,omitempty"`
	TargetType      TargetType      `json:"target_type"`
	TargetID        string          `json:"target_id,omitempty"`
	TargetUserID    string          `json:"target_user_id,omitempty"`
	DeliveryClass   DeliveryClass   `json:"delivery_class"`
	MessageType     MessageType     `json:"message_type"`
	Sequence        int64           `json:"sequence,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	ClientTS        int64           `json:"client_ts,omitempty"`
	ServerTS        int64           `json:"server_ts,omitempty"`
}

func (m MessageEnvelope) Validate() error {
	switch m.TargetType {
	case TargetUser, TargetChannel:
		if strings.TrimSpace(m.TargetID) == "" {
			return errors.New("target_id is required for USER/CHANNEL target")
		}
	case TargetDevice:
		if strings.TrimSpace(m.TargetID) == "" {
			return errors.New("target_id is required for DEVICE target")
		}
		if strings.TrimSpace(m.TargetUserID) == "" {
			return errors.New("target_user_id is required for DEVICE target")
		}
	case TargetBroadcast:
		// Global broadcast intentionally permits an empty target_id.
	default:
		return errors.New("invalid target_type")
	}

	switch m.DeliveryClass {
	case DeliveryEphemeral, DeliveryReliable:
	default:
		return errors.New("invalid delivery_class")
	}

	if m.MessageType == "" {
		return errors.New("message_type is required")
	}
	return nil
}

// DanmuChannelID is the single compatibility mapping from legacy room IDs to
// the generic channel namespace. Routing/experiments should reuse this helper
// instead of constructing independent channel keys.
func DanmuChannelID(roomID string) string {
	return "danmu:room:" + roomID
}

// DanmuRoomID reverses DanmuChannelID. The bool is false for non-Danmu channels.
func DanmuRoomID(channelID string) (string, bool) {
	const prefix = "danmu:room:"
	if !strings.HasPrefix(channelID, prefix) {
		return "", false
	}
	return strings.TrimPrefix(channelID, prefix), true
}

// NewDanmuEnvelope adapts the existing room-broadcast input into the unified
// contract without changing its semantics: Danmu remains CHANNEL + EPHEMERAL,
// so the current bounded-queue/drop-on-backpressure behavior is preserved.
func NewDanmuEnvelope(roomID, senderID, content string, clientTS, serverTS int64) MessageEnvelope {
	payload, _ := json.Marshal(struct {
		Content string `json:"content"`
	}{Content: content})

	return MessageEnvelope{
		SenderID:      senderID,
		TargetType:    TargetChannel,
		TargetID:      DanmuChannelID(roomID),
		DeliveryClass: DeliveryEphemeral,
		MessageType:   MessageDanmu,
		Payload:       payload,
		ClientTS:      clientTS,
		ServerTS:      serverTS,
	}
}
