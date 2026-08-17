package main

import (
	"sync"
)

// Message 弹幕消息，通过 sync.Pool 复用减少 GC 压力
type Message struct {
	Type         string `json:"type"`
	MsgID        string `json:"msg_id,omitempty"` // 全局唯一消息ID，Redis/Kafka双路径都可能到达客户端时用于去重
	RoomID       string `json:"room_id,omitempty"`
	UID          string `json:"uid,omitempty"`
	Content      string `json:"content,omitempty"`
	ClientTS     int64  `json:"client_ts,omitempty"`
	ClientTSNano int64  `json:"client_ts_ns,omitempty"` // 客户端纳秒时间戳，服务端原样透传；压测端据此算亚毫秒级 E2E 延迟
	ServerTS     int64  `json:"server_ts,omitempty"`
	SourceServer string `json:"source_server,omitempty"` // 标记消息来源服务器，用于去重
	Seq          uint64 `json:"seq,omitempty"`           // 房间内单调递增序号（worker flush 时打号），重连补发/进房拉最近 N 条的依据
	Priority     int    `json:"priority,omitempty"`      // 0=普通弹幕（可丢），1=高优先级（醒目留言/礼物，走独立通道，不静默丢弃）
	PinUntil     int64  `json:"pin_until,omitempty"`     // 高优先级置顶截止时间（unix 毫秒，0=不置顶）；客户端本地计时解除
	Flag         string `json:"flag,omitempty"`          // 风控标记（如 "spam"）：房间词库 flag 模式命中的消息，放行但打标
}

// UpMessage 上行消息（客户端发往服务端）
type UpMessage struct {
	Type         string `json:"type"`
	Content      string `json:"content"`
	ClientTS     int64  `json:"client_ts"`
	ClientTSNano int64  `json:"client_ts_ns,omitempty"`
	Token        string `json:"token,omitempty"`     // type=="reauth" 时携带的新会话令牌
	AfterSeq     uint64 `json:"after_seq,omitempty"` // type=="reconnect" 时携带：已收到的最大 seq，服务端补发缺口
	MsgID        string `json:"msg_id,omitempty"`    // 客户端生成的幂等 ID：服务端据此去重（TTL 窗口）并回 ack
	Priority     int    `json:"priority,omitempty"`  // 0=普通，1=高优先级（醒目留言等）
	PinUntil     int64  `json:"pin_until,omitempty"` // 高优先级置顶截止（unix 毫秒）
}

var messagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

func acquireMessage() *Message {
	msg := messagePool.Get().(*Message)
	msg.Type = ""
	msg.MsgID = ""
	msg.RoomID = ""
	msg.UID = ""
	msg.Content = ""
	msg.ClientTS = 0
	msg.ClientTSNano = 0
	msg.ServerTS = 0
	msg.SourceServer = ""
	msg.Seq = 0
	msg.Priority = 0
	msg.PinUntil = 0
	msg.Flag = ""
	return msg
}

func releaseMessage(msg *Message) {
	messagePool.Put(msg)
}
