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
}

// UpMessage 上行消息（客户端发往服务端）
type UpMessage struct {
	Type         string `json:"type"`
	Content      string `json:"content"`
	ClientTS     int64  `json:"client_ts"`
	ClientTSNano int64  `json:"client_ts_ns,omitempty"`
	Token        string `json:"token,omitempty"` // type=="reauth" 时携带的新会话令牌
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
	return msg
}

func releaseMessage(msg *Message) {
	messagePool.Put(msg)
}
