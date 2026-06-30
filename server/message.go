package main

import (
	"encoding/json"
	"sync"
)

// Message 弹幕消息，通过 sync.Pool 复用减少 GC 压力
type Message struct {
	Type         string `json:"type"`
	RoomID       string `json:"room_id,omitempty"`
	UID          string `json:"uid,omitempty"`
	Content      string `json:"content,omitempty"`
	ClientTS     int64  `json:"client_ts,omitempty"`
	ServerTS     int64  `json:"server_ts,omitempty"`
	SourceServer string `json:"source_server,omitempty"` // 标记消息来源服务器，用于去重
}

// UpMessage 上行消息（客户端发往服务端）
type UpMessage struct {
	Type     string `json:"type"`
	Content  string `json:"content"`
	ClientTS int64  `json:"client_ts"`
}

var messagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

func acquireMessage() *Message {
	msg := messagePool.Get().(*Message)
	msg.Type = ""
	msg.RoomID = ""
	msg.UID = ""
	msg.Content = ""
	msg.ClientTS = 0
	msg.ServerTS = 0
	msg.SourceServer = ""
	return msg
}

func releaseMessage(msg *Message) {
	messagePool.Put(msg)
}

// bufferPool 复用序列化 buffer
var bufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

func acquireBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

func releaseBuffer(buf *[]byte) {
	*buf = (*buf)[:0]
	bufferPool.Put(buf)
}

// serializeMessages 批量序列化消息为 JSON 数组，使用 buffer pool
func serializeMessages(msgs []*Message) ([]byte, error) {
	buf := acquireBuffer()
	defer releaseBuffer(buf)

	data, err := json.Marshal(msgs)
	if err != nil {
		return nil, err
	}
	// 返回新分配的 slice，因为 buf 会被回收
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}
