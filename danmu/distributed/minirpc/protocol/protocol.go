// Package protocol 定义 mini-RPC 的线上协议（wire protocol）。
//
// 一条消息 = 固定 17 字节的 Header + 变长 Body：
//
//	| magic(2B) | version(1B) | codec(1B) | type(1B) | seq(8B) | bodyLen(4B) | body... |
//
// 所有多字节整数一律使用大端序（网络字节序）。
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// MagicNumber 用于快速识别"这是不是我们的协议"，
	// 防止端口被误连（比如有人拿浏览器访问 RPC 端口）时读到垃圾数据还继续解析。
	MagicNumber uint16 = 0x3bef

	Version byte = 1

	// HeaderSize 固定头长度：2 + 1 + 1 + 1 + 8 + 4 = 17 字节。
	HeaderSize = 17

	// MaxBodyLen 单条消息 body 上限（16 MB）。
	// 没有这个上限，恶意方发一个 bodyLen=0xFFFFFFFF 的头就能让服务端尝试分配 4GB 内存。
	MaxBodyLen uint32 = 16 << 20
)

// MessageType 区分消息种类。
type MessageType byte

const (
	MsgRequest MessageType = iota
	MsgResponse
	MsgHeartbeat
)

// CodecType 标识 body 的序列化方式。第一版先只做 JSON，留出扩展位。
type CodecType byte

const (
	CodecJSON CodecType = iota
	CodecGob
)

var (
	ErrBadMagic    = errors.New("protocol: bad magic number")
	ErrBadVersion  = errors.New("protocol: unsupported version")
	ErrBodyTooLong = errors.New("protocol: body length exceeds limit")
)

// Header 是解析后的消息头。
type Header struct {
	Magic   uint16
	Version byte
	Codec   CodecType
	Type    MessageType
	Seq     uint64
	BodyLen uint32
}

// Message 是一条完整消息。
type Message struct {
	Header Header
	Body   []byte
}

// WriteMessage 把 msg 编码后写入 w。调用方无需预先填 Header.Magic/Version/BodyLen，
// 由本函数根据常量和 len(msg.Body) 填充。
func WriteMessage(w io.Writer, msg *Message) error {
	// header+body 拼进一个 buf、一次 Write 写出：多个 goroutine 共用一条 conn 时，
	// 两次 Write 之间可能插进别人的字节，导致流被写花。
	buf := make([]byte, HeaderSize+len(msg.Body))
	binary.BigEndian.PutUint16(buf[0:2], MagicNumber)
	buf[2] = Version
	buf[3] = byte(msg.Header.Codec)
	buf[4] = byte(msg.Header.Type)
	binary.BigEndian.PutUint64(buf[5:13], msg.Header.Seq)
	// 长度的唯一可信来源是 len(Body)，不用调用方填的 BodyLen
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(msg.Body)))
	copy(buf[HeaderSize:], msg.Body)
	_, err := w.Write(buf)
	return err
}

// ReadMessage 从 r 中读出一条完整消息。这是处理"粘包/拆包"的核心：
// TCP 是字节流，一次 Read 可能只到半个头、也可能带着下一条消息的开头。
func ReadMessage(r io.Reader) (*Message, error) {
	// io.ReadFull 保证精确读满 len(hbuf) 字节（Read 只保证"最多"），
	// 这一行就是拆包处理的全部：不够 17 字节就一直等。
	hbuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, hbuf); err != nil {
		return nil, err
	}
	h := Header{
		Magic:   binary.BigEndian.Uint16(hbuf[0:2]),
		Version: hbuf[2],
		Codec:   CodecType(hbuf[3]),
		Type:    MessageType(hbuf[4]),
		Seq:     binary.BigEndian.Uint64(hbuf[5:13]),
		BodyLen: binary.BigEndian.Uint32(hbuf[13:17]),
	}
	// 校验必须在 make(body) 之前，否则伪造的超大 bodyLen 会先把内存打爆
	if h.Magic != MagicNumber {
		return nil, ErrBadMagic
	}
	if h.Version != Version {
		return nil, ErrBadVersion
	}
	if h.BodyLen > MaxBodyLen {
		return nil, ErrBodyTooLong
	}
	body := make([]byte, h.BodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return &Message{Header: h, Body: body}, nil
}
