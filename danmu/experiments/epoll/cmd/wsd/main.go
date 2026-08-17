// wsd 是基于 gnet（epoll 事件循环）的最小 WebSocket 空闲连接服务器。
// 实验目的（P8）：对照 gorilla/websocket（goroutine-per-conn）与 gnet（事件循环）
// 在大量空闲长连接下的内存（RSS）与建连性能差异。
//
// WebSocket 协议层按最小实现自写（实验性质，仅覆盖空闲连接场景）：
//   - 握手：解析 HTTP Upgrade 请求，计算 Sec-WebSocket-Accept，回 101
//   - 帧层：解析 FIN/opcode/长度；处理 ping→pong、close；文本帧忽略（空闲测试无业务）
//   - 不做 TLS（边缘终止；gnet 无内置 TLS，自备是已知结论）
//
// 用法：wsd -addr :19000 -stats :19001
package main

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsState 每个连接的解析状态机
type wsState struct {
	handshakeDone bool
}

type wsServer struct {
	gnet.BuiltinEventEngine
	connCount atomic.Int64
	statsAddr string
}

func (s *wsServer) OnBoot(eng gnet.Engine) gnet.Action {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/count", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"conns": s.connCount.Load()})
	})
	go http.ListenAndServe(s.statsAddr, mux)
	return gnet.None
}

func (s *wsServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetContext(&wsState{})
	s.connCount.Add(1)
	return nil, gnet.None
}

func (s *wsServer) OnClose(c gnet.Conn, err error) gnet.Action {
	s.connCount.Add(-1)
	return gnet.None
}

func (s *wsServer) OnTraffic(c gnet.Conn) gnet.Action {
	state := c.Context().(*wsState)

	for {
		buf, _ := c.Next(-1)
		if len(buf) == 0 {
			return gnet.None
		}

		if !state.handshakeDone {
			// 找 \r\n\r\n 结束的请求头
			idx := indexDoubleCRLF(buf)
			if idx < 0 {
				if len(buf) > 64*1024 {
					return gnet.Close
				}
				return gnet.None // 等完整请求头
			}
			key, ok := extractSecWebSocketKey(buf[:idx])
			if !ok {
				return gnet.Close
			}
			resp := buildUpgradeResponse(key)
			c.Write(resp)
			c.Discard(idx + 4)
			state.handshakeDone = true
			continue
		}

		// 帧层：解析一帧
		consumed, action := parseOneFrame(c, buf)
		if action != gnet.None {
			return action
		}
		if consumed == 0 {
			return gnet.None // 数据不足
		}
		c.Discard(consumed)
	}
}

// parseOneFrame 解析单帧；返回消费字节数与 action（Close 表示连接应关闭）。
func parseOneFrame(c gnet.Conn, buf []byte) (int, gnet.Action) {
	if len(buf) < 2 {
		return 0, gnet.None
	}
	opcode := buf[0] & 0x0F
	masked := buf[1]&0x80 != 0
	payloadLen := uint64(buf[1] & 0x7F)

	offset := 2
	switch payloadLen {
	case 126:
		if len(buf) < 4 {
			return 0, gnet.None
		}
		payloadLen = uint64(binary.BigEndian.Uint16(buf[2:4]))
		offset = 4
	case 127:
		if len(buf) < 10 {
			return 0, gnet.None
		}
		payloadLen = binary.BigEndian.Uint64(buf[2:10])
		offset = 10
	}
	maskKey := [4]byte{}
	if masked {
		if len(buf) < offset+4 {
			return 0, gnet.None
		}
		copy(maskKey[:], buf[offset:offset+4])
		offset += 4
	}
	if payloadLen > 1024*1024 {
		return 0, gnet.Close
	}
	if len(buf) < offset+int(payloadLen) {
		return 0, gnet.None // 等完整 payload
	}
	payload := buf[offset : offset+int(payloadLen)]

	switch opcode {
	case 0x8: // close
		return offset + int(payloadLen), gnet.Close
	case 0x9: // ping → pong
		pong := append([]byte{0x8A, byte(len(payload))}, payload...)
		c.Write(pong)
	case 0xA: // pong：忽略
	default: // text/binary/continuation：空闲实验忽略
	}
	return offset + int(payloadLen), gnet.None
}

func indexDoubleCRLF(b []byte) int {
	for i := 0; i+3 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' && b[i+2] == '\r' && b[i+3] == '\n' {
			return i
		}
	}
	return -1
}

func extractSecWebSocketKey(head []byte) (string, bool) {
	lines := splitLines(head)
	for _, line := range lines {
		if len(line) > 18 && string(line[:18]) == "Sec-WebSocket-Key:" {
			key := trimSpace(line[18:])
			if len(key) == 24 {
				return string(key), true
			}
		}
	}
	return "", false
}

func buildUpgradeResponse(key string) []byte {
	h := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h[:])
	return []byte(fmt.Sprintf(
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: %s\r\n\r\n", accept))
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			line := b[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	return out
}

func trimSpace(b []byte) string {
	s := 0
	e := len(b)
	for s < e && (b[s] == ' ' || b[s] == '\t') {
		s++
	}
	for e > s && (b[e-1] == ' ' || b[e-1] == '\t' || b[e-1] == '\r') {
		e--
	}
	return string(b[s:e])
}

func main() {
	addr := flag.String("addr", ":19000", "listen address")
	statsAddr := flag.String("stats", ":19001", "stats HTTP address")
	flag.Parse()

	srv := &wsServer{statsAddr: *statsAddr}
	log.Printf("[wsd] starting on %s (stats %s)", *addr, *statsAddr)
	// gnet v2 要求带协议前缀的地址
	listenAddr := *addr
	if !strings.HasPrefix(listenAddr, "tcp://") {
		listenAddr = "tcp://" + listenAddr
	}
	if err := gnet.Run(srv, listenAddr,
		gnet.WithMulticore(false),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithReadBufferCap(32*1024),
	); err != nil {
		log.Fatalf("gnet.Run: %v", err)
	}
}
