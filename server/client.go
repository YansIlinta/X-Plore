package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 4096
	sendChSize     = 512
)

// Client 代表一个 WebSocket 连接
// writePump 是 conn 的唯一写者，所有外发消息必须经 sendCh 串行写出
// 禁止其他 goroutine 直接调用 conn.WriteMessage
type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	sendCh  chan []byte // 外发消息 channel，writePump 是唯一消费者
	uid     string
	roomID  string
	limiter *TokenBucket
	ctx     context.Context
	cancel  context.CancelFunc

	closeOnce   sync.Once
	closeCode   int
	closeReason string

	sessionExpiresAt atomic.Int64
}

func NewClient(hub *Hub, conn *websocket.Conn, uid, roomID string, parentCtx context.Context) *Client {
	ctx, cancel := context.WithCancel(parentCtx)
	c := &Client{
		hub:     hub,
		conn:    conn,
		sendCh:  make(chan []byte, sendChSize),
		uid:     uid,
		roomID:  roomID,
		limiter: NewTokenBucket(20, 50),
		ctx:     ctx,
		cancel:  cancel,
	}
	c.sessionExpiresAt.Store(time.Now().Add(sessionTTL).UnixNano())
	return c
}

// readPump 只读 goroutine，从 WebSocket 读取上行消息
// 持锁约束：此函数不持任何锁，不发 RPC，只往 channel 发消息
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
		c.cancel()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[readPump] uid=%s room=%s err=%v", c.uid, c.roomID, err)
			}
			return
		}

		var up UpMessage
		if err := json.Unmarshal(data, &up); err != nil {
			continue
		}

		if up.Type == "reauth" {
			// 会话续期消息不占用弹幕限流配额，也不经过 msgQueue
			c.handleReauth(up.Token)
			continue
		}

		// 限流检查
		if !c.limiter.Allow() {
			// 超额消息丢弃，不断开连接
			rateLimitMsg := []byte(`[{"type":"rate_limited"}]`)
			select {
			case c.sendCh <- rateLimitMsg:
			default:
			}
			continue
		}

		if up.Type != "danmu" || up.Content == "" {
			continue
		}

		// 限制内容长度
		content := up.Content
		if len(content) > 500 {
			content = content[:500]
		}
		// 本地 AC 自动机敏感词过滤，纯内存匹配不阻塞主链路
		content = c.hub.filter.Filter(content)

		msg := acquireMessage()
		msg.Type = "danmu"
		msg.MsgID = c.hub.nextMsgID()
		msg.RoomID = c.roomID
		msg.UID = c.uid
		msg.Content = content
		msg.ClientTS = up.ClientTS
		msg.ServerTS = time.Now().UnixMilli()
		msg.SourceServer = c.hub.serverID

		// 投递到进程内消息队列（带缓冲 channel 做削峰）
		select {
		case c.hub.msgQueue <- msg:
			metricMessagesTotal.WithLabelValues(c.roomID, "in").Inc()
		default:
			// 队列满，丢弃消息
			releaseMessage(msg)
			log.Printf("[readPump] msgQueue full, dropping message from uid=%s room=%s", c.uid, c.roomID)
		}
	}
}

// handleReauth 校验客户端上报的续期令牌，通过则延长会话到期时间并回 ack，
// 不通过只记录日志，不主动断开——真正的强制点是 writePump 里的到期检查
func (c *Client) handleReauth(token string) {
	if c.hub.tokenIssuer == nil {
		return
	}
	expiresAt, err := c.hub.tokenIssuer.Verify(token, c.uid, c.roomID)
	if err != nil {
		log.Printf("[reauth] uid=%s room=%s reject: %v", c.uid, c.roomID, err)
		return
	}
	c.sessionExpiresAt.Store(expiresAt.UnixNano())
	ack := []byte(`[{"type":"reauth_ack"}]`)
	select {
	case c.sendCh <- ack:
	default:
	}
}

// writePump 唯一写者 goroutine，从 sendCh 消费消息写往 WebSocket
// 并发约束：只有 writePump 调用 conn.WriteMessage，其他 goroutine 禁止直接写
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case <-c.ctx.Done():
			code := c.closeCode
			reason := c.closeReason
			if code == 0 {
				code = 1001
				reason = "server shutting down"
			}
			c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(code, reason),
				time.Now().Add(writeWait))
			return

		case message := <-c.sendCh:
			// 批量排空：将 sendCh 中所有待发消息合并为一次 WebSocket 写入
			pending := len(c.sendCh)
			if pending == 0 {
				c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
					return
				}
			} else {
				batched := make([][]byte, 0, pending+1)
				batched = append(batched, message)
				for i := 0; i < pending; i++ {
					batched = append(batched, <-c.sendCh)
				}
				merged := mergeJSONArrays(batched)
				c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := c.conn.WriteMessage(websocket.TextMessage, merged); err != nil {
					return
				}
			}

		case <-ticker.C:
			if time.Now().UnixNano() > c.sessionExpiresAt.Load() {
				c.Close(4008, "session expired")
				continue
			}
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// mergeJSONArrays 将多个 JSON 数组合并为一个
// 例如 [{"a":1}] + [{"b":2},{"c":3}] → [{"a":1},{"b":2},{"c":3}]
func mergeJSONArrays(arrays [][]byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	first := true
	for _, a := range arrays {
		a = bytes.TrimSpace(a)
		if len(a) < 2 {
			continue
		}
		inner := a[1 : len(a)-1] // strip outer []
		inner = bytes.TrimSpace(inner)
		if len(inner) == 0 {
			continue
		}
		if !first {
			buf.WriteByte(',')
		}
		buf.Write(inner)
		first = false
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// Close 主动关闭连接（踢人/关房间）
// 不直接写 conn（写者只能是 writePump，否则与其正常下行消息竞争同一个 conn），
// 也不 close(sendCh)（BroadcastToRoom 等可能仍在并发向 sendCh 发送，close 后再 send 会 panic）。
// 只记录关闭码/原因后 cancel ctx，由 writePump 感知 ctx.Done() 后统一发送 CloseMessage 并退出。
func (c *Client) Close(code int, reason string) {
	c.closeOnce.Do(func() {
		c.closeCode = code
		c.closeReason = reason
		c.cancel()
	})
}
