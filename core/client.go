package core

import (
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
	sendChSize     = 256
)

// Client 一个 WebSocket 连接。writePump 是 conn 唯一写者，所有外发经 sendCh 串行写出。
type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	sendCh  chan []byte
	UID     string
	RoomID  string
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
		UID:     uid,
		RoomID:  roomID,
		limiter: NewTokenBucket(20, 50),
		ctx:     ctx,
		cancel:  cancel,
	}
	c.sessionExpiresAt.Store(time.Now().Add(SessionTTL).UnixNano())
	return c
}

// TrySend 非阻塞下发一段已序列化好的 payload；sendCh 满则丢弃返回 false。
func (c *Client) TrySend(data []byte) bool {
	select {
	case c.sendCh <- data:
		return true
	default:
		return false
	}
}

// ReadPump 只读 goroutine：收上行，弹幕经 hub.Uplink 转发给 Logic。
func (c *Client) ReadPump() {
	defer func() {
		c.hub.RemoveClient(c)
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
				log.Printf("[readPump] uid=%s room=%s err=%v", c.UID, c.RoomID, err)
			}
			return
		}

		var up UpMessage
		if err := json.Unmarshal(data, &up); err != nil {
			continue
		}

		if up.Type == "reauth" {
			c.handleReauth(up.Token)
			continue
		}

		if !c.limiter.Allow() {
			c.TrySend([]byte(`[{"type":"rate_limited"}]`))
			continue
		}

		if up.Type != "danmu" || up.Content == "" {
			continue
		}
		content := up.Content
		if len(content) > 500 {
			content = content[:500]
		}
		// 敏感词过滤 + msg_id 生成 + 落 Kafka 都在 Logic 侧做；comet 只转发。
		MetricMsgIn()
		if c.hub.Uplink != nil {
			c.hub.Uplink(c.UID, c.RoomID, content, up.ClientTS, up.ClientTSNano, up.OffsetMS)
		}
	}
}

func (c *Client) handleReauth(token string) {
	if c.hub.TokenIssuer == nil {
		return
	}
	expiresAt, err := c.hub.TokenIssuer.Verify(token, c.UID, c.RoomID)
	if err != nil {
		log.Printf("[reauth] uid=%s room=%s reject: %v", c.UID, c.RoomID, err)
		return
	}
	c.sessionExpiresAt.Store(expiresAt.UnixNano())
	c.TrySend([]byte(`[{"type":"reauth_ack"}]`))
}

// WritePump 唯一写者：消费 sendCh 写往 conn，兼做心跳与会话到期检查。
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case <-c.ctx.Done():
			code, reason := c.closeCode, c.closeReason
			if code == 0 {
				code, reason = 1001, "server shutting down"
			}
			c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(code, reason), time.Now().Add(writeWait))
			return

		case message := <-c.sendCh:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
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

// Close 主动关闭：记录关闭码后 cancel，由 WritePump 感知后发送 CloseMessage。
// 不直接写 conn（写者只能是 WritePump），也不 close(sendCh)（广播可能仍在并发写）。
func (c *Client) Close(code int, reason string) {
	c.closeOnce.Do(func() {
		c.closeCode = code
		c.closeReason = reason
		c.cancel()
	})
}
