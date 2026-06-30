package main

import (
	"context"
	"encoding/json"
	"log"
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
}

func NewClient(hub *Hub, conn *websocket.Conn, uid, roomID string, parentCtx context.Context) *Client {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Client{
		hub:     hub,
		conn:    conn,
		sendCh:  make(chan []byte, sendChSize),
		uid:     uid,
		roomID:  roomID,
		limiter: NewTokenBucket(20, 50), // 每秒 20 条，突发 50
		ctx:     ctx,
		cancel:  cancel,
	}
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

		var up UpMessage
		if err := json.Unmarshal(data, &up); err != nil {
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

		msg := acquireMessage()
		msg.Type = "danmu"
		msg.RoomID = c.roomID
		msg.UID = c.uid
		msg.Content = content
		msg.ClientTS = up.ClientTS
		msg.ServerTS = time.Now().UnixMilli()
		msg.SourceServer = c.hub.serverID

		// 投递到进程内消息队列（带缓冲 channel 做削峰）
		select {
		case c.hub.msgQueue <- msg:
		default:
			// 队列满，丢弃消息
			releaseMessage(msg)
			log.Printf("[readPump] msgQueue full, dropping message from uid=%s room=%s", c.uid, c.roomID)
		}
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
			c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(1001, "server shutting down"),
				time.Now().Add(writeWait))
			return

		case message, ok := <-c.sendCh:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(1000, ""),
					time.Now().Add(writeWait))
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Close 主动关闭连接（踢人/关房间），发送 CloseMessage
func (c *Client) Close(code int, reason string) {
	c.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason),
		time.Now().Add(writeWait))
	c.cancel()
	close(c.sendCh)
}
