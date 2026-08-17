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
	hpSendChSize   = 64 // 高优通道容量：writePump 优先排空，正常远不会满
)

// Client 代表一个 WebSocket 连接
// writePump 是 conn 的唯一写者，所有外发消息必须经 sendCh/hpSendCh 串行写出
// 禁止其他 goroutine 直接调用 conn.WriteMessage
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	sendCh   chan []byte // 普通外发通道，writePump 是唯一消费者
	hpSendCh chan []byte // 高优先级外发通道（醒目留言等）：writePump 优先排空
	uid      string
	roomID   string
	limiter  *TokenBucket
	ctx      context.Context
	cancel   context.CancelFunc

	closeOnce   sync.Once
	closeCode   int
	closeReason string

	// registered 在 hub.addClient 完成后置位：重连补发需等注册完成，
	// 避免「注册窗口内广播既没实时投递、又没进热历史快照」的漏补。
	registered atomic.Bool

	sessionExpiresAt atomic.Int64
}

func NewClient(hub *Hub, conn *websocket.Conn, uid, roomID string, parentCtx context.Context) *Client {
	ctx, cancel := context.WithCancel(parentCtx)
	c := &Client{
		hub:      hub,
		conn:     conn,
		sendCh:   make(chan []byte, sendChSize),
		hpSendCh: make(chan []byte, hpSendChSize),
		uid:      uid,
		roomID:   roomID,
		limiter:  NewTokenBucket(20, 50),
		ctx:      ctx,
		cancel:   cancel,
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

		if up.Type == "reconnect" {
			// 重连/进房补发请求：不占用限流配额，也不经过 msgQueue
			c.handleReconnect(up.AfterSeq)
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

		// 房间慢速模式（per-room 间隔，0=关闭）；命中同样回 rate_limited
		if !c.hub.slowMode.Allow(c.roomID, c.uid) {
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

		// 限制内容长度。按 rune 边界截断：字节切片会切断 UTF-8 字符，
		// 广播出去变成 � 乱码（与 distributed/core 的 ReadPump 对齐）。
		content := truncateContent(up.Content, 500)
		// 本地 AC 自动机敏感词过滤，纯内存匹配不阻塞主链路
		content = c.hub.filter.Filter(content)
		// 房间词库裁决：block 打码；flag 放行但标记
		maskedContent, flagged := c.hub.wordBank.Apply(c.roomID, content)
		content = maskedContent
		if flagged {
			metricFlaggedTotal.Inc()
		}

		// 幂等去重：客户端携带 msg_id 时，TTL 窗口内重复的 msg_id 只广播一次
		//（重试仍回 ack，客户端可安全重发）。
		msgID := up.MsgID
		if msgID == "" {
			msgID = c.hub.nextMsgID()
		} else if !c.hub.idem.MarkSeen(c.roomID, msgID) {
			c.sendAck(msgID)
			continue
		}

		msg := acquireMessage()
		msg.Type = "danmu"
		msg.MsgID = msgID
		msg.RoomID = c.roomID
		msg.UID = c.uid
		msg.Content = content
		msg.ClientTS = up.ClientTS
		msg.ClientTSNano = up.ClientTSNano
		msg.ServerTS = time.Now().UnixMilli()
		msg.SourceServer = c.hub.serverID
		msg.Priority = up.Priority
		msg.PinUntil = up.PinUntil
		if flagged {
			msg.Flag = "spam"
		}

		// 投递到进程内消息队列（带缓冲 channel 做削峰）
		select {
		case c.hub.msgQueue <- msg:
			metricMessagesTotal.WithLabelValues("in").Inc()
			// 入队即视为「已接受进入广播路径」：回 ack（不等 Kafka 落库）
			c.sendAck(msgID)
		default:
			// 队列满，丢弃消息（不 ack——客户端超时后可按自身策略重试）
			releaseMessage(msg)
			log.Printf("[readPump] msgQueue full, dropping message from uid=%s room=%s", c.uid, c.roomID)
		}
	}
}

// sendAck 回一条消息级 ack。走普通通道即可（ack 量级=客户端发送速率），
// 阻塞式投递保证 ack 不丢：writePump 批量排空很快，正常不会久等。
func (c *Client) sendAck(msgID string) {
	ack, err := json.Marshal([]map[string]string{{"type": "ack", "msg_id": msgID}})
	if err != nil {
		return
	}
	select {
	case c.sendCh <- ack:
	case <-c.ctx.Done():
	}
}

// handleReconnect 处理重连/进房补发：等本连接注册完成后，从热历史取
// seq > afterSeq 的消息一次性下发（帧尾带 replay_done 控制消息）。
// after_seq=0（首连）等价于「进房拉最近 N 条」。
func (c *Client) handleReconnect(afterSeq uint64) {
	if c.hub.hist == nil {
		return
	}
	// 注册在 Hub.Run 单 goroutine 里完成，正常亚毫秒级；上限 500ms 兜底
	deadline := time.Now().Add(500 * time.Millisecond)
	for !c.registered.Load() {
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	msgs, latestSeq := c.hub.hist.ReplayFrom(c.roomID, afterSeq, 0)
	frame := make([]any, 0, len(msgs)+1)
	for i := range msgs {
		frame = append(frame, msgs[i])
	}
	frame = append(frame, map[string]any{
		"type":       "replay_done",
		"room_id":    c.roomID,
		"latest_seq": latestSeq,
		"recovered":  len(msgs),
	})
	data, err := json.Marshal(frame)
	if err != nil {
		log.Printf("[reconnect] uid=%s room=%s marshal error: %v", c.uid, c.roomID, err)
		return
	}
	// 阻塞式投递：补发语义不允许静默丢弃；writePump 批量排空很快，正常不会久等
	select {
	case c.sendCh <- data:
	case <-c.ctx.Done():
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
	sessionTicker := time.NewTicker(time.Second)
	defer func() {
		ticker.Stop()
		sessionTicker.Stop()
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

		case message := <-c.hpSendCh:
			// 高优通道优先排空
			if err := c.writeBatched(c.hpSendCh, message); err != nil {
				return
			}

		case message := <-c.sendCh:
			if err := c.writeBatched(c.sendCh, message); err != nil {
				return
			}

		case <-sessionTicker.C:
			// 会话到期检查独立于 ping 周期：若挂在 30s ping ticker 上，
			// 到期后最长 30s 才断开，踢人/续期失效的感知被严重延迟。
			if time.Now().UnixNano() > c.sessionExpiresAt.Load() {
				c.Close(4008, "session expired")
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// writeBatched 把首条消息与通道内待发消息合并为一次 WebSocket 写入。
func (c *Client) writeBatched(ch chan []byte, message []byte) error {
	// 批量排空：将通道中所有待发消息合并为一次 WebSocket 写入
	pending := len(ch)
	if pending == 0 {
		c.conn.SetWriteDeadline(time.Now().Add(writeWait))
		return c.conn.WriteMessage(websocket.TextMessage, message)
	}
	batched := make([][]byte, 0, pending+1)
	batched = append(batched, message)
	for i := 0; i < pending; i++ {
		batched = append(batched, <-ch)
	}
	merged := mergeJSONArrays(batched)
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(websocket.TextMessage, merged)
}

// truncateContent 把内容截断到最多 maxRunes 个 rune。字节切片（content[:n]）会切断
// UTF-8 字符产生 � 乱码，必须先按 rune 边界截断；长度未超限时原样返回（不分配）。
func truncateContent(content string, maxRunes int) string {
	if len(content) <= maxRunes {
		return content
	}
	runes := []rune(content)
	if len(runes) > maxRunes {
		runes = runes[:maxRunes]
	}
	return string(runes)
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
