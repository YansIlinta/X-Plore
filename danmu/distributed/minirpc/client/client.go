// Package client 实现 mini-RPC 客户端：
// 一条 TCP 连接上并发发起多个调用，靠 seq 把乱序回来的响应匹配回等待方。
//
// 结构：
//   - Call():   分配 seq → 登记 pending[seq]=ch → 发请求 → select 等 ch 或 ctx 超时
//   - readLoop: 唯一的读 goroutine，循环读响应 → 按 seq 查 pending → 投递
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"minirpc/protocol"
)

// request / response 与 server 侧的 JSON 结构对应。
type request struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

type response struct {
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// ErrShutdown 连接已因错误关闭，所有未完成调用以此失败。
var ErrShutdown = errors.New("client: connection is shut down")

type Client struct {
	conn net.Conn

	wmu sync.Mutex // 串行化对 conn 的写（多个 Call goroutine 并发发请求）

	mu      sync.Mutex // 保护以下三个字段
	seq     uint64
	pending map[uint64]chan *response
	err     error // 连接级错误，一旦置位客户端不可再用
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: conn, pending: make(map[uint64]chan *response)}
	go c.readLoop()
	return c, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Call 发起一次调用并等待结果。ctx 控制超时/取消——这是防"响应永远不回来"
// 导致 goroutine 泄漏的关键（第一天面试题的答案落地处）。
func (c *Client) Call(ctx context.Context, method string, args, reply any) error {
	// 1. 登记：分配 seq，挂上接收 chan。
	// chan 必须带 1 缓冲！readLoop 往里投递时不能阻塞（原因见 readLoop 注释和考题）。
	ch := make(chan *response, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return c.err
	}
	c.seq++
	seq := c.seq
	c.pending[seq] = ch
	c.mu.Unlock()

	// 2. 编码并发送请求。失败要把 pending 里刚登记的自己摘掉，不能留垃圾。
	if err := c.send(seq, method, args); err != nil {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
		return err
	}

	// 3. 等结果或超时。
	select {
	case resp := <-ch:
		if resp == nil {
			return ErrShutdown // readLoop 挂了，连接级失败
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		if reply != nil && len(resp.Result) > 0 {
			return json.Unmarshal(resp.Result, reply)
		}
		return nil
	case <-ctx.Done():
		// 超时/取消：必须把 seq 从 pending 摘掉，否则这个条目永远留在 map 里
		// （对应服务端"永远不回响应"的场景——客户端侧的自我保护）
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *Client) send(seq uint64, method string, args any) error {
	argBytes, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("client: marshal args: %w", err)
	}
	body, err := json.Marshal(&request{Method: method, Args: argBytes})
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return protocol.WriteMessage(c.conn, &protocol.Message{
		Header: protocol.Header{Codec: protocol.CodecJSON, Type: protocol.MsgRequest, Seq: seq},
		Body:   body,
	})
}

// readLoop 是唯一从连接上读的 goroutine：读到响应就按 seq 分发。
func (c *Client) readLoop() {
	for {
		msg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			c.shutdown(err)
			return
		}
		var resp response
		if err := json.Unmarshal(msg.Body, &resp); err != nil {
			c.shutdown(fmt.Errorf("client: bad response body: %w", err))
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[msg.Header.Seq]
		delete(c.pending, msg.Header.Seq)
		c.mu.Unlock()
		// ok==false 说明是"迟到的响应"：调用方已超时退场并删掉了 seq。
		// 直接丢弃即可。这也是 ch 要带缓冲的另一半原因：
		// 就算调用方"正要退场还没删掉"，往缓冲 chan 发送也不会把 readLoop 卡死。
		if ok {
			ch <- &resp
		}
	}
}

// shutdown 连接级失败：置错误位，唤醒所有还在等的调用。
func (c *Client) shutdown(err error) {
	c.conn.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
	for seq, ch := range c.pending {
		close(ch) // 关闭 chan，等待方收到零值 nil → 返回 ErrShutdown
		delete(c.pending, seq)
	}
}
