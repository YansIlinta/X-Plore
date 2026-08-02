package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"minirpc/server"
)

// ---- 测试服务 ----

type Args struct{ A, B int }

type Arith struct{}

func (a *Arith) Mul(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (a *Arith) Div(args *Args, reply *int) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	*reply = args.A / args.B
	return nil
}

// Slow 模拟"响应永远（很久）不回来"的服务端。
func (a *Arith) Slow(args *Args, reply *int) error {
	time.Sleep(2 * time.Second)
	*reply = 1
	return nil
}

func startServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	s := server.New()
	if err := s.Register(&Arith{}); err != nil {
		t.Fatal(err)
	}
	go s.Serve(lis)
	return lis.Addr().String()
}

// ---- 测试 ----

func TestCall(t *testing.T) {
	c, err := Dial(startServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var result int
	if err := c.Call(context.Background(), "Arith.Mul", &Args{A: 6, B: 7}, &result); err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("want 42, got %d", result)
	}
}

func TestBusinessError(t *testing.T) {
	c, _ := Dial(startServer(t))
	defer c.Close()

	var result int
	err := c.Call(context.Background(), "Arith.Div", &Args{A: 1, B: 0}, &result)
	if err == nil || err.Error() != "divide by zero" {
		t.Fatalf("want business error, got %v", err)
	}
}

// 100 个 goroutine 共用一个 Client 并发调用，验证 seq 分发不串号。
func TestConcurrentCalls(t *testing.T) {
	c, _ := Dial(startServer(t))
	defer c.Close()

	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result int
			if err := c.Call(context.Background(), "Arith.Mul", &Args{A: i, B: 3}, &result); err != nil {
				t.Errorf("call %d: %v", i, err)
				return
			}
			if result != i*3 {
				t.Errorf("call %d: want %d got %d", i, i*3, result)
			}
		}(i)
	}
	wg.Wait()
}

// 超时：Slow 要睡 2s，客户端 100ms 就撤，必须快速返回 DeadlineExceeded，
// 且 pending 表被清干净（不泄漏）。
func TestCallTimeout(t *testing.T) {
	c, _ := Dial(startServer(t))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	var result int
	err := c.Call(ctx, "Arith.Slow", &Args{}, &result)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %v, should be ~100ms", elapsed)
	}

	c.mu.Lock()
	n := len(c.pending)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("pending map leaked %d entries", n)
	}

	// 超时后客户端必须还能正常发起新调用（连接没被污染，迟到响应会被丢弃）
	if err := c.Call(context.Background(), "Arith.Mul", &Args{A: 2, B: 5}, &result); err != nil {
		t.Fatalf("call after timeout: %v", err)
	}
	if result != 10 {
		t.Fatalf("want 10, got %d", result)
	}
}

// 连接断开时，所有阻塞中的调用应立刻以 ErrShutdown 失败，而不是永远等下去。
func TestConnectionDrop(t *testing.T) {
	c, err := Dial(startServer(t))
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		var result int
		errCh <- c.Call(context.Background(), "Arith.Slow", &Args{}, &result)
	}()

	time.Sleep(100 * time.Millisecond) // 等请求发出去
	c.conn.Close()                     // 模拟连接断开（网络抖动/对端崩溃）

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("want ErrShutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("call still blocked after connection drop")
	}
}
