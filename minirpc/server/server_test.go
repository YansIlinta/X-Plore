package server

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"

	"minirpc/protocol"
)

// 实现完 Register/handleRequest 后运行: go test ./server/ -v

// ---- 测试用的服务 ----

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

// notAMethod 不符合调用约定，Register 时应被静默跳过。
func (a *Arith) String() string { return "arith" }

// Boom 模拟有 bug 的业务方法：一调就 panic。
func (a *Arith) Boom(args *Args, reply *int) error { panic("boom") }

// ---- 工具 ----

func startServer(t *testing.T) net.Addr {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	s := New()
	if err := s.Register(&Arith{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	go s.Serve(lis)
	return lis.Addr()
}

func call(t *testing.T, conn net.Conn, seq uint64, method string, args any) {
	t.Helper()
	argBytes, _ := json.Marshal(args)
	body, _ := json.Marshal(&Request{Method: method, Args: argBytes})
	err := protocol.WriteMessage(conn, &protocol.Message{
		Header: protocol.Header{Codec: protocol.CodecJSON, Type: protocol.MsgRequest, Seq: seq},
		Body:   body,
	})
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func readResp(t *testing.T, conn net.Conn) (uint64, *Response) {
	t.Helper()
	msg, err := protocol.ReadMessage(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp Response
	if err := json.Unmarshal(msg.Body, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return msg.Header.Seq, &resp
}

// ---- 测试 ----

func TestCall(t *testing.T) {
	addr := startServer(t)
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	call(t, conn, 1, "Arith.Mul", &Args{A: 3, B: 4})
	seq, resp := readResp(t, conn)
	if seq != 1 || resp.Error != "" {
		t.Fatalf("seq=%d err=%q", seq, resp.Error)
	}
	var result int
	json.Unmarshal(resp.Result, &result)
	if result != 12 {
		t.Fatalf("want 12, got %d", result)
	}
}

func TestBusinessError(t *testing.T) {
	addr := startServer(t)
	conn, _ := net.Dial("tcp", addr.String())
	defer conn.Close()

	call(t, conn, 2, "Arith.Div", &Args{A: 1, B: 0})
	seq, resp := readResp(t, conn)
	if seq != 2 || resp.Error != "divide by zero" {
		t.Fatalf("seq=%d err=%q", seq, resp.Error)
	}
}

func TestMethodNotFound(t *testing.T) {
	addr := startServer(t)
	conn, _ := net.Dial("tcp", addr.String())
	defer conn.Close()

	call(t, conn, 3, "Arith.Nope", &Args{})
	seq, resp := readResp(t, conn)
	if seq != 3 || resp.Error == "" {
		t.Fatalf("want error response, got seq=%d resp=%+v", seq, resp)
	}
}

// 并发发 50 个请求，响应可能乱序，靠 seq 对账：seq=i 的请求算 i*2，结果必须是 i*2。
func TestConcurrentOutOfOrder(t *testing.T) {
	addr := startServer(t)
	conn, _ := net.Dial("tcp", addr.String())
	defer conn.Close()

	const n = 50
	var wmu sync.Mutex
	for i := 1; i <= n; i++ {
		wmu.Lock()
		call(t, conn, uint64(i), "Arith.Mul", &Args{A: i, B: 2})
		wmu.Unlock()
	}
	got := make(map[uint64]int, n)
	for i := 0; i < n; i++ {
		seq, resp := readResp(t, conn)
		if resp.Error != "" {
			t.Fatalf("seq=%d err=%q", seq, resp.Error)
		}
		var result int
		json.Unmarshal(resp.Result, &result)
		got[seq] = result
	}
	for i := uint64(1); i <= n; i++ {
		if got[i] != int(i)*2 {
			t.Fatalf("seq=%d want %d got %d", i, i*2, got[i])
		}
	}
}

// 业务方法 panic 时：进程不死、当前请求收到错误响应、同一服务后续请求照常工作。
func TestPanicRecovery(t *testing.T) {
	addr := startServer(t)
	conn, _ := net.Dial("tcp", addr.String())
	defer conn.Close()

	call(t, conn, 9, "Arith.Boom", &Args{})
	seq, resp := readResp(t, conn)
	if seq != 9 || resp.Error == "" {
		t.Fatalf("want error response for panic, got seq=%d resp=%+v", seq, resp)
	}

	// 服务端必须还活着，能继续服务
	call(t, conn, 10, "Arith.Mul", &Args{A: 2, B: 3})
	seq, resp = readResp(t, conn)
	var result int
	json.Unmarshal(resp.Result, &result)
	if seq != 10 || resp.Error != "" || result != 6 {
		t.Fatalf("server should survive panic: seq=%d err=%q result=%d", seq, resp.Error, result)
	}
}

// Register 传值而不是指针时，方法集里没有指针接收者方法，应该报错而不是静默成功。
func TestRegisterValueReceiver(t *testing.T) {
	s := New()
	if err := s.Register(Arith{}); err == nil {
		t.Fatal("Register(Arith{}) should fail: no methods in value method set")
	}
}
