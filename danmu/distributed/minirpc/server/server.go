// Package server 实现 mini-RPC 服务端：
// 反射注册服务 → Accept 循环 → 每连接一个读 goroutine → 每请求一个处理 goroutine。
//
// 调用约定：可注册的方法必须长这样（和 net/rpc 相同）：
//
//	func (s *Svc) Method(args *ArgT, reply *ReplyT) error
//
// body 采用 JSON：请求 {"method":"Svc.Method","args":{...}}，
// 响应 {"error":"...","result":...}。
package server

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"minirpc/protocol"
)

// ReadTimeout 一条消息在这个时间内凑不齐就断开连接——防"恶意沉默"（见第一步的面试题）。
const ReadTimeout = 30 * time.Second

// Request / Response 是 body 里的 JSON 结构。
// Args/Result 用 json.RawMessage 延迟解析：路由的时候还不知道具体类型，
// 等查到注册表里的 argType 再解到真正的结构体上。
type Request struct {
	Method string          `json:"method"` // "Service.Method"
	Args   json.RawMessage `json:"args"`
}

type Response struct {
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// methodType 是注册表里的一个方法条目。
type methodType struct {
	method    reflect.Method
	argType   reflect.Type // 指针类型 *ArgT
	replyType reflect.Type // 指针类型 *ReplyT
}

// service 是一个已注册的服务实例。
type service struct {
	name    string
	rcvr    reflect.Value // 服务实例本身，reflect.Call 时当第 0 个参数（接收者）
	methods map[string]*methodType
}

type Server struct {
	mu       sync.RWMutex
	services map[string]*service
}

func New() *Server {
	return &Server{services: make(map[string]*service)}
}

var errType = reflect.TypeOf((*error)(nil)).Elem()

// Register 把 rcvr 的所有符合调用约定的导出方法登记进注册表，
// 服务名取类型名（*server.Arith → "Arith"）。
func (s *Server) Register(rcvr any) error {
	t := reflect.TypeOf(rcvr)
	v := reflect.ValueOf(rcvr)
	name := reflect.Indirect(v).Type().Name()
	if name == "" {
		return fmt.Errorf("server: cannot register unnamed type %s", t)
	}
	svc := &service{name: name, rcvr: v, methods: make(map[string]*methodType)}
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		mt := m.Type
		// 签名筛选必须在循环体内——出了循环 m/mt 就不存在了
		// 不符合约定的方法（如 String()）静默跳过，不报错
		if mt.NumIn() != 3 || mt.NumOut() != 1 || mt.Out(0) != errType {
			continue
		}
		// Go 没有链式比较，a == b == c 是 (a==b) 得到的 bool 再和 c 比
		if mt.In(1).Kind() != reflect.Ptr || mt.In(2).Kind() != reflect.Ptr {
			continue
		}
		svc.methods[m.Name] = &methodType{method: m, argType: mt.In(1), replyType: mt.In(2)}
	}
	if len(svc.methods) == 0 {
		return fmt.Errorf("server: %s has no methods like func(*Args, *Reply) error (did you pass a value instead of a pointer?)", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.services[name]; dup {
		return fmt.Errorf("server: service %s already registered", name)
	}
	s.services[name] = svc
	return nil
}

// Serve 在 lis 上循环 Accept，每条连接交给一个 goroutine。
func (s *Server) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn 是单条连接的读循环：串行读消息，每条消息开 goroutine 并发处理。
// 这就是"响应可能乱序"的来源——所以响应必须带回请求的 seq。
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	// 所有处理 goroutine 共享这把写锁：WriteMessage 内部虽是一次 Write，
	// 但多 goroutine 同时对一个 conn 调 Write 的顺序仍需要串行化保护。
	var wmu sync.Mutex
	for {
		_ = conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			return // 超时、EOF、坏包统一断连（坏包时流已污染，不可恢复）
		}
		if msg.Header.Type == protocol.MsgHeartbeat {
			continue
		}
		go s.handleRequest(conn, &wmu, msg)
	}
}

// handleRequest 处理一条请求：解码 → 查表 → 反射调用 → 把结果写回。
func (s *Server) handleRequest(conn net.Conn, wmu *sync.Mutex, msg *protocol.Message) {
	seq, codec := msg.Header.Seq, msg.Header.Codec

	// 业务方法是用户代码，panic 不接住会带崩整个进程（见 panicdemo 实验）。
	// recover 只在 defer 的函数里生效；接住后照样要回响应，调用方还在等这个 seq。
	defer func() {
		if r := recover(); r != nil {
			writeError(conn, wmu, seq, codec, "internal error: %v", r)
		}
	}()

	var req Request
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		writeError(conn, wmu, seq, codec, "bad request: %v", err)
		return
	}
	svcName, mName, ok := strings.Cut(req.Method, ".")
	if !ok {
		writeError(conn, wmu, seq, codec, "malformed method %q, want \"Service.Method\"", req.Method)
		return
	}

	s.mu.RLock()
	svc := s.services[svcName]
	s.mu.RUnlock()
	if svc == nil {
		writeError(conn, wmu, seq, codec, "service %q not found", svcName)
		return
	}
	mt := svc.methods[mName]
	if mt == nil {
		writeError(conn, wmu, seq, codec, "method %q not found", req.Method)
		return
	}

	// reflect.New(T) 返回 *T，恰好满足"参数必须是指针"的约定，
	// 也让 json.Unmarshal 有地方可写
	argv := reflect.New(mt.argType.Elem())
	replyv := reflect.New(mt.replyType.Elem())
	if len(req.Args) > 0 {
		if err := json.Unmarshal(req.Args, argv.Interface()); err != nil {
			writeError(conn, wmu, seq, codec, "bad args: %v", err)
			return
		}
	}

	// Func.Call 的第 0 个参数是接收者——"方法"本质上就是第一个参数
	// 固定为接收者的函数
	rets := mt.method.Func.Call([]reflect.Value{svc.rcvr, argv, replyv})
	if errv := rets[0].Interface(); errv != nil {
		// 业务错误照原样透传给调用方，seq 一并带回
		writeError(conn, wmu, seq, codec, "%v", errv.(error))
		return
	}

	result, err := json.Marshal(replyv.Interface())
	if err != nil {
		writeError(conn, wmu, seq, codec, "marshal reply: %v", err)
		return
	}
	_ = writeMsg(conn, wmu, seq, codec, &Response{Result: result})
}

// writeMsg 在写锁保护下把响应写回连接。
func writeMsg(conn net.Conn, wmu *sync.Mutex, seq uint64, codec protocol.CodecType, resp *Response) error {
	body, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	wmu.Lock()
	defer wmu.Unlock()
	return protocol.WriteMessage(conn, &protocol.Message{
		Header: protocol.Header{Codec: codec, Type: protocol.MsgResponse, Seq: seq},
		Body:   body,
	})
}

func writeError(conn net.Conn, wmu *sync.Mutex, seq uint64, codec protocol.CodecType, format string, a ...any) {
	_ = writeMsg(conn, wmu, seq, codec, &Response{Error: fmt.Sprintf(format, a...)})
}
