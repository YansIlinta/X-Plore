package main

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/YansIlinta/danmu-distributed/etcdreg"
	"github.com/YansIlinta/danmu-distributed/internal/testetcd"
	"github.com/YansIlinta/danmu-distributed/pb"
)

// mockLogic 记录 OnMessage 命中次数并回显一个固定 msg_id。
type mockLogic struct {
	pb.UnimplementedLogicServiceServer
	hits atomic.Int64
}

func (m *mockLogic) OnMessage(_ context.Context, req *pb.OnMessageReq) (*pb.OnMessageResp, error) {
	m.hits.Add(1)
	return &pb.OnMessageResp{MsgId: "m-" + strconv.FormatInt(m.hits.Load(), 10), Filtered: req.Content}, nil
}

// 起一个 mock logic gRPC 服务，返回其监听地址与实例。
func serveMockLogic(t *testing.T) (string, *mockLogic) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	m := &mockLogic{}
	pb.RegisterLogicServiceServer(srv, m)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), m
}

// 端到端验证替换后的 comet→logic 路由：etcd 注册 → naming/resolver 发现 →
// round_robin 在两个 logic 实例间分发（上行弹幕不再按房间粘性）。
func TestLogicPoolRoundRobinViaEtcd(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a1, m1 := serveMockLogic(t)
	a2, m2 := serveMockLogic(t)
	if _, err := etcdreg.Register(ctx, cli, "logic", a1, 10*time.Second); err != nil {
		t.Fatalf("register %s: %v", a1, err)
	}
	if _, err := etcdreg.Register(ctx, cli, "logic", a2, 10*time.Second); err != nil {
		t.Fatalf("register %s: %v", a2, err)
	}

	pool, err := newLogicPool(cli)
	if err != nil {
		t.Fatalf("newLogicPool: %v", err)
	}
	defer pool.close()

	// 等 resolver 拉到两个端点再开始打请求。
	deadline := time.Now().Add(10 * time.Second)
	for {
		addrs, err := etcdreg.List(ctx, cli, "logic")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(addrs) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("etcd 中 logic 地址数=%d, want 2", len(addrs))
		}
		time.Sleep(100 * time.Millisecond)
	}

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			resp, err := pool.cli.OnMessage(rctx, &pb.OnMessageReq{RoomId: "r", Content: "hi"})
			if err != nil {
				t.Errorf("OnMessage: %v", err)
				return
			}
			if resp.MsgId == "" || resp.Filtered != "hi" {
				t.Errorf("resp=%+v", resp)
			}
		}()
	}
	wg.Wait()

	// round_robin 生效：两个实例都必须吃到请求，且总数守恒。
	h1, h2 := m1.hits.Load(), m2.hits.Load()
	if h1 == 0 || h2 == 0 {
		t.Fatalf("负载未分摊: logic1=%d logic2=%d", h1, h2)
	}
	if h1+h2 != n {
		t.Fatalf("命中总数 %d+%d != %d", h1, h2, n)
	}
}
