package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"minirpc/breaker"
	"minirpc/registry"
	"minirpc/server"
)

// Ident.Who 返回实例自己的编号，用来观察负载均衡把请求打到了谁身上。
type Ident struct{ ID int }

func (s *Ident) Who(args *Args, reply *int) error {
	*reply = s.ID
	return nil
}

func startIdent(t *testing.T, id int) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	s := server.New()
	if err := s.Register(&Ident{ID: id}); err != nil {
		t.Fatal(err)
	}
	go s.Serve(lis)
	return lis.Addr().String()
}

// 一致性哈希路由：同一 key 的请求永远命中同一实例；不同 key 能摊到多个实例。
func TestCallKeyedStickiness(t *testing.T) {
	const ttl = time.Minute
	regSrv := httptest.NewServer(registry.New(ttl))
	defer regSrv.Close()

	addr1, addr2 := startIdent(t, 1), startIdent(t, 2)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go registry.KeepAlive(ctx, regSrv.URL, "Ident", addr1, ttl)
	go registry.KeepAlive(ctx, regSrv.URL, "Ident", addr2, ttl)
	time.Sleep(100 * time.Millisecond)

	x := NewXClient(NewDiscovery(regSrv.URL, "Ident", time.Minute))
	defer x.Close()

	// 粘性：room-42 连打 10 次，必须都落在同一个实例
	var first int
	if err := x.CallKeyed(context.Background(), "room-42", "Ident.Who", &Args{}, &first); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		var id int
		if err := x.CallKeyed(context.Background(), "room-42", "Ident.Who", &Args{}, &id); err != nil {
			t.Fatal(err)
		}
		if id != first {
			t.Fatalf("key room-42 jumped from instance %d to %d", first, id)
		}
	}

	// 打散：足够多不同的 key，两个实例都应该接到过流量
	seen := map[int]bool{}
	for i := 0; i < 50; i++ {
		var id int
		key := fmt.Sprintf("room-%d", i)
		if err := x.CallKeyed(context.Background(), key, "Ident.Who", &Args{}, &id); err != nil {
			t.Fatal(err)
		}
		seen[id] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("keys should spread across both instances, seen: %v", seen)
	}
}

// 实例死透（端口没人听）时：前几次调用吃 dial 失败，随后熔断器打开，
// 后续调用不再碰网络、直接秒拒 ErrOpen。
func TestBreakerFailFast(t *testing.T) {
	const ttl = time.Minute
	regSrv := httptest.NewServer(registry.New(ttl))
	defer regSrv.Close()

	// 拿一个真实端口然后立刻关掉 → 一定拨不通的"死实例"
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go registry.KeepAlive(ctx, regSrv.URL, "Ident", deadAddr, ttl)
	time.Sleep(50 * time.Millisecond)

	x := NewXClient(NewDiscovery(regSrv.URL, "Ident", time.Minute))
	defer x.Close()

	// 只有一个实例，每次 Call 内部重试 3 次 → 一次 Call 就攒满 3 次失败，熔断
	var id int
	if err := x.Call(context.Background(), "Ident.Who", &Args{}, &id); err == nil {
		t.Fatal("want dial error, got nil")
	}

	// 熔断已打开：这次调用应该拿到 ErrOpen（本地秒拒，不再拨号）
	start := time.Now()
	err = x.Call(context.Background(), "Ident.Who", &Args{}, &id)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("want breaker.ErrOpen, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("fail-fast took %v, should be near-instant", elapsed)
	}
}

// 端到端：两个实例注册进注册中心 → XClient 轮询打散请求；
// 一个实例心跳停止 → 租约过期 → 流量全部切到幸存者（自动摘除故障节点）。
func TestRoundRobinAndFailover(t *testing.T) {
	const ttl = 300 * time.Millisecond
	regSrv := httptest.NewServer(registry.New(ttl))
	defer regSrv.Close()

	addr1, addr2 := startIdent(t, 1), startIdent(t, 2)

	ctx1, stop1 := context.WithCancel(context.Background())
	defer stop1()
	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	go registry.KeepAlive(ctx1, regSrv.URL, "Ident", addr1, ttl)
	go registry.KeepAlive(ctx2, regSrv.URL, "Ident", addr2, ttl)
	time.Sleep(100 * time.Millisecond) // 等两个 KeepAlive 完成首次注册

	x := NewXClient(NewDiscovery(regSrv.URL, "Ident", 100*time.Millisecond))
	defer x.Close()

	// 阶段 1: round-robin —— 4 次调用应交替命中两个实例
	var ids []int
	for range 4 {
		var id int
		if err := x.Call(context.Background(), "Ident.Who", &Args{}, &id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if ids[0] == ids[1] || ids[0] != ids[2] || ids[1] != ids[3] {
		t.Fatalf("want alternating instances, got %v", ids)
	}

	// 阶段 2: failover —— 实例 1 心跳停止，等租约过期 + 发现缓存刷新
	stop1()
	time.Sleep(ttl + 200*time.Millisecond)

	for range 4 {
		var id int
		if err := x.Call(context.Background(), "Ident.Who", &Args{}, &id); err != nil {
			t.Fatal(err)
		}
		if id != 2 {
			t.Fatalf("instance 1 should be evicted, but got answer from %d", id)
		}
	}
}
