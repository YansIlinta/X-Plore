package etcdreg

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/YansIlinta/danmu-distributed/internal/testetcd"
)

// startEmbedEtcd 在指定端口起一个单节点 embed etcd（供「后起 etcd」场景复用端口）。
func startEmbedEtcd(t *testing.T, clientPort, peerPort int) (string, *embed.Etcd, func()) {
	t.Helper()
	clientURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", clientPort))
	peerURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", peerPort))

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = "testetcd"
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.LogLevel = "fatal"

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("embed etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		e.Close()
		t.Fatalf("etcd 15s 内未就绪")
	}
	return clientURL.String(), e, func() { e.Close() }
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// Register→List 基本往返：注册两个地址，按序取回。
func TestRegisterList(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := Register(ctx, cli, "comet", "localhost:7501", 10*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := Register(ctx, cli, "comet", "localhost:7500", 10*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}

	addrs, err := List(ctx, cli, "comet")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(addrs) != 2 || addrs[0] != "localhost:7500" || addrs[1] != "localhost:7501" {
		t.Fatalf("addrs=%v", addrs)
	}
}

// ctx 取消 → 主动 Revoke → 地址立即从发现面消失（不必等租约过期）。
func TestRegisterRevokeOnCancel(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := Register(ctx, cli, "logic", "localhost:7400", 10*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}
	cancel() // 触发 keepalive 退出 + Revoke

	deadline := time.Now().Add(5 * time.Second)
	for {
		addrs, err := List(context.Background(), cli, "logic")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(addrs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("revoke 后地址仍在: %v", addrs)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ListAll 跨服务枚举（Ops Console 拓扑入口）。
func TestListAll(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()
	ctx := context.Background()

	for _, s := range []struct{ svc, addr string }{
		{"comet", "a:1"}, {"comet", "b:1"}, {"logic", "l:1"}, {"job-http", "j:1"},
	} {
		if _, err := Register(ctx, cli, s.svc, s.addr, 10*time.Second); err != nil {
			t.Fatalf("register %s/%s: %v", s.svc, s.addr, err)
		}
	}

	all, err := ListAll(ctx, cli)
	if err != nil {
		t.Fatalf("listall: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all=%v", all)
	}
	if len(all["comet"]) != 2 || all["comet"][0] != "a:1" || all["comet"][1] != "b:1" {
		t.Fatalf("comet=%v", all["comet"])
	}
}

// Watch：初始空列表回调一次，注册后回调新列表。
func TestWatch(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan []string, 16)
	go Watch(ctx, cli, "comet", func(addrs []string) { updates <- addrs })

	select {
	case a := <-updates:
		if len(a) != 0 {
			t.Fatalf("initial addrs=%v, want empty", a)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("未收到初始回调")
	}

	if _, err := Register(ctx, cli, "comet", "localhost:7500", 10*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}
	select {
	case a := <-updates:
		if len(a) != 1 || a[0] != "localhost:7500" {
			t.Fatalf("watch addrs=%v", a)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("watch 未感知新注册")
	}
}

// 与官方 naming/endpoints 的读取兼容性：注册的 value 必须能被
// endpoints.Manager 解析出 Addr（comet→logic 的 resolver 消费同一份数据）。
func TestRegisterCompatibleWithNamingEndpoints(t *testing.T) {
	_, cli, cleanup := testetcd.Start(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := Register(ctx, cli, "logic", "localhost:7400", 10*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}

	em, err := endpoints.NewManager(cli, "danmu/services/logic")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	eps, err := em.List(ctx)
	if err != nil {
		t.Fatalf("manager list: %v", err)
	}
	found := false
	for key, ep := range eps {
		if ep.Addr == "localhost:7400" {
			found = true
			_ = key
		}
	}
	if !found {
		t.Fatalf("官方 endpoints.Manager 未解析出 Addr: %v", eps)
	}
}

// KeepAlive 自愈：etcd 晚于服务就绪时先重试，etcd 起来后自动注册成功。
// 对应 compose 里 depends_on 不保证 etcd 就绪的启动竞态（同原 registry.KeepAlive 语义）。
func TestKeepAliveRetriesUntilEtcdUp(t *testing.T) {
	clientPort := freePort(t)
	peerPort := freePort(t)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://127.0.0.1:%d", clientPort)},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go KeepAlive(ctx, cli, "comet", "localhost:7500", 10*time.Second)

	time.Sleep(500 * time.Millisecond) // 让 KeepAlive 先撞上「etcd 未就绪」几轮

	// etcd 现在才起来
	_, e, cleanup := startEmbedEtcd(t, clientPort, peerPort)
	defer cleanup()
	_ = e

	deadline := time.Now().Add(8 * time.Second)
	for {
		addrs, err := List(ctx, cli, "comet")
		if err == nil && len(addrs) == 1 && addrs[0] == "localhost:7500" {
			return // 自愈成功
		}
		if time.Now().After(deadline) {
			t.Fatalf("KeepAlive 未在 etcd 就绪后自动注册: addrs=%v err=%v", addrs, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
