// Package testetcd 为集成测试起一个进程内 embed etcd，避免测试依赖外部 etcd 二进制。
package testetcd

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// Start 起一个单节点 embed etcd，返回客户端 URL、Client 与清理函数。
func Start(t *testing.T) (string, *clientv3.Client, func()) {
	t.Helper()

	clientPort := freePort(t)
	peerPort := freePort(t)
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

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		e.Close()
		t.Fatalf("etcd client: %v", err)
	}
	return clientURL.String(), cli, func() {
		_ = cli.Close()
		e.Close()
	}
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
