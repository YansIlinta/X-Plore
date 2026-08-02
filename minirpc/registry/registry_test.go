package registry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func register(t *testing.T, base, service, addr string) {
	t.Helper()
	resp, err := http.PostForm(base+"/register", url.Values{"service": {service}, "addr": {addr}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func list(t *testing.T, base, service string) []string {
	t.Helper()
	resp, err := http.Get(base + "/services?service=" + service)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var addrs []string
	if err := json.NewDecoder(resp.Body).Decode(&addrs); err != nil {
		t.Fatal(err)
	}
	return addrs
}

func TestRegisterAndList(t *testing.T) {
	srv := httptest.NewServer(New(time.Second))
	defer srv.Close()

	register(t, srv.URL, "Arith", "127.0.0.1:9001")
	register(t, srv.URL, "Arith", "127.0.0.1:9002")
	register(t, srv.URL, "Other", "127.0.0.1:9999")

	addrs := list(t, srv.URL, "Arith")
	if len(addrs) != 2 || addrs[0] != "127.0.0.1:9001" || addrs[1] != "127.0.0.1:9002" {
		t.Fatalf("got %v", addrs)
	}
}

// 心跳停止 → 租约过期 → 实例从列表消失（僵尸节点自动摘除）。
func TestLeaseExpiry(t *testing.T) {
	srv := httptest.NewServer(New(150 * time.Millisecond))
	defer srv.Close()

	register(t, srv.URL, "Arith", "127.0.0.1:9001")
	if got := list(t, srv.URL, "Arith"); len(got) != 1 {
		t.Fatalf("before expiry: got %v", got)
	}

	time.Sleep(250 * time.Millisecond) // 不续租，等租约过期
	if got := list(t, srv.URL, "Arith"); len(got) != 0 {
		t.Fatalf("after expiry: got %v, want empty", got)
	}
}

// 持续心跳则一直存活。
func TestHeartbeatKeepsAlive(t *testing.T) {
	srv := httptest.NewServer(New(150 * time.Millisecond))
	defer srv.Close()

	for i := 0; i < 5; i++ {
		register(t, srv.URL, "Arith", "127.0.0.1:9001") // 每 50ms 续一次
		time.Sleep(50 * time.Millisecond)
	}
	// 总时长 250ms > ttl 150ms，但因为一直续租，应该还活着
	if got := list(t, srv.URL, "Arith"); len(got) != 1 {
		t.Fatalf("got %v, want 1 alive instance", got)
	}
}
