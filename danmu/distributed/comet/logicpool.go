package main

import (
	"log"
	"net/http"
	"sync"

	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/YansIlinta/danmu-distributed/lb"

	"github.com/YansIlinta/danmu-distributed/pb"
)

// logicPool 维护到各 logic 实例的 gRPC 连接，并用一致性哈希环按 roomID 路由，
// 使同一房间的上行总落到同一 logic 实例（便于 logic 侧做每房间聚合/限流）。
type logicPool struct {
	registryURL string
	static      []string

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.LogicServiceClient
	ring    *lb.Ring
}

func newLogicPool(registryURL string, static []string) *logicPool {
	p := &logicPool{
		registryURL: registryURL,
		static:      static,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.LogicServiceClient),
		ring:        lb.NewRing(100),
	}
	if len(static) > 0 {
		p.apply(static)
	} else if registryURL != "" {
		p.refresh()
	}
	return p
}

func (p *logicPool) refresh() {
	if len(p.static) > 0 {
		return // 静态配置不刷新
	}
	addrs, err := fetchService(p.registryURL, "logic")
	if err != nil {
		log.Printf("[comet] registry fetch logic error: %v", err)
		return
	}
	p.apply(addrs)
}

func (p *logicPool) apply(addrs []string) {
	alive := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		alive[a] = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range addrs {
		if _, ok := p.conns[a]; ok {
			continue
		}
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[comet] dial logic %s: %v", a, err)
			continue
		}
		p.conns[a] = conn
		p.clients[a] = pb.NewLogicServiceClient(conn)
	}
	for a, conn := range p.conns {
		if !alive[a] {
			conn.Close()
			delete(p.conns, a)
			delete(p.clients, a)
		}
	}
	p.ring.Reset(addrs)
}

// forRoom 返回 roomID 一致性哈希归属的 logic 客户端。
func (p *logicPool) forRoom(roomID string) (pb.LogicServiceClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	addr := p.ring.Get(roomID)
	if addr == "" {
		return nil, false
	}
	c, ok := p.clients[addr]
	return c, ok
}

func (p *logicPool) empty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients) == 0
}

func fetchService(registryURL, service string) ([]string, error) {
	resp, err := http.Get(registryURL + "/services?service=" + service)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var addrs []string
	if err := json.NewDecoder(resp.Body).Decode(&addrs); err != nil {
		return nil, err
	}
	return addrs, nil
}
