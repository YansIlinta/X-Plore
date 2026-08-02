// job 是 goim 式架构的扇出层：消费 Kafka，从 registry 发现所有 comet，按房间把
// 消息定向 PushRoom 推给每个 comet（comet 本地无该房间即丢弃）。这替代了原单体
// 的 Redis Pub/Sub 跨机广播——Redis Cluster Pub/Sub 每条 publish 广播到每个节点、
// 吞吐随集群规模负向扩展；Kafka→Job→定向 push 是 goim 的做法，扇出与削峰解耦。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"danmu/pb"
)

const flushWindow = 10 * time.Millisecond

// cometPool 维护到各 comet 的 gRPC 连接，按 registry 定期刷新。
type cometPool struct {
	registryURL string
	mu          sync.RWMutex
	conns       map[string]*grpc.ClientConn // addr -> conn
	clients     map[string]pb.CometServiceClient
}

func newCometPool(registryURL string) *cometPool {
	return &cometPool{
		registryURL: registryURL,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.CometServiceClient),
	}
}

// refresh 从 registry 拉取存活 comet 列表，新增拨号、消失关闭。
func (p *cometPool) refresh() {
	addrs, err := fetchService(p.registryURL, "comet")
	if err != nil {
		log.Printf("[job] registry fetch error: %v", err)
		return
	}
	alive := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		alive[a] = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// 新增
	for _, a := range addrs {
		if _, ok := p.conns[a]; ok {
			continue
		}
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[job] dial comet %s: %v", a, err)
			continue
		}
		p.conns[a] = conn
		p.clients[a] = pb.NewCometServiceClient(conn)
		log.Printf("[job] comet added: %s (total=%d)", a, len(p.conns))
	}
	// 消失
	for a, conn := range p.conns {
		if !alive[a] {
			conn.Close()
			delete(p.conns, a)
			delete(p.clients, a)
			log.Printf("[job] comet removed: %s", a)
		}
	}
}

func (p *cometPool) snapshot() []pb.CometServiceClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]pb.CometServiceClient, 0, len(p.clients))
	for _, c := range p.clients {
		out = append(out, c)
	}
	return out
}

// pushRoom 把一个房间的一批消息推给所有 comet（各自本地过滤）。
func (p *cometPool) pushRoom(roomID string, payload []byte) {
	clients := p.snapshot()
	for _, c := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.PushRoom(ctx, &pb.PushRoomReq{RoomId: roomID, Payload: payload})
		cancel()
		if err != nil {
			log.Printf("[job] pushRoom room=%s error: %v", roomID, err)
		}
	}
}

func main() {
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("kafka-topic", "danmu-broadcast", "Kafka topic")
	registryURL := flag.String("registry", "http://localhost:7350", "registry base URL")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newCometPool(*registryURL)
	pool.refresh()
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pool.refresh()
			}
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        splitComma(*kafkaBrokers),
		Topic:          *kafkaTopic,
		GroupID:        "danmu-job",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second, // 广播是 fire-and-forget，at-most-once 可接受（弹幕允许丢）
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; log.Println("[job] shutdown"); cancel() }()

	log.Printf("[job] consuming %s, registry=%s", *kafkaTopic, *registryURL)

	// 按房间聚合一个 flushWindow 的消息再推，减少 RPC 次数（对齐单体的 worker 批聚合）。
	roomBatch := make(map[string][][]byte)
	timer := time.NewTimer(flushWindow)
	defer timer.Stop()

	flush := func() {
		for roomID, values := range roomBatch {
			payload := buildArray(values)
			pool.pushRoom(roomID, payload)
			delete(roomBatch, roomID)
		}
	}

	// 用独立 goroutine 读 Kafka，喂进 channel，主循环 select 聚合窗口。
	msgCh := make(chan kafka.Message, 4096)
	go func() {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[job] read error: %v", err)
				time.Sleep(time.Second)
				continue
			}
			select {
			case msgCh <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			flush()
			log.Println("[job] stopped")
			return
		case m := <-msgCh:
			roomID := string(m.Key) // logic 用 roomID 作 key，无需反序列化
			roomBatch[roomID] = append(roomBatch[roomID], m.Value)
		case <-timer.C:
			flush()
			timer.Reset(flushWindow)
		}
	}
}

// buildArray 把若干条单消息 JSON 包成一个 JSON 数组 payload（与客户端下行格式一致）。
func buildArray(values [][]byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, v := range values {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(v)
	}
	buf.WriteByte(']')
	return buf.Bytes()
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

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
