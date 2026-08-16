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
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
	"github.com/YansIlinta/danmu-distributed/registry"
)

const flushWindow = 10 * time.Millisecond

// tracer 是进程级 trace 记录器，main 里按 flag 初始化。
// job 不做采样判定——它读 logic 写在 Kafka header 里的结果，热路径上不碰 payload。
var tracer *core.TraceRecorder

// 观测计数器（HTTP /api/v1/stats）。单进程单实例，用包级原子计数最省事。
var (
	statConsumed  atomic.Int64 // 消费 Kafka 消息数
	statPushOK    atomic.Int64 // PushRoom RPC 成功次数
	statPushErr   atomic.Int64 // PushRoom RPC 失败次数
	statDelivered atomic.Int64 // PushRoomResp.delivered 累计投递数
)

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
// traceIDs 是本批中命中采样的 msg_id：经 gRPC metadata 透传给 comet，
// 让它能在投递环节记 span——PushRoom 的 proto 契约因此不用动。
func (p *cometPool) pushRoom(roomID string, payload []byte, traceIDs []string) {
	clients := p.snapshot()
	var delivered int64
	for _, c := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if len(traceIDs) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, core.TraceMetadataKey, strings.Join(traceIDs, ","))
		}
		resp, err := c.PushRoom(ctx, &pb.PushRoomReq{RoomId: roomID, Payload: payload})
		cancel()
		if err != nil {
			statPushErr.Add(1)
			log.Printf("[job] pushRoom room=%s error: %v", roomID, err)
			continue
		}
		statPushOK.Add(1)
		statDelivered.Add(int64(resp.Delivered))
		delivered += int64(resp.Delivered)
	}
	// 扇出完成才记 span：一条 msg 对应一条 job.push，detail 汇总本次扇出结果。
	if len(traceIDs) > 0 {
		now := time.Now().UnixNano()
		detail := "comets=" + strconv.Itoa(len(clients)) + " delivered=" + strconv.FormatInt(delivered, 10)
		for _, id := range traceIDs {
			tracer.Record(id, core.HopJobPush, roomID, detail, now)
		}
	}
}

// addrs 返回当前 comet 地址列表（观测用，需读锁）。
func (p *cometPool) addrs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.conns))
	for a := range p.conns {
		out = append(out, a)
	}
	return out
}

func main() {
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("kafka-topic", "danmu-broadcast", "Kafka topic")
	registryURL := flag.String("registry", "http://localhost:7350", "registry base URL")
	httpAddr := flag.String("http-addr", ":7420", "HTTP 观测 listen address（/health、/api/v1/stats）")
	advertiseHTTP := flag.String("advertise-http", "", "对外可达的 HTTP 观测地址(host:port)，默认 localhost + http-addr 端口")
	traceRate := flag.Uint("trace-sample", 100, "消息 trace 采样率：1/N 采样，0=关闭。须与 logic 一致才有意义")
	traceBuf := flag.Int("trace-buffer", 512, "trace span 环形缓冲条数")
	flag.Parse()

	// job 自身不判采样，rate 只用于 Enabled() 开关与 /api/v1/traces 的自述。
	tracer = core.NewTraceRecorder("job", uint32(*traceRate), *traceBuf)

	startTime := time.Now()
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

	// HTTP 观测面：/health + /api/v1/stats。放在 Kafka 消费循环之前监听，
	// 即使 Kafka 不可达也能回答问题（消费循环会自行重试）。
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	httpMux.HandleFunc("/api/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"server_id":       "job",
			"uptime_ms":       time.Since(startTime).Milliseconds(),
			"consumed_total":  statConsumed.Load(),
			"push_ok_total":   statPushOK.Load(),
			"push_err_total":  statPushErr.Load(),
			"delivered_total": statDelivered.Load(),
			"comets":          pool.addrs(),
			"goroutines":      runtime.NumGoroutine(),
			"heap_mb":         mem.HeapAlloc / 1024 / 1024,
		})
	})
	httpMux.HandleFunc("/api/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node":  "job",
			"stats": tracer.Stats(),
			"spans": tracer.Recent(core.TraceLimit(r)),
		})
	})
	go func() {
		log.Printf("[job] http observability listening on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, httpMux); err != nil {
			log.Printf("[job] http serve: %v", err)
		}
	}()

	// 注册 job-http（HTTP 观测地址），供 Ops Console 经 registry 发现。
	// job 的 RPC 面是「主动调用方」，原本不注册；这里只为观测发现注册 HTTP 地址。
	if *registryURL != "" {
		go registryKeepAlive(ctx, *registryURL, "job-http", advertiseHTTPAddr(*advertiseHTTP, *httpAddr))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; log.Println("[job] shutdown"); cancel() }()

	log.Printf("[job] consuming %s, registry=%s", *kafkaTopic, *registryURL)

	// 按房间聚合一个 flushWindow 的消息再推，减少 RPC 次数（对齐单体的 worker 批聚合）。
	roomBatch := make(map[string]*batch)
	timer := time.NewTimer(flushWindow)
	defer timer.Stop()

	flush := func() {
		for roomID, b := range roomBatch {
			payload := buildArray(b.values)
			pool.pushRoom(roomID, payload, b.traceIDs)
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
			statConsumed.Add(1)
			roomID := string(m.Key) // logic 用 roomID 作 key，无需反序列化
			b := roomBatch[roomID]
			if b == nil {
				b = &batch{}
				roomBatch[roomID] = b
			}
			b.values = append(b.values, m.Value)
			// 只扫 header，不解 payload：非采样消息在这条路径上零额外成本。
			if id := traceIDOf(m); id != "" {
				b.traceIDs = append(b.traceIDs, id)
				tracer.Record(id, core.HopJobConsume, roomID, "", time.Now().UnixNano())
			}
		case <-timer.C:
			flush()
			timer.Reset(flushWindow)
		}
	}
}

// batch 是一个 flushWindow 内、同房间待推的消息，以及其中命中采样的 msg_id。
type batch struct {
	values   [][]byte
	traceIDs []string
}

// traceIDOf 从 Kafka header 取采样标记（值即 msg_id）；未命中采样返回空串。
func traceIDOf(m kafka.Message) string {
	if !tracer.Enabled() {
		return ""
	}
	for _, h := range m.Headers {
		if h.Key == core.TraceHeaderKey {
			return string(h.Value)
		}
	}
	return ""
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

// registryKeepAlive 简化包装：隐藏 ttl 细节，统一 10s 租约。
func registryKeepAlive(ctx context.Context, registryURL, service, addr string) {
	registry.KeepAlive(ctx, registryURL, service, addr, 10*time.Second)
}

// advertiseHTTPAddr 推导 HTTP 观测地址：显式 advertise-http 优先，否则 localhost + 端口。
func advertiseHTTPAddr(advertiseHTTP, httpAddr string) string {
	if advertiseHTTP != "" {
		return advertiseHTTP
	}
	if _, port, err := net.SplitHostPort(httpAddr); err == nil && port != "" {
		return net.JoinHostPort("localhost", port)
	}
	return "localhost" + httpAddr
}
