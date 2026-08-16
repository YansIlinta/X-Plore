// logic 是 goim 式架构的逻辑层：接收 comet 转发来的上行弹幕，做敏感词过滤、
// 生成全局唯一 msg_id，再 produce 到 Kafka（key=roomID，同房间进同一 partition 保序）。
// 无状态，可按 CPU 水平扩容；comet 用一致性哈希把同房间上行路由到固定 logic 实例。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
	"github.com/YansIlinta/danmu-distributed/registry"
)

type logicServer struct {
	pb.UnimplementedLogicServiceServer
	id       string
	filter   *core.SensitiveFilter
	writer   *kafka.Writer
	msgIDSeq atomic.Uint64
	tracer   *core.TraceRecorder

	// 观测计数器（HTTP /api/v1/stats）；kafkaProduceErrs 由 main 注入，
	// 在 kafka.Writer 的 ErrorLogger 回调里递增。
	onmessageTotal   atomic.Int64
	filteredTotal    atomic.Int64
	onmessageErrs    atomic.Int64
	kafkaProduceErrs *atomic.Int64
}

func (s *logicServer) nextMsgID() string {
	return s.id + "-" + strconv.FormatUint(s.msgIDSeq.Add(1), 10)
}

// OnMessage 过滤 → 生成 msg_id → 写 Kafka。返回 msg_id 供 comet 回执/观测。
func (s *logicServer) OnMessage(ctx context.Context, req *pb.OnMessageReq) (*pb.OnMessageResp, error) {
	s.onmessageTotal.Add(1)
	filtered := s.filter.Filter(req.Content)
	if filtered != req.Content {
		s.filteredTotal.Add(1) // 命中敏感词、内容被改写
	}
	msgID := s.nextMsgID()

	msg := core.Message{
		Type:         "danmu",
		MsgID:        msgID,
		RoomID:       req.RoomId,
		UID:          req.Uid,
		Content:      filtered,
		ClientTS:     req.ClientTs,
		ClientTSNano: req.ClientTsNano,
		ServerTS:     time.Now().UnixMilli(),
		SourceServer: req.SourceComet,
		OffsetMS:     req.OffsetMs,
	}
	value, err := json.Marshal(&msg)
	if err != nil {
		s.onmessageErrs.Add(1)
		return nil, fmt.Errorf("marshal: %w", err)
	}

	// 采样决策在这里做——logic 是 msg_id 的唯一生成方，也是链路上第一个能判定的点。
	// 命中就把 msg_id 写进 Kafka header，下游 job 读 header 即可，无需反序列化 payload。
	km := kafka.Message{Key: []byte(req.RoomId), Value: value}
	sampled := s.tracer.Sampled(msgID)
	if sampled {
		km.Headers = []kafka.Header{{Key: core.TraceHeaderKey, Value: []byte(msgID)}}
	}

	// Async writer：非阻塞入队，错误走 ErrorLogger；不阻塞 comet 的上行 RPC。
	if err := s.writer.WriteMessages(ctx, km); err != nil {
		s.onmessageErrs.Add(1)
		return nil, fmt.Errorf("kafka write: %w", err)
	}
	if sampled {
		// 注意：Async writer 下这个时刻是"交给 writer 入队"，不是"broker 已确认"。
		// 真实的 produce 耗时不在这条 span 里，别拿它当 Kafka 写入延迟看。
		s.tracer.Record(msgID, core.HopLogicProduce, req.RoomId, "enqueued", time.Now().UnixNano())
	}
	return &pb.OnMessageResp{MsgId: msgID, Filtered: filtered}, nil
}

func main() {
	addr := flag.String("addr", ":7400", "gRPC listen address")
	id := flag.String("id", "logic1", "logic instance id (用于 msg_id 前缀)")
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("kafka-topic", "danmu-broadcast", "Kafka topic")
	registryURL := flag.String("registry", "", "registry base URL；配置后注册 logic 服务供 comet 发现")
	advertise := flag.String("advertise", "", "对外可达的 gRPC 地址(host:port)，默认由 addr 推导")
	httpAddr := flag.String("http-addr", ":7410", "HTTP 观测 listen address（/health、/api/v1/stats）")
	advertiseHTTP := flag.String("advertise-http", "", "对外可达的 HTTP 观测地址(host:port)，默认由 advertise 主机名 + http-addr 端口推导")
	traceRate := flag.Uint("trace-sample", 100, "消息 trace 采样率：1/N 采样，0=关闭")
	traceBuf := flag.Int("trace-buffer", 512, "trace span 环形缓冲条数")
	flag.Parse()

	startTime := time.Now()
	var kafkaProduceErrs atomic.Int64

	brokers := splitComma(*kafkaBrokers)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        *kafkaTopic,
		Balancer:     &kafka.Hash{}, // key=roomID → 同房间同 partition，保序
		BatchSize:    500,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  3,
		WriteTimeout: 5 * time.Second,
		ErrorLogger: kafka.LoggerFunc(func(m string, a ...interface{}) {
			kafkaProduceErrs.Add(1) // 观测：异步 produce 失败计数
			log.Printf("[kafka] "+m, a...)
		}),
	}
	defer writer.Close()

	ls := &logicServer{
		id:               *id,
		filter:           core.NewSensitiveFilter(core.DefaultSensitiveWords),
		writer:           writer,
		kafkaProduceErrs: &kafkaProduceErrs,
		tracer:           core.NewTraceRecorder(*id, uint32(*traceRate), *traceBuf),
	}
	// msg_id 序列用进程启动时刻播种：重启后 seq 归零也不会与旧 msg_id 重复
	// （前端按 msg_id 去重，跨重启重复会造成已显示过的弹幕再次出现）。
	ls.msgIDSeq.Store(uint64(time.Now().UnixNano()))
	srv := grpc.NewServer()
	pb.RegisterLogicServiceServer(srv, ls)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[logic] listen: %v", err)
	}
	log.Printf("[logic] id=%s gRPC listening on %s, kafka topic=%s", *id, *addr, *kafkaTopic)

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			// ErrServerStopped 是 GracefulStop 后的正常返回；误判成致命错误会
			// log.Fatalf 提前 os.Exit(1)，跳过 defer writer.Close() 丢在途 Kafka 消息
			log.Fatalf("[logic] serve: %v", err)
		}
	}()

	// HTTP 观测面：/health + /api/v1/stats（独立 goroutine，不阻塞 gRPC）
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
			"server_id":            *id,
			"uptime_ms":            time.Since(startTime).Milliseconds(),
			"onmessage_total":      ls.onmessageTotal.Load(),
			"filtered_total":       ls.filteredTotal.Load(),
			"onmessage_errors":     ls.onmessageErrs.Load(),
			"kafka_produce_errors": ls.kafkaProduceErrs.Load(),
			"goroutines":           runtime.NumGoroutine(),
			"heap_mb":              mem.HeapAlloc / 1024 / 1024,
		})
	})
	httpMux.HandleFunc("/api/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node":  *id,
			"stats": ls.tracer.Stats(),
			"spans": ls.tracer.Recent(core.TraceLimit(r)),
		})
	})
	go func() {
		log.Printf("[logic] http observability listening on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, httpMux); err != nil {
			log.Printf("[logic] http serve: %v", err)
		}
	}()

	// 注册到 registry 供 comet 一致性哈希发现
	if *registryURL != "" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go registry.KeepAlive(ctx, *registryURL, "logic", advertiseAddr(*advertise, *addr), 10*time.Second)
		// 额外注册 logic-http（HTTP 观测地址），供 Ops Console 经 registry 发现
		go registry.KeepAlive(ctx, *registryURL, "logic-http", advertiseHTTPAddr(*advertiseHTTP, *advertise, *httpAddr), 10*time.Second)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[logic] shutting down")
	srv.GracefulStop()
}

// advertiseAddr 返回对外可达地址：显式 advertise 优先，否则把 ":7400" 推成 "localhost:7400"。
func advertiseAddr(advertise, listen string) string {
	if advertise != "" {
		return advertise
	}
	if len(listen) > 0 && listen[0] == ':' {
		return "localhost" + listen
	}
	return listen
}

// advertiseHTTPAddr 推导 HTTP 观测地址：显式 advertise-http 优先；
// 否则取 advertise 的主机名 + http-addr 的端口；advertise 为空则主机名用 localhost。
func advertiseHTTPAddr(advertiseHTTP, advertise, httpAddr string) string {
	if advertiseHTTP != "" {
		return advertiseHTTP
	}
	host := "localhost"
	if advertise != "" {
		if h, _, err := net.SplitHostPort(advertise); err == nil && h != "" {
			host = h
		}
	}
	if _, port, err := net.SplitHostPort(httpAddr); err == nil && port != "" {
		return net.JoinHostPort(host, port)
	}
	return host + httpAddr
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
	out = append(out, s[start:])
	return out
}
