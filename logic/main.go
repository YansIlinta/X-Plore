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
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"

	"minirpc/registry"

	"danmu/core"
	"danmu/pb"
)

type logicServer struct {
	pb.UnimplementedLogicServiceServer
	id       string
	filter   *core.SensitiveFilter
	writer   *kafka.Writer
	msgIDSeq atomic.Uint64
}

func (s *logicServer) nextMsgID() string {
	return s.id + "-" + strconv.FormatUint(s.msgIDSeq.Add(1), 10)
}

// OnMessage 过滤 → 生成 msg_id → 写 Kafka。返回 msg_id 供 comet 回执/观测。
func (s *logicServer) OnMessage(ctx context.Context, req *pb.OnMessageReq) (*pb.OnMessageResp, error) {
	filtered := s.filter.Filter(req.Content)
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
		return nil, fmt.Errorf("marshal: %w", err)
	}
	// Async writer：非阻塞入队，错误走 ErrorLogger；不阻塞 comet 的上行 RPC。
	if err := s.writer.WriteMessages(ctx, kafka.Message{Key: []byte(req.RoomId), Value: value}); err != nil {
		return nil, fmt.Errorf("kafka write: %w", err)
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
	flag.Parse()

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
		ErrorLogger:  kafka.LoggerFunc(func(m string, a ...interface{}) { log.Printf("[kafka] "+m, a...) }),
	}
	defer writer.Close()

	srv := grpc.NewServer()
	pb.RegisterLogicServiceServer(srv, &logicServer{
		id:     *id,
		filter: core.NewSensitiveFilter(core.DefaultSensitiveWords),
		writer: writer,
	})

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[logic] listen: %v", err)
	}
	log.Printf("[logic] id=%s gRPC listening on %s, kafka topic=%s", *id, *addr, *kafkaTopic)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("[logic] serve: %v", err)
		}
	}()

	// 注册到 registry 供 comet 一致性哈希发现
	if *registryURL != "" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go registry.KeepAlive(ctx, *registryURL, "logic", advertiseAddr(*advertise, *addr), 10*time.Second)
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
