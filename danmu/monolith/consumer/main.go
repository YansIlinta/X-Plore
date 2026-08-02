package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message 与 server/core 包的 Message 保持一致
type Message struct {
	Type         string `json:"type"`
	MsgID        string `json:"msg_id,omitempty"`
	RoomID       string `json:"room_id,omitempty"`
	UID          string `json:"uid,omitempty"`
	Content      string `json:"content,omitempty"`
	ClientTS     int64  `json:"client_ts,omitempty"`
	ServerTS     int64  `json:"server_ts,omitempty"`
	SourceServer string `json:"source_server,omitempty"`
	OffsetMS     int64  `json:"offset_ms,omitempty"` // 点播进度；CH 表可后续加列
}

func main() {
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("topic", "danmu-history", "Kafka topic")
	chAddr := flag.String("clickhouse-addr", "localhost:9000", "ClickHouse native TCP address")
	chDatabase := flag.String("clickhouse-db", "default", "ClickHouse database")
	chUsername := flag.String("clickhouse-user", "default", "ClickHouse username")
	chPassword := flag.String("clickhouse-password", "", "ClickHouse password")
	mode := flag.String("mode", "storage", "Consumer mode: storage|broadcast")
	flag.Parse()

	brokers := strings.Split(*kafkaBrokers, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[consumer] shutdown signal received")
		cancel()
	}()

	switch *mode {
	case "storage":
		runStorageConsumer(ctx, brokers, *kafkaTopic, *chAddr, *chDatabase, *chUsername, *chPassword)
	case "broadcast":
		runBroadcastConsumer(ctx, brokers, *kafkaTopic)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

// runStorageConsumer 落库消费组：将弹幕写入 ClickHouse
// 消费组 ID: danmu-storage，支持水平扩容和自动 rebalance
func runStorageConsumer(ctx context.Context, brokers []string, topic, chAddr, chDatabase, chUsername, chPassword string) {
	log.Printf("[storage-consumer] starting, topic=%s, clickhouse=%s db=%s", topic, chAddr, chDatabase)

	db, err := NewDB(chAddr, chDatabase, chUsername, chPassword)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// CommitInterval=0 关闭自动提交：offset 必须在 ClickHouse 落库成功后才提交，
	// 避免"读即提交、落库前崩溃"导致 offset 前进但数据丢失（原实现是 at-most-once）。
	// 改为 FetchMessage 读取（不提交）+ 落库成功后 CommitMessages 显式提交，得到 at-least-once。
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "danmu-storage",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	// 单 goroutine 拥有 batch/pending，无需锁：fetch goroutine 只负责把消息喂进 channel
	msgCh := make(chan kafka.Message, 2000)
	go func() {
		for {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[storage-consumer] fetch error: %v", err)
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

	var (
		batch   []Message       // 待落库的解码消息
		pending []kafka.Message // 与 batch 一一对应的原始消息，落库成功后用于提交 offset
	)

	// flush 落库并提交 offset。落库失败则保留缓冲、不提交，下次重试（at-least-once，
	// 依赖 ClickHouse 侧幂等/去重容忍极少量重复）。
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := db.BatchInsert(batch); err != nil {
			log.Printf("[storage-consumer] batch insert error (will retry, offset not committed): %v", err)
			return
		}
		if err := reader.CommitMessages(ctx, pending...); err != nil {
			// 落库已成功但提交失败：下次会重读这批 → 重复插入（可接受的 at-least-once）
			log.Printf("[storage-consumer] commit error: %v", err)
		}
		log.Printf("[storage-consumer] flushed %d messages", len(batch))
		batch = batch[:0]
		pending = pending[:0]
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flush() // 退出前尽力落库剩余
			log.Println("[storage-consumer] stopped")
			return
		case m := <-msgCh:
			var danmu Message
			if err := json.Unmarshal(m.Value, &danmu); err != nil {
				log.Printf("[storage-consumer] unmarshal error: %v", err)
				// 解析失败的坏消息也要提交，否则会卡住该 partition
				if err := reader.CommitMessages(ctx, m); err != nil {
					log.Printf("[storage-consumer] commit poison msg error: %v", err)
				}
				continue
			}
			batch = append(batch, danmu)
			pending = append(pending, m)
			if len(batch) >= 1000 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// runBroadcastConsumer 实时广播消费组（示例：通过 Kafka 做跨机广播的备选方案）
// 消费组 ID: danmu-broadcast
func runBroadcastConsumer(ctx context.Context, brokers []string, topic string) {
	log.Printf("[broadcast-consumer] starting, topic=%s", topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "danmu-broadcast",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[broadcast-consumer] read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var danmu Message
		if err := json.Unmarshal(msg.Value, &danmu); err != nil {
			log.Printf("[broadcast-consumer] unmarshal error: %v", err)
			continue
		}

		// 实际部署中，这里会将消息推送给本机的 WebSocket 连接
		// 本示例仅打印日志
		log.Printf("[broadcast-consumer] room=%s uid=%s content=%s",
			danmu.RoomID, danmu.UID, danmu.Content)
	}

	log.Println("[broadcast-consumer] stopped")
}
