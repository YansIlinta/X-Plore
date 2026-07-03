package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message 与 server 包的 Message 保持一致
type Message struct {
	Type         string `json:"type"`
	MsgID        string `json:"msg_id,omitempty"`
	RoomID       string `json:"room_id,omitempty"`
	UID          string `json:"uid,omitempty"`
	Content      string `json:"content,omitempty"`
	ClientTS     int64  `json:"client_ts,omitempty"`
	ServerTS     int64  `json:"server_ts,omitempty"`
	SourceServer string `json:"source_server,omitempty"`
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "danmu-storage", // 消费组 ID
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	// 批量写入缓冲
	var (
		batch   []Message
		batchMu sync.Mutex
	)

	// 定时 flush goroutine
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				batchMu.Lock()
				if len(batch) > 0 {
					toFlush := make([]Message, len(batch))
					copy(toFlush, batch)
					batch = batch[:0]
					batchMu.Unlock()
					if err := db.BatchInsert(toFlush); err != nil {
						log.Printf("[storage-consumer] batch insert error: %v", err)
					} else {
						log.Printf("[storage-consumer] flushed %d messages", len(toFlush))
					}
				} else {
					batchMu.Unlock()
				}
			}
		}
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[storage-consumer] read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var danmu Message
		if err := json.Unmarshal(msg.Value, &danmu); err != nil {
			log.Printf("[storage-consumer] unmarshal error: %v", err)
			continue
		}

		batchMu.Lock()
		batch = append(batch, danmu)
		needFlush := len(batch) >= 1000
		batchMu.Unlock()

		if needFlush {
			batchMu.Lock()
			toFlush := make([]Message, len(batch))
			copy(toFlush, batch)
			batch = batch[:0]
			batchMu.Unlock()
			if err := db.BatchInsert(toFlush); err != nil {
				log.Printf("[storage-consumer] batch insert error: %v", err)
			}
		}
	}

	// 退出前 flush 剩余
	batchMu.Lock()
	if len(batch) > 0 {
		db.BatchInsert(batch)
	}
	batchMu.Unlock()

	log.Println("[storage-consumer] stopped")
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
