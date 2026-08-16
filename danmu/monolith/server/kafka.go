package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer 异步批量 Kafka 生产者
// 职责：
// 1. 持久化弹幕历史到 topic danmu-history
// 2. 跨服务削峰的二级缓冲
// 3. 多消费者扇出（实时广播组、落库组、分析组等）
//
// Kafka 不可用时降级：记录日志，不阻塞实时广播主链路
type KafkaProducer struct {
	writer *kafka.Writer
	ctx    context.Context
}

func NewKafkaProducer(brokers []string, topic string, ctx context.Context) *KafkaProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // 按 key hash 分区，同 roomId 进同一 partition 保证有序
		BatchSize:    500,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true, // 异步批量写，不逐条同步 flush
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  3,
		WriteTimeout: 5 * time.Second,
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger:  kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Printf("[kafka-writer] "+msg, args...) }),
	}

	return &KafkaProducer{
		writer: w,
		ctx:    ctx,
	}
}

// Send 异步发送消息到 Kafka，按 roomId 做 partition key
func (kp *KafkaProducer) Send(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	return kp.writer.WriteMessages(kp.ctx, kafka.Message{
		Key:   []byte(msg.RoomID),
		Value: data,
	})
}

// PrepareBatch 在消息被回收前完成序列化，返回可安全异步发送的 Kafka 消息
func (kp *KafkaProducer) PrepareBatch(msgs []*Message) []kafka.Message {
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		data, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		kafkaMsgs = append(kafkaMsgs, kafka.Message{
			Key:   []byte(msg.RoomID),
			Value: data,
		})
	}
	return kafkaMsgs
}

// SendPrepared 异步发送已序列化的 Kafka 消息
func (kp *KafkaProducer) SendPrepared(kafkaMsgs []kafka.Message) error {
	if len(kafkaMsgs) == 0 {
		return nil
	}
	return kp.writer.WriteMessages(kp.ctx, kafkaMsgs...)
}

// Close 关闭 Kafka 生产者
func (kp *KafkaProducer) Close() error {
	return kp.writer.Close()
}

// EnsureTopic 确保 Kafka topic 存在
func EnsureTopic(brokers []string, topic string, partitions int) error {
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		// topic 已存在不算错误
		log.Printf("[kafka] create topic %s: %v (may already exist)", topic, err)
	}
	return nil
}
