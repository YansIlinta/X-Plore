package main

import (
	"encoding/json"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	batchSize    = 2000
	batchTimeout = 20 * time.Millisecond
)

// WorkerPool 固定大小的 worker 池，消费 msgQueue，批量聚合后广播
// worker 数量 = runtime.NumCPU() * 4
type WorkerPool struct {
	hub     *Hub
	workers int
	wg      sync.WaitGroup
}

func NewWorkerPool(hub *Hub) *WorkerPool {
	return &WorkerPool{
		hub:     hub,
		workers: runtime.NumCPU() * 2,
	}
}

// Start 启动所有 worker goroutine
func (wp *WorkerPool) Start() {
	log.Printf("[WorkerPool] starting %d workers", wp.workers)
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	go wp.reportQueueLength()
}

// reportQueueLength 定期采样 msgQueue 长度，供 Prometheus danmu_msgqueue_length 使用
func (wp *WorkerPool) reportQueueLength() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-wp.hub.ctx.Done():
			return
		case <-ticker.C:
			metricMsgQueueLength.Set(float64(len(wp.hub.msgQueue)))
		}
	}
}

// Wait 等待所有 worker 退出
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// worker 单个 worker goroutine
// 批量聚合策略：攒满 batchSize 条或每 batchTimeout 触发一次
// 聚合后按房间分组广播，减少 syscall
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	batch := make([]*Message, 0, batchSize)
	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// 按房间分组
		roomMsgs := make(map[string][]*Message)
		for _, msg := range batch {
			roomMsgs[msg.RoomID] = append(roomMsgs[msg.RoomID], msg)
		}

		// 按房间批量广播
		for roomID, msgs := range roomMsgs {
			data, err := json.Marshal(msgs)
			if err != nil {
				log.Printf("[worker %d] marshal error: %v", id, err)
				continue
			}

			// 本机广播
			wp.hub.BroadcastToRoom(roomID, data)

			// 跨机广播：Redis Pub/Sub（整房间一批消息单次 PUBLISH）和/或 Kafka（逐条，保留 Kafka 分区/持久化语义）
			wp.publishRoomBatch(roomID, data, msgs)

			metricMessagesTotal.WithLabelValues(roomID, "out").Add(float64(len(msgs)))
			now := time.Now()
			for _, msg := range msgs {
				metricBroadcastLatency.Observe(now.Sub(time.UnixMilli(msg.ServerTS)).Seconds())
			}
		}

		// 回收消息对象
		for _, msg := range batch {
			releaseMessage(msg)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-wp.hub.ctx.Done():
			// 优雅退出：处理完队列中剩余消息
			for {
				select {
				case msg := <-wp.hub.msgQueue:
					batch = append(batch, msg)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case msg := <-wp.hub.msgQueue:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(batchTimeout)
			}

		case <-timer.C:
			flush()
			timer.Reset(batchTimeout)
		}
	}
}

// publishRoomBatch 将一个房间的一批消息发布到 Redis（整批单次 PUBLISH）和/或 Kafka（逐条）
func (wp *WorkerPool) publishRoomBatch(roomID string, data []byte, msgs []*Message) {
	h := wp.hub

	switch h.mqMode {
	case "redis":
		wp.publishRedisBatch(roomID, data)
	case "kafka":
		for _, msg := range msgs {
			wp.publishKafka(msg)
		}
	case "both":
		wp.publishRedisBatch(roomID, data)
		for _, msg := range msgs {
			wp.publishKafka(msg)
		}
	}
}

func (wp *WorkerPool) publishRedisBatch(roomID string, data []byte) {
	if wp.hub.redisHub == nil {
		return
	}
	if err := wp.hub.redisHub.PublishBatch(roomID, data); err != nil {
		log.Printf("[worker] redis publish error: %v", err)
	}
}

func (wp *WorkerPool) publishKafka(msg *Message) {
	if wp.hub.kafkaProd == nil {
		return
	}
	if err := wp.hub.kafkaProd.Send(msg); err != nil {
		// Kafka 不可用时降级：记录日志，不阻塞实时广播主链路
		log.Printf("[worker] kafka send error (degraded): %v", err)
	}
}
