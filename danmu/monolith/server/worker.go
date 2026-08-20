package main

import (
	"encoding/json"
	"log"
	"runtime"
	"sync"
	"time"
)

// 默认批量参数（可被 -batch-size / -batch-timeout / -workers 覆盖，见 main.go）。
// 这些是 startup config：运行时不可改，改变它必须重启 server 进程。
var (
	defaultBatchSize    = 2000
	defaultBatchTimeout = 20 * time.Millisecond
)

// WorkerPool 固定大小的 worker 池，消费 msgQueue，批量聚合后广播
// worker 数量默认 = runtime.NumCPU() * 2（可被 -workers 覆盖）。
type WorkerPool struct {
	hub          *Hub
	workers      int
	batchSize    int
	batchTimeout time.Duration
	wg           sync.WaitGroup
}

func NewWorkerPool(hub *Hub) *WorkerPool {
	return NewWorkerPoolCfg(hub, runtime.NumCPU()*2, defaultBatchSize, defaultBatchTimeout)
}

// NewWorkerPoolCfg 以显式参数构造 WorkerPool（供 -batch-size/-batch-timeout/-workers 使用）。
func NewWorkerPoolCfg(hub *Hub, workers, batchSize int, batchTimeout time.Duration) *WorkerPool {
	if workers <= 0 {
		workers = runtime.NumCPU() * 2
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchTimeout <= 0 {
		batchTimeout = defaultBatchTimeout
	}
	return &WorkerPool{hub: hub, workers: workers, batchSize: batchSize, batchTimeout: batchTimeout}
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
// 批量聚合策略：攒满 wp.batchSize 条或每 wp.batchTimeout 触发一次
// 聚合后按房间分组广播，减少 syscall
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	batch := make([]*Message, 0, wp.batchSize)
	timer := time.NewTimer(wp.batchTimeout)
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

		// 按房间批量广播（优先完成本机 + Redis 实时路径）
		for roomID, msgs := range roomMsgs {
			// 先按房间打序号并入热历史，再序列化广播：保证「补发可见」的消息
			// 一定不会漏出实时路径（顺序相反时，注册窗口内会漏补）。
			for _, msg := range msgs {
				msg.Seq = wp.hub.nextRoomSeq(roomID)
			}
			// 按优先级拆两路：普通弹幕可丢（sendCh 满静默丢弃）；高优先级走
			// 独立通道（满则显式计数，不无声丢失）。两路都先入热历史。
			var normal, high []*Message
			for _, m := range msgs {
				if m.Priority > 0 {
					high = append(high, m)
				} else {
					normal = append(normal, m)
				}
			}
			wp.hub.hist.AppendBatch(roomID, msgs)

			broadcast := func(msgs []*Message, isHigh bool) {
				if len(msgs) == 0 {
					return
				}
				data, err := json.Marshal(msgs)
				if err != nil {
					log.Printf("[worker %d] marshal error: %v", id, err)
					return
				}
				if isHigh {
					wp.hub.BroadcastToRoomHigh(roomID, data)
				} else {
					wp.hub.BroadcastToRoom(roomID, data)
				}
				// Redis 跨机广播（实时路径）；对端按 payload 首条 priority 路由
				wp.publishRedisBatch(roomID, data)
			}
			broadcast(normal, false)
			broadcast(high, true)

			metricMessagesTotal.WithLabelValues("out").Add(float64(len(msgs)))
			now := time.Now()
			for _, msg := range msgs {
				metricBroadcastLatency.Observe(now.Sub(time.UnixMilli(msg.ServerTS)).Seconds())
			}
		}

		// Kafka 持久化路径：先同步序列化（在 releaseMessage 之前），再异步发送
		if wp.hub.kafkaProd != nil && (wp.hub.mqMode == "kafka" || wp.hub.mqMode == "both") {
			prepared := wp.hub.kafkaProd.PrepareBatch(batch)
			go func() {
				if err := wp.hub.kafkaProd.SendPrepared(prepared); err != nil {
					log.Printf("[worker %d] kafka send error: %v", id, err)
				}
			}()
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
					if len(batch) >= wp.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case msg := <-wp.hub.msgQueue:
			batch = append(batch, msg)
			if len(batch) >= wp.batchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(wp.batchTimeout)
			}

		case <-timer.C:
			flush()
			timer.Reset(wp.batchTimeout)
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
