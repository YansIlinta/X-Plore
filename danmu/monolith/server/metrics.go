package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// 不以 room_id 为标签：房间数可达数十万~百万，按房间打标签会产生同量级
	// 且永不回收的时间序列，撑爆 Prometheus 并让进程内 metric map 无限增长。
	// 房间维度的观测走日志/Top-N，不进标签。
	metricConnectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_connections_total",
		Help: "累计建立的 WebSocket 连接数",
	})

	metricMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "danmu_messages_total",
		Help: "弹幕消息计数，direction=in 为客户端上行接收，direction=out 为广播下行",
	}, []string{"direction"})

	metricBroadcastLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "danmu_broadcast_latency_seconds",
		Help:    "消息从生成(server_ts)到广播出去的延迟",
		Buckets: prometheus.DefBuckets,
	})

	metricMsgQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "danmu_msgqueue_length",
		Help: "进程内消息队列 msgQueue 当前堆积长度",
	})

	// 高优先级消息的丢弃必须显式计数（普通弹幕允许静默丢弃，高优先级不允许
	// 无声丢失——满则记入此计数供告警观测）
	metricHighPriorityDrops = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_high_priority_drops_total",
		Help: "高优先级消息因客户端高优通道满而丢弃的累计次数",
	})

	// 房间词库 flag 模式命中计数（不带房间标签，避免高基数）
	metricFlaggedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_flagged_total",
		Help: "房间词库 flag 模式命中的消息累计数（放行但打标）",
	})

	// ack 通道满时的丢弃计数：绝不阻塞 readPump（阻塞会导致高扇出下上行死锁）
	metricAckDrops = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_ack_drops_total",
		Help: "ack 因客户端 ack 通道满而丢弃的累计数（客户端可凭广播回声确认）",
	})
)
