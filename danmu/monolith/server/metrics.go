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
)
