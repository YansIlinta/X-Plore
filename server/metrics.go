package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricConnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "danmu_connections_total",
		Help: "累计建立的 WebSocket 连接数",
	}, []string{"room_id"})

	metricMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "danmu_messages_total",
		Help: "弹幕消息计数，direction=in 为客户端上行接收，direction=out 为广播下行",
	}, []string{"room_id", "direction"})

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
