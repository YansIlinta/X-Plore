package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 不以 room_id 为标签（房间基数可达百万，会撑爆 TSDB）——见 REVIEW.md H1。
var (
	metricConnections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_connections_total",
		Help: "累计建立的 WebSocket 连接数",
	})
	metricMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "danmu_messages_total",
		Help: "弹幕消息计数，direction=in 上行 / out 下行广播",
	}, []string{"direction"})
	metricBroadcastLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "danmu_broadcast_latency_seconds",
		Help:    "消息从生成(server_ts)到广播出去的延迟",
		Buckets: prometheus.DefBuckets,
	})
	metricDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "danmu_broadcast_dropped_total",
		Help: "sendCh 满导致丢弃的下行投递数（慢客户端保护）——见 REVIEW.md D3",
	})
)

func MetricConnInc()               { metricConnections.Inc() }
func MetricMsgIn()                 { metricMessages.WithLabelValues("in").Inc() }
func MetricMsgOut(n int)           { metricMessages.WithLabelValues("out").Add(float64(n)) }
func MetricDropped(n int)          { metricDropped.Add(float64(n)) }
func ObserveBroadcast(sec float64) { metricBroadcastLatency.Observe(sec) }
