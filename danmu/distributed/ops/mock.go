package ops

import (
	"context"
	"math"
	"time"
)

// mock 模式：仅 -mock 显式启用时运行。喂带缓慢波动的假数据，且每个响应都带
// "mock": true，前端会显著标记 MOCK DATA。开发/UI 联调用，绝不能冒充真实监控。
func (c *Collector) runMock(ctx context.Context) {
	t := time.NewTicker(c.cfg.Poll)
	defer t.Stop()
	start := time.Now()
	for {
		c.mockOnce(start)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Collector) mockOnce(start time.Time) {
	now := time.Now()
	el := now.Sub(start).Seconds()
	wave := math.Sin(el/15) * 0.15 // 缓慢波动，模拟负载起伏

	conn1 := 18000 + int(wave*3000)
	conn2 := 17000 - int(wave*2000)
	inRate, outRate := 850.0*(1+wave), 1800.0*(1+wave)

	conns1 := float64(conn1)
	conns2 := float64(conn2)
	rooms1 := float64(700 + int(wave*100))
	rooms2 := float64(650 - int(wave*80))
	in1, in2 := inRate/2, inRate/2
	out1, out2 := outRate/2, outRate/2

	snap := Snapshot{
		Mock:         true,
		TS:           now,
		RegistryUp:   true,
		Health:       healthHealthy,
		HealthDetail: []string{"mock 模式：所有数值为演示数据，不代表真实系统"},
		Services: []Service{
			{
				Name: "comet",
				Instances: []Instance{
					mockInstance("comet1:8080", "comet1:7500", true, conns1, rooms1, &in1, &out1),
					mockInstance("comet2:8080", "comet2:7500", true, conns2, rooms2, &in2, &out2),
				},
			},
			{
				Name: "logic",
				Instances: []Instance{
					{HTTPAddr: "logic:7410", RPCAddr: "logic:7400", Healthy: true,
						Stats: map[string]any{"server_id": "logic1", "onmessage_total": 1_200_000 + int(el*850), "kafka_produce_errors": 0}},
				},
			},
			{
				Name: "job",
				Instances: []Instance{
					{HTTPAddr: "job:7420", Healthy: true,
						Stats: map[string]any{"server_id": "job", "consumed_total": 1_200_000 + int(el*850), "push_ok_total": 2_400_000, "push_err_total": 0}},
				},
			},
		},
		Kafka: KafkaInfo{
			Available: true,
			Lag: map[string]*int64{
				"danmu-job":     i64p(42 + int64(wave*100)),
				"danmu-storage": i64p(128 + int64(wave*300)),
			},
		},
	}
	c.mu.Lock()
	c.snap = snap
	c.mu.Unlock()
}

func mockInstance(httpAddr, rpcAddr string, healthy bool, conns, rooms float64, in, out *float64) Instance {
	return Instance{
		HTTPAddr:   httpAddr,
		RPCAddr:    rpcAddr,
		Healthy:    healthy,
		MsgInRate:  in,
		MsgOutRate: out,
		Stats: map[string]any{
			"server_id":  "comet1",
			"conn_count": conns,
			"room_count": rooms,
		},
	}
}

func i64p(v int64) *int64 { return &v }
