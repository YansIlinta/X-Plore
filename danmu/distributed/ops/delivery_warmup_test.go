package ops

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// --- Warm-up / measurement 分离（§8,§4）：run 记录真实观测窗口；速率按测量窗计算 ---

func TestRunMeasurementWindowFromReport(t *testing.T) {
	report := map[string]any{
		"summary": map[string]any{
			"target_conns": 10, "success_conns": 10, "failed_conns": 0,
			"total_sent": 1000, "total_recv": 2000,
			"e2e_p50_us": 100, "e2e_p90_us": 200, "e2e_p99_us": 300, "e2e_max_us": 500,
		},
		"measurement": map[string]any{
			"start": "2026-08-20T04:00:05Z", "end": "2026-08-20T04:00:15Z",
			"warmup": "2s", "measurement": "10s",
		},
		"delivery": map[string]any{
			"enabled": true, "observed_deliveries": float64(2000), "missing_deliveries": float64(3),
			"expected_deliveries": float64(2003), "delivery_rate": float64(2000) / 2003,
		},
	}
	run := &ExperimentRun{StartedAt: ptrTime(time.Now().UTC()), FinishedAt: ptrTime(time.Now().UTC())}
	res, _ := resultFromLoadtestReport(report, ExperimentSpec{}, run, &Experiment{})
	if run.MeasurementStart == nil || run.MeasurementEnd == nil {
		t.Fatalf("measurement window not recorded: %+v", run)
	}
	// 速率基于 10s 测量窗（1000/10=100；2000/10=200）→ 不是墙钟。
	if res.SendRate == nil || *res.SendRate != 100 {
		t.Fatalf("send_rate=%v, want 100 (measurement window)", res.SendRate)
	}
	if res.ReceiveRate == nil || *res.ReceiveRate != 200 {
		t.Fatalf("receive_rate=%v, want 200", res.ReceiveRate)
	}
	if res.DeliveryRate == nil || res.MissingDeliveries == nil || *res.MissingDeliveries != 3 {
		t.Fatalf("delivery accounting not parsed: %+v", res)
	}
	if res.Drops == nil || *res.Drops != 3 {
		t.Fatalf("drops must reflect real missing deliveries, got %+v", res.Drops)
	}
}

func TestWarmupOnlyStillRecordsWindow(t *testing.T) {
	// warmup>0 且报告不含 measurement 段 → 窗口保持原样（N/A），速率用 run 起止兜底。
	report := map[string]any{"summary": map[string]any{"total_sent": 500, "total_recv": 900, "e2e_p90_us": 100}}
	now := time.Now().UTC()
	run := &ExperimentRun{StartedAt: &now, FinishedAt: ptrTime(now.Add(5 * time.Second))}
	res, _ := resultFromLoadtestReport(report, ExperimentSpec{Warmup: "2s"}, run, &Experiment{})
	if res.SendRate == nil || *res.SendRate != 100 {
		t.Fatalf("fallback send_rate=%v, want 100 (run elapsed)", res.SendRate)
	}
	if run.MeasurementStart != nil {
		t.Fatalf("no measurement window info → must stay nil")
	}
}

// --- Delivery accounting 解析：无 measurement 段时不产生虚假速率 ---
func TestNoMeasurementNoFakeRate(t *testing.T) {
	report := map[string]any{"summary": map[string]any{}}
	run := &ExperimentRun{}
	res, _ := resultFromLoadtestReport(report, ExperimentSpec{}, run, &Experiment{})
	if res.SendRate != nil || res.ReceiveRate != nil {
		t.Fatalf("no data must produce N/A rates, got %+v/%+v", res.SendRate, res.ReceiveRate)
	}
	if res.Drops != nil {
		t.Fatalf("no delivery data → drops must stay nil")
	}
}

// --- Evidence 新 claims 由存储数据推导状态（§25,§26）---

func mkAggExp(id, regime string, reps int, stability string, p99, receiveRate, deliveryRate *float64) *Experiment {
	runs := make([]*ExperimentRun, reps)
	for i := range runs {
		res := ExperimentResult{ConnectionsEstablished: intp(100)}
		if p99 != nil {
			res.P99LatencyUS = intp(int64(*p99))
		}
		if deliveryRate != nil {
			miss := int64(2)
			res.MissingDeliveries = &miss
			res.DeliveryRate = deliveryRate
		}
		runs[i] = &ExperimentRun{Index: i + 1, Status: RunStatusCompleted, Result: res}
	}
	agg := &ExperimentAggregate{
		SuccessfulRepetitions: reps, TotalRepetitions: reps, Status: ExpStatusCompleted,
		Stability: stability, Metrics: map[string]*MetricAggregate{},
	}
	if p99 != nil {
		agg.Metrics["p99_latency_us"] = aggMetric(*p99, *p99, 0.02)
	}
	if receiveRate != nil {
		agg.Metrics["receive_rate"] = aggMetric(*receiveRate, *receiveRate, 0.02)
	}
	if deliveryRate != nil {
		agg.Metrics["delivery_rate"] = aggMetric(*deliveryRate, *deliveryRate, 0.005)
	}
	return &Experiment{ID: id, Regime: regime, Status: ExpStatusCompleted, Runs: runs, Aggregate: agg}
}

func TestEvidenceDerivedClaims(t *testing.T) {
	s := evidenceStore(t)
	p99lo := float64(10000)
	p99hi := float64(30000)
	thrHot := float64(3000)
	thrLo := float64(1000)
	thrSkew := float64(2000)
	deliv := float64(1.0)
	// hot-room P99 > low-fanout P99。
	saveExp(t, s, mkAggExp("e-hot", RegimeHotRoom, 3, AggStable, &p99hi, &thrHot, &deliv))
	saveExp(t, s, mkAggExp("e-lo", RegimeLowFanout, 3, AggStable, &p99lo, &thrLo, &deliv))
	saveExp(t, s, mkAggExp("e-skewed", RegimeSkewedHotRoom, 3, AggStable, &p99hi, &thrSkew, &deliv))

	ev := NewEvidenceService(s, "")
	byID := map[string]Claim{}
	for _, c := range ev.List() {
		byID[c.ID] = c
	}

	if c := byID["claim-repeatability-observed"]; c.Status != ClaimVerified {
		t.Fatalf("repeatability claim not VERIFIED: %+v", c)
	}
	if c := byID["claim-hot-room-higher-tail-latency"]; c.Status != ClaimVerified {
		t.Fatalf("hot-room tail claim not VERIFIED: %+v", c)
	}
	if c := byID["claim-delivery-accounting-supported"]; c.Status != ClaimVerified {
		t.Fatalf("delivery accounting claim must be VERIFIED with real run data: %+v", c)
	}
	// 单 regime 占优检查：所有 regime 的 best 相同才有 dominance VERIFIED；这里 3 个 regime 都是 e-hot? 不一定。
	// 至少 cross-regime 结论应与推导一致（有数据支撑 → not UNKNOWN）。
	if c := byID["claim-one-config-dominates-all"]; c.Status == ClaimUnknown {
		t.Fatalf("dominance claim should not be UNKNOWN with cross-regime data: %+v", c)
	}
}

func TestEvidenceDeliveryUnknownWithoutRuns(t *testing.T) {
	s := evidenceStore(t)
	// 没有任何投递数据 → CODE VERIFIED（算法被单测证明），但不升级 VERIFIED。
	saveExp(t, s, &Experiment{ID: "e0", Regime: RegimeLowFanout, Status: ExpStatusCompleted,
		Runs: []*ExperimentRun{{Index: 1, Status: RunStatusCompleted}}, Aggregate: &ExperimentAggregate{SuccessfulRepetitions: 1, TotalRepetitions: 1, Status: ExpStatusCompleted, Metrics: map[string]*MetricAggregate{}}})
	ev := NewEvidenceService(s, "")
	for _, c := range ev.List() {
		if c.ID == "claim-delivery-accounting-supported" {
			if c.Status != ClaimCodeVerified {
				t.Fatalf("no real delivery data → CODE VERIFIED, got %s", c.Status)
			}
		}
	}
}

// --- API validation（§30）---
func TestAPISweepValidation(t *testing.T) {
	a, _, done := testAPI(t, 0.01)
	defer done()
	code, body := doJSON(t, a.Handler(), "POST", "/api/sweeps", map[string]any{
		"params": []map[string]any{{"name": "batch_size", "values": []string{"1", "2", "3", "4", "5", "6", "7"}},
			{"name": "batch_timeout", "values": []string{"1ms", "2ms", "3ms", "4ms", "5ms"}}},
	})
	if code != 422 {
		t.Fatalf("oversized sweep must be 422, got %d (%v)", code, body)
	}
	// 无维度。
	code2, _ := doJSON(t, a.Handler(), "POST", "/api/sweeps", map[string]any{"params": []map[string]any{}})
	if code2 != 400 && code2 != 422 {
		t.Fatalf("empty sweep params must be rejected, got %d", code2)
	}
}

var _ = context.Background
var _ = filepath.Join
