package fanoutevidence

import (
	"encoding/json"
	"testing"
)

func artifactFor(mode string, rpcPer float64, internal, consumed, p90, missing int64, deliveryRate float64) Artifact {
	lt, _ := json.Marshal(map[string]any{
		"summary": map[string]any{
			"target_conns": 200, "success_conns": 200, "failed_conns": 0,
			"total_sent": 1000, "total_recv": 5000,
			"e2e_p50_us": p90 / 2, "e2e_p90_us": p90, "e2e_p99_us": p90 * 2, "e2e_max_us": p90 * 3,
		},
		"delivery": map[string]any{
			"enabled": true, "observed_deliveries": 5000 - missing,
			"missing_deliveries": missing, "expected_deliveries": 5000, "delivery_rate": deliveryRate,
		},
	})
	return Artifact{
		SchemaVersion: 1,
		JobStatsURL:   "http://job:7420/api/v1/stats",
		Settle:        "2s",
		Workload: Workload{Server: "ws://gateway:8080", Connections: 200, Rooms: 100, Rate: 2, Duration: "10s", Distribution: "uniform", Seed: 1, DeliveryCheck: true},
		Fanout: Report{FanoutMode: mode, RPCPerConsumedMessage: f64(rpcPer), Delta: CounterDelta{InternalRPCs: internal, ConsumedMessages: consumed}},
		LoadtestReport: lt,
	}
}

func TestCompareArtifacts(t *testing.T) {
	left := artifactFor(ModeBroadcastAll, 3.0, 3000, 1000, 20000, 10, 0.998)
	right := artifactFor(ModeRouteAware, 0.8, 800, 1000, 15000, 2, 0.9996)
	c, err := Compare(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Comparable {
		t.Fatalf("expected comparable: %s", c.ComparableNote)
	}
	m := c.Metrics["rpc_per_consumed_message"]
	if m.Delta == nil || *m.Delta != -2.2 {
		t.Fatalf("rpc delta=%v want -2.2", m.Delta)
	}
	p90 := c.Metrics["p90_latency_us"]
	if p90.Delta == nil || *p90.Delta != -5000 {
		t.Fatalf("p90 delta=%v want -5000", p90.Delta)
	}
	delivery := c.Metrics["delivery_rate"]
	if delivery.Delta == nil || *delivery.Delta <= 0 {
		t.Fatalf("delivery delta=%v want >0", delivery.Delta)
	}
}

func TestCompareRejectsDifferentWorkloadSemantics(t *testing.T) {
	left := artifactFor(ModeBroadcastAll, 3, 30, 10, 1000, 0, 1)
	right := artifactFor(ModeRouteAware, 1, 10, 10, 900, 0, 1)
	right.Workload.Rooms++
	c, err := Compare(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if c.Comparable || c.ComparableNote == "" {
		t.Fatal("different workloads must not be directly comparable")
	}

	right = artifactFor(ModeRouteAware, 1, 10, 10, 900, 0, 1)
	right.Settle = "5s"
	c, err = Compare(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if c.Comparable {
		t.Fatal("different settle windows must not be directly comparable")
	}
}

func TestCompareDeliveryNA(t *testing.T) {
	left := artifactFor(ModeBroadcastAll, 3, 30, 10, 1000, 0, 1)
	right := artifactFor(ModeRouteAware, 1, 10, 10, 900, 0, 1)
	var raw map[string]any
	_ = json.Unmarshal(right.LoadtestReport, &raw)
	raw["delivery"] = map[string]any{"enabled": false}
	right.LoadtestReport, _ = json.Marshal(raw)
	c, err := Compare(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if c.Metrics["delivery_rate"].Left != nil || c.Metrics["delivery_rate"].Right != nil {
		t.Fatal("delivery metric must be N/A unless both artifacts measured it")
	}
}
