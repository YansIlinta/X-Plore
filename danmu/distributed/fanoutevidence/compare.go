package fanoutevidence

import (
	"encoding/json"
	"fmt"
)

// MetricDelta keeps N/A semantics explicit. Delta is Right-Left and DeltaPct is
// relative to Left; both remain nil when either side is unmeasured. DeltaPct is
// also nil when Left is zero.
type MetricDelta struct {
	Left     *float64 `json:"left"`
	Right    *float64 `json:"right"`
	Delta    *float64 `json:"delta"`
	DeltaPct *float64 `json:"delta_pct"`
}

type Comparison struct {
	Comparable     bool                   `json:"comparable"`
	ComparableNote string                 `json:"comparable_note,omitempty"`
	LeftMode       string                 `json:"left_mode"`
	RightMode      string                 `json:"right_mode"`
	Workload       Workload               `json:"workload"`
	Settle         string                 `json:"settle"`
	Metrics        map[string]MetricDelta `json:"metrics"`
	Notes          []string               `json:"notes,omitempty"`
}

type loadtestEnvelope struct {
	Summary struct {
		TargetConns  int64 `json:"target_conns"`
		SuccessConns int64 `json:"success_conns"`
		FailedConns  int64 `json:"failed_conns"`
		TotalSent    int64 `json:"total_sent"`
		TotalRecv    int64 `json:"total_recv"`
		P50US        int64 `json:"e2e_p50_us"`
		P90US        int64 `json:"e2e_p90_us"`
		P99US        int64 `json:"e2e_p99_us"`
		MaxUS        int64 `json:"e2e_max_us"`
	} `json:"summary"`
	Delivery struct {
		Enabled              bool    `json:"enabled"`
		ObservedDeliveries   int64   `json:"observed_deliveries"`
		MissingDeliveries    int64   `json:"missing_deliveries"`
		ExpectedDeliveries   int64   `json:"expected_deliveries"`
		DeliveryRate         float64 `json:"delivery_rate"`
	} `json:"delivery"`
}

// Compare requires identical workload and settle semantics. It compares the
// routing-plane ratio together with loadtest latency/delivery fields embedded
// in each artifact. It does not infer statistical significance from one pair;
// repeated runs still belong in the Systems Lab aggregate layer.
func Compare(left, right Artifact) (Comparison, error) {
	c := Comparison{
		LeftMode:  left.Fanout.FanoutMode,
		RightMode: right.Fanout.FanoutMode,
		Workload:  left.Workload,
		Settle:    left.Settle,
		Metrics:   make(map[string]MetricDelta),
	}
	if left.SchemaVersion != right.SchemaVersion {
		c.ComparableNote = fmt.Sprintf("schema versions differ: %d vs %d", left.SchemaVersion, right.SchemaVersion)
		return c, nil
	}
	if left.Workload != right.Workload {
		c.ComparableNote = "workloads differ; A/B deltas are not directly comparable"
		return c, nil
	}
	if left.Settle != right.Settle {
		c.ComparableNote = fmt.Sprintf("settle windows differ: %q vs %q", left.Settle, right.Settle)
		return c, nil
	}

	var l, r loadtestEnvelope
	if err := json.Unmarshal(left.LoadtestReport, &l); err != nil {
		return c, fmt.Errorf("decode left loadtest report: %w", err)
	}
	if err := json.Unmarshal(right.LoadtestReport, &r); err != nil {
		return c, fmt.Errorf("decode right loadtest report: %w", err)
	}

	c.Comparable = true
	if left.Fanout.FanoutMode == right.Fanout.FanoutMode {
		c.Notes = append(c.Notes, "both artifacts use the same fanout mode; this is a repeatability comparison, not broadcast_all vs route_aware")
	}
	if left.JobStatsURL != right.JobStatsURL {
		c.Notes = append(c.Notes, "Job stats URLs differ; confirm both runs represent the intended comparable environments")
	}
	c.Notes = append(c.Notes, "single-pair deltas are descriptive; use repeated runs before promoting a performance claim")

	c.Metrics["rpc_per_consumed_message"] = delta(left.Fanout.RPCPerConsumedMessage, right.Fanout.RPCPerConsumedMessage)
	c.Metrics["internal_rpcs"] = delta(i64f(left.Fanout.Delta.InternalRPCs), i64f(right.Fanout.Delta.InternalRPCs))
	c.Metrics["consumed_messages"] = delta(i64f(left.Fanout.Delta.ConsumedMessages), i64f(right.Fanout.Delta.ConsumedMessages))
	c.Metrics["route_resolve_errors"] = delta(i64f(left.Fanout.Delta.RouteResolveErrors), i64f(right.Fanout.Delta.RouteResolveErrors))
	c.Metrics["route_missing_comets"] = delta(i64f(left.Fanout.Delta.RouteMissingComets), i64f(right.Fanout.Delta.RouteMissingComets))
	c.Metrics["p50_latency_us"] = delta(f64(float64(l.Summary.P50US)), f64(float64(r.Summary.P50US)))
	c.Metrics["p90_latency_us"] = delta(f64(float64(l.Summary.P90US)), f64(float64(r.Summary.P90US)))
	c.Metrics["p99_latency_us"] = delta(f64(float64(l.Summary.P99US)), f64(float64(r.Summary.P99US)))
	c.Metrics["messages_received"] = delta(f64(float64(l.Summary.TotalRecv)), f64(float64(r.Summary.TotalRecv)))
	c.Metrics["connections_established"] = delta(f64(float64(l.Summary.SuccessConns)), f64(float64(r.Summary.SuccessConns)))
	if l.Delivery.Enabled && r.Delivery.Enabled {
		c.Metrics["delivery_rate"] = delta(f64(l.Delivery.DeliveryRate), f64(r.Delivery.DeliveryRate))
		c.Metrics["missing_deliveries"] = delta(f64(float64(l.Delivery.MissingDeliveries)), f64(float64(r.Delivery.MissingDeliveries)))
	} else {
		c.Metrics["delivery_rate"] = MetricDelta{}
		c.Metrics["missing_deliveries"] = MetricDelta{}
		c.Notes = append(c.Notes, "delivery-check was not enabled in both artifacts; delivery deltas are N/A")
	}
	return c, nil
}

func delta(left, right *float64) MetricDelta {
	m := MetricDelta{Left: left, Right: right}
	if left == nil || right == nil {
		return m
	}
	d := *right - *left
	m.Delta = &d
	if *left != 0 {
		p := d / *left * 100
		m.DeltaPct = &p
	}
	return m
}

func f64(v float64) *float64 { return &v }
func i64f(v int64) *float64 { return f64(float64(v)) }
