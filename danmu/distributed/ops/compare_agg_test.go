package ops

import (
	"testing"
)

func aggExpWith(id, regime string, reps int, p99s []int64, rate float64, specHash string) *Experiment {
	agg := &ExperimentAggregate{
		SuccessfulRepetitions: reps, TotalRepetitions: reps, Status: ExpStatusCompleted,
		Stability: AggStable, Metrics: map[string]*MetricAggregate{},
	}
	m := &MetricAggregate{Measured: true, Samples: reps, TotalRep: reps}
	med := float64(p99s[0])
	m.Median = &med
	lo, hi := med-500.0, med+1500.0
	m.CI95Low, m.CI95High = &lo, &hi
	m.CIStatus = "ok"
	agg.Metrics["p99_latency_us"] = m
	agg.Metrics["receive_rate"] = aggMetric(rate, rate, 0.05)
	return &Experiment{
		ID: id, Regime: regime, Status: ExpStatusCompleted, Repetitions: reps,
		SpecHash: specHash, Duration: "8s", Warmup: "2s", Aggregate: agg,
		Result: ExperimentResult{P99LatencyUS: intp(p99s[0])},
	}
}

// TestAggregateCompareSameSpecComparable：同 spec_hash + 同测量窗 → comparable。
func TestAggregateCompareSameSpecComparable(t *testing.T) {
	l := aggExpWith("l", RegimeLowFanout, 5, []int64{10000}, 1200, "hash1")
	r := aggExpWith("r", RegimeLowFanout, 5, []int64{8000}, 1500, "hash1")
	res := CompareExperiments(l, r)
	if !res.Comparable {
		t.Fatalf("same spec+window must be comparable: %s", res.ComparableNote)
	}
	if res.LeftAgg == nil || res.RightAgg == nil {
		t.Fatalf("aggregate briefs missing")
	}
	if res.DiffConclusion == "" {
		t.Fatalf("conclusion missing")
	}
	// r 的 p99 更低（更好）→ likely improvement（latency 方向）。
	if res.DiffConclusion != "likely improvement" {
		t.Fatalf("conclusion=%q, want likely improvement (p99 improved, rate improved)", res.DiffConclusion)
	}
}

// TestAggregateCompareDifferentSpecNotComparable：spec_hash 不同 → NOT DIRECTLY COMPARABLE。
func TestAggregateCompareDifferentSpecNotComparable(t *testing.T) {
	l := aggExpWith("l", RegimeLowFanout, 5, []int64{10000}, 1200, "hash-a")
	r := aggExpWith("r", RegimeLowFanout, 5, []int64{10000}, 1200, "hash-b")
	res := CompareExperiments(l, r)
	if res.Comparable {
		t.Fatalf("different spec_hash must NOT be directly comparable")
	}
	if res.ComparableNote == "" || !containsSub(res.ComparableNote, "NOT DIRECTLY COMPARABLE") {
		t.Fatalf("note=%q", res.ComparableNote)
	}
}

// TestAggregateCompareHighVarianceFlag：任一侧 CV>=0.30 → high variance。
func TestAggregateCompareHighVarianceFlag(t *testing.T) {
	l := aggExpWith("l", RegimeLowFanout, 3, []int64{10000}, 1200, "h1")
	r := aggExpWith("r", RegimeLowFanout, 3, []int64{10000}, 1200, "h1")
	cv := 0.5
	l.Aggregate.Metrics["p99_latency_us"].CV = &cv
	res := CompareExperiments(l, r)
	if res.DiffConclusion != "high variance" {
		t.Fatalf("conclusion=%q, want high variance", res.DiffConclusion)
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestCompareDifferentMeasurementWindowNotComparable：窗口不同 → NOT DIRECTLY COMPARABLE。
func TestCompareDifferentMeasurementWindowNotComparable(t *testing.T) {
	l := aggExpWith("l", RegimeLowFanout, 5, []int64{10000}, 1200, "h1")
	r := aggExpWith("r", RegimeLowFanout, 5, []int64{10000}, 1200, "h1")
	r.Duration = "30s"
	res := CompareExperiments(l, r)
	if res.Comparable {
		t.Fatalf("different measurement windows must NOT be comparable")
	}
}
