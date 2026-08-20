package ops

import (
	"math"
	"math/rand"
	"testing"
)

// --- 纯统计正确性（§37 Benchmark Correctness Tests）---

func TestMomentsKnownValues(t *testing.T) {
	// [10,20,30] mean=20 median=20
	m := MomentsOf([]float64{10, 20, 30})
	if m.Mean == nil || *m.Mean != 20 {
		t.Fatalf("mean=%v, want 20", m.Mean)
	}
	if m.Median == nil || *m.Median != 20 {
		t.Fatalf("median=%v, want 20", m.Median)
	}
	if m.Min == nil || *m.Min != 10 || m.Max == nil || *m.Max != 30 {
		t.Fatalf("min/max=%v/%v", m.Min, m.Max)
	}
	if m.StdDev == nil || math.Abs(*m.StdDev-10) > 1e-9 {
		t.Fatalf("stddev=%v, want 10 (sample ddof=1)", m.StdDev)
	}
	if m.CV == nil || math.Abs(*m.CV-0.5) > 1e-9 {
		t.Fatalf("cv=%v, want 0.5", m.CV)
	}
}

func TestCVZeroForConstantValues(t *testing.T) {
	m := MomentsOf([]float64{10, 10, 10})
	if m.Mean == nil || *m.Mean != 10 {
		t.Fatalf("mean=%v", m.Mean)
	}
	if m.CV == nil || *m.CV != 0 {
		t.Fatalf("cv=%v, want 0 (perfect stability)", m.CV)
	}
	if m.StdDev == nil || *m.StdDev != 0 {
		t.Fatalf("stddev=%v, want 0", m.StdDev)
	}
}

func TestSingleSampleNoStdDevNoCV(t *testing.T) {
	m := MomentsOf([]float64{42})
	if m.Count != 1 || m.Mean == nil || *m.Mean != 42 {
		t.Fatalf("single sample moments wrong: %+v", m)
	}
	if m.StdDev != nil || m.CV != nil {
		t.Fatalf("n=1 must have no stddev/cv, got %+v", m)
	}
}

func TestEmptyMomentsAllNil(t *testing.T) {
	m := MomentsOf(nil)
	if m.Count != 0 || m.Mean != nil || m.Median != nil || m.Min != nil || m.Max != nil {
		t.Fatalf("empty must be all-N/A, got %+v", m)
	}
}

func TestMedianEvenVsOdd(t *testing.T) {
	if v := Median([]float64{4, 1, 2}); v == nil || *v != 2 {
		t.Fatalf("odd median=%v", v)
	}
	if v := Median([]float64{1, 2, 3, 4}); v == nil || *v != 2.5 {
		t.Fatalf("even median=%v, want 2.5", v)
	}
}

// --- Bootstrap CI：确定性 + insufficient_samples ---

func TestBootstrapCIDeterministicSeed(t *testing.T) {
	values := []float64{100, 102, 96, 104, 99, 101, 98, 105}
	seed := int64(7)
	a1, b1, ok1 := BootstrapMeanCI(values, rand.New(rand.NewSource(seed)), 500, 0.05)
	a2, b2, ok2 := BootstrapMeanCI(values, rand.New(rand.NewSource(seed)), 500, 0.05)
	if !ok1 || !ok2 || a1 != a2 || b1 != b2 {
		t.Fatalf("bootstrap CI not deterministic across same seed: (%v,%v) vs (%v,%v)", a1, b1, a2, b2)
	}
	if !ok1 || a1 > b1 {
		t.Fatalf("bootstrap CI invalid (low>high): (%v,%v)", a1, b1)
	}
}

func TestBootstrapCIInsufficientSamples(t *testing.T) {
	if _, _, ok := BootstrapMeanCI([]float64{1, 2}, rand.New(rand.NewSource(1)), 100, 0.05); ok {
		t.Fatalf("n=2 must be insufficient_samples")
	}
	// aggregateValues 对 <3 标记 insufficient_samples
	m := aggregateValues([]float64{1, 2}, rand.New(rand.NewSource(1)))
	if m.CIStatus != "insufficient_samples" {
		t.Fatalf("ci_status=%q, want insufficient_samples", m.CIStatus)
	}
	if m.CI95Low != nil || m.CI95High != nil {
		t.Fatalf("no CI may be fabricated for n<3")
	}
}

// TestMetricAggregateSamplesFraction 混合 N/A：aggregate 只聚合实测值，samples=3/5。
func TestMetricAggregateSamplesFraction(t *testing.T) {
	runs := []*ExperimentRun{
		{Status: RunStatusCompleted, Result: ExperimentResult{P90LatencyUS: intp(1000)}},
		{Status: RunStatusCompleted, Result: ExperimentResult{P90LatencyUS: intp(1200)}},
		{Status: RunStatusCompleted, Result: ExperimentResult{}}, // 未测到 p90
		{Status: RunStatusFailed, Result: ExperimentResult{}},
	}
	exp := &Experiment{Runs: runs}
	agg := BuildExperimentAggregate(exp, 42)
	m := agg.Metrics["p90_latency_us"]
	if m == nil || !m.Measured || m.Samples != 2 || m.TotalRep != 3 {
		t.Fatalf("mixed N/A aggregation wrong: %+v (must be samples=2/3, not fake 3/3)", m)
	}
	// 失败 run 不算进 aggregate。
	if m.TotalRep != 3 || agg.SuccessfulRepetitions != 3 {
		t.Fatalf("aggregate must count only successful reps: %+v", agg)
	}
}
