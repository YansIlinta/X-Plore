package ops

import (
	"testing"
)

// --- Best static configuration / cross-regime / dominance / adaptive gate（§19,§21,§22,§35）---

func aggMetric(mean, median, cv float64) *MetricAggregate {
	mm, md, cc := mean, median, cv
	return &MetricAggregate{Measured: true, Mean: &mm, Median: &md, CV: &cc, Samples: 3, TotalRep: 3}
}

func mkRow(regime, cfg string, through, p99, deliv float64, cfgIdx int) SweepConfigResult {
	return SweepConfigResult{
		Regime: regime, Config: cfg, ConfigIdx: cfgIdx,
		Status: ExpStatusCompleted, Repetitions: 3, SuccessReps: 3,
		Throughput:   aggMetric(through, through, 0.05),
		P99:          aggMetric(p99, p99, 0.05),
		DeliveryRate: aggMetric(deliv, deliv, 0.01),
	}
}

// TestBestStaticConfigPerRegime 约束化选优：max throughput subject to p99<=50ms。
func TestBestStaticConfigPerRegime(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 50000, DeliveryMin: 0.999}
	rows := []SweepConfigResult{
		mkRow("low-fanout", "A", 1000, 10000, 1.0, 1),
		mkRow("low-fanout", "B", 2000, 20000, 1.0, 2), // 更高吞吐，p99 仍达标
		mkRow("hot-room", "A", 800, 120000, 0.98, 1),  // p99 超标约束 → infeasible
		mkRow("hot-room", "B", 900, 30000, 1.0, 2),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout", "hot-room"}, false)
	if rep == nil {
		t.Fatal("report nil")
	}
	if b := rep.BestPerRegime["low-fanout"]; !b.Feasible || b.Config != "B" {
		t.Fatalf("low-fanout best=%+v, want B (higher throughput within constraint)", b)
	}
	if b := rep.BestPerRegime["hot-room"]; !b.Feasible || b.Config != "B" {
		t.Fatalf("hot-room best=%+v, want B", b)
	}
}

// TestConstraintViolationNoFeasible：没有任何 config 满足约束 → NO FEASIBLE CONFIGURATION。
func TestConstraintViolationNoFeasible(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 10000, DeliveryMin: 0.999}
	rows := []SweepConfigResult{
		mkRow("low-fanout", "A", 1000, 99000, 0.90, 1),
		mkRow("low-fanout", "B", 2000, 99000, 0.90, 2),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout"}, false)
	if rep == nil {
		t.Fatal("report nil")
	}
	b := rep.BestPerRegime["low-fanout"]
	if b.Feasible {
		t.Fatalf("no config may be feasible under these constraints: %+v", b)
	}
}

// TestCrossRegimeWinnerDiffers：不同 regime 选不同 best → StaticOptimumShifts。
func TestCrossRegimeWinnerDiffers(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 50000, DeliveryMin: 0}
	rows := []SweepConfigResult{
		mkRow("low-fanout", "A", 1000, 10000, 1, 1),
		mkRow("low-fanout", "B", 900, 9900, 1, 2),
		mkRow("hot-room", "A", 500, 49000, 1, 1),
		mkRow("hot-room", "B", 2000, 8000, 1, 2),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout", "hot-room"}, false)
	if !rep.Domination.StaticOptimumShifts {
		t.Fatalf("expected STATIC OPTIMUM SHIFTS (low-fanout→A, hot-room→B): %+v", rep.Domination)
	}
	if rep.Domination.OneConfigDominates {
		t.Fatalf("no single config dominates")
	}
	if rep.BestPerRegime["low-fanout"].Config != "A" || rep.BestPerRegime["hot-room"].Config != "B" {
		t.Fatalf("winners wrong: %+v", rep.BestPerRegime)
	}
}

// TestOneConfigDominatesAll：同一 config 在所有 regime 都 best → NO SHIFT。
func TestOneConfigDominatesAll(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 0, DeliveryMin: 0}
	rows := []SweepConfigResult{
		mkRow("low-fanout", "C", 3000, 5000, 1, 3),
		mkRow("low-fanout", "A", 1000, 9900, 1, 1),
		mkRow("hot-room", "C", 2500, 8000, 1, 3),
		mkRow("hot-room", "A", 500, 40000, 1, 1),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout", "hot-room"}, false)
	if !rep.Domination.OneConfigDominates || rep.Domination.DominantConfig != "C" {
		t.Fatalf("expected C dominates all: %+v", rep.Domination)
	}
	if rep.Domination.StaticOptimumShifts {
		t.Fatalf("same winner everywhere must NOT shift")
	}
}

// TestAdaptiveGateGo：A/B/C/D 齐备 → GO。
func TestAdaptiveGateGo(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 50000, DeliveryMin: 0}
	defaultRows := []SweepConfigResult{
		mkRow("low-fanout", "default", 1000, 10000, 1, 0),
		mkRow("hot-room", "default", 800, 40000, 1, 0),
	}
	slidingRows := []SweepConfigResult{
		// low-fanout 偏好 B（更高吞吐）；hot-room 偏好 A（A 在热房吞吐反超）。
		mkRow("low-fanout", "A", 1000, 10000, 1, 1),
		mkRow("low-fanout", "B", 3000, 12000, 1, 2),
		mkRow("hot-room", "A", 2500, 20000, 1, 1),
		mkRow("hot-room", "B", 900, 9000, 1, 2),
	}
	rows := append(append([]SweepConfigResult{}, defaultRows...), slidingRows...)
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout", "hot-room"}, true)
	g := rep.AdaptiveGate
	if rep.BestPerRegime["low-fanout"].Config != "B" || rep.BestPerRegime["hot-room"].Config != "A" {
		t.Fatalf("winners wrong: %+v", rep.BestPerRegime)
	}
	if !g.ConditionA {
		t.Fatalf("condition A expected true (optimum shifts? check): %+v", g)
	}
	if !g.ConditionD {
		t.Fatalf("condition D expected true (tunable param)")
	}
	if g.Go != (g.ConditionA && g.ConditionB && g.ConditionC && g.ConditionD) {
		t.Fatalf("gate must be deterministic conjunction: %+v", g)
	}
}

// TestAdaptiveGateNotYetJustified：单 regime → NOT YET JUSTIFIED。
func TestAdaptiveGateNotYetJustified(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 0, DeliveryMin: 0}
	rows := []SweepConfigResult{
		mkRow("low-fanout", "A", 1000, 5000, 1, 1),
		mkRow("low-fanout", "B", 1500, 6000, 1, 2),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"low-fanout"}, true)
	if rep.AdaptiveGate.Go || rep.AdaptiveGate.Verdict != "NOT YET JUSTIFIED" {
		t.Fatalf("single-regime must be NOT YET JUSTIFIED: %+v", rep.AdaptiveGate)
	}
	if rep.AdaptiveGate.ConditionA {
		t.Fatalf("condition A needs >=2 regimes with differing best")
	}
}

// TestDominanceRuleAsymmetric：config A 在所有目标上不劣于 B 且至少一个更优 → A 支配 B。
func TestDominanceRuleAsymmetric(t *testing.T) {
	obj := RankingObjective{Primary: "throughput", Maximize: true, P99MaxUS: 0, DeliveryMin: 0}
	rows := []SweepConfigResult{
		mkRow("r1", "A", 2000, 10000, 1, 1),
		mkRow("r1", "B", 1000, 10000, 1, 2),
		mkRow("r2", "A", 2500, 11000, 1, 1),
		mkRow("r2", "B", 1200, 12000, 1, 2),
	}
	rep := BuildCrossRegimeReport(rows, obj, []string{"r1", "r2"}, false)
	if !rep.Domination.OneConfigDominates || rep.Domination.DominantConfig != "A" {
		t.Fatalf("A should dominate (better throughput, no worse p99): %+v", rep.Domination)
	}
}
