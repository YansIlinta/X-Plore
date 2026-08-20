package ops

import (
	"strings"
	"testing"
	"time"
)

func strPtr(s string) *string { return &s }

func intp(v int64) *int64 { return &v }

// mkExp 构造一个可直接用于 compare/evidence 的已完结实验（测值由闭包设置）。
func mkExp(id, arch string, set func(r *ExperimentResult)) *Experiment {
	r := ExperimentResult{}
	if set != nil {
		set(&r)
	}
	now := time.Now().UTC()
	return &Experiment{
		ID: id, Name: "exp " + id, Architecture: arch, Status: ExpStatusCompleted,
		Workload:    WorkloadConfig{Connections: 1000, Rooms: 100, MessageRate: 1, Duration: "60s", Target: "ws://h"},
		CreatedAt:   now,
		StartedAt:   &now,
		FinishedAt:  &now,
		Environment: &EnvironmentSnapshot{GitCommit: strPtr("abc123"), GoVersion: "go1.26"},
		Result:      r,
	}
}

func rowsByKey(out *CompareResult) map[string]CompareRow {
	m := map[string]CompareRow{}
	for _, row := range out.Rows {
		m[row.Metric] = row
	}
	return m
}

func TestCompareBasicRowsAndDelta(t *testing.T) {
	a := mkExp("a", ArchMonolith, func(r *ExperimentResult) {
		r.ConnectionsEstablished = intp(1000)
		r.MessagesReceived = intp(95000)
		r.P90LatencyUS = intp(500)
		r.P50LatencyUS = intp(300)
		r.ReadErrors = intp(10)
		r.SendRate = newf64(1000)
	})
	b := mkExp("b", ArchMonolith, func(r *ExperimentResult) {
		r.ConnectionsEstablished = intp(1000)
		r.MessagesReceived = intp(98000)
		r.P90LatencyUS = intp(440) // -12%
		r.P50LatencyUS = intp(300)
		r.ReadErrors = intp(20)
		r.SendRate = newf64(1030)
	})
	out := CompareExperiments(a, b)
	if out.Left.ID != "a" || out.Right.ID != "b" {
		t.Fatalf("refs wrong")
	}
	byKey := rowsByKey(out)
	// P90：-60，Δpct≈-12%，better（低于）
	row := byKey["p90_latency_us"]
	if row.Delta == nil || *row.Delta != -60 {
		t.Fatalf("p90 delta=%v", row.Delta)
	}
	if row.DeltaPct == nil || *row.DeltaPct > -11.9 || *row.DeltaPct < -12.1 {
		t.Fatalf("p90 pct=%v", row.DeltaPct)
	}
	if row.Verdict != "better" {
		t.Fatalf("p90 verdict=%s", row.Verdict)
	}
	// read_errors：+10，worse
	row = byKey["read_errors"]
	if row.Verdict != "worse" {
		t.Fatalf("read_errors verdict=%s", row.Verdict)
	}
	// connections_requested 无方向 → verdict ""
	row = byKey["connections_requested"]
	if row.Verdict != "" {
		t.Fatalf("requested must have no verdict, got %s", row.Verdict)
	}
}

func TestCompareNANotComputed(t *testing.T) {
	a := mkExp("a", ArchDistributed, func(r *ExperimentResult) {
		r.KafkaLag = intp(500)
		r.P90LatencyUS = intp(200)
	})
	b := mkExp("b", ArchMonolith, func(r *ExperimentResult) {
		r.P90LatencyUS = intp(210)
		// 无 KafkaLag（monolith 不支持 → null）
	})
	out := CompareExperiments(a, b)
	byKey := rowsByKey(out)
	row := byKey["kafka_lag"]
	if row.Left == nil || row.Right != nil || row.Delta != nil {
		t.Fatalf("kafka_lag N/A semantics broken: %+v", row)
	}
	// p90 两侧都有 → 正常算
	row = byKey["p90_latency_us"]
	if row.Delta == nil || *row.Delta != 10 {
		t.Fatalf("p90 delta=%v", row.Delta)
	}
}

func TestComparePercentWhenLeftZero(t *testing.T) {
	a := mkExp("a", ArchMonolith, func(r *ExperimentResult) { r.WriteErrors = intp(0) })
	b := mkExp("b", ArchMonolith, func(r *ExperimentResult) { r.WriteErrors = intp(1) })
	out := CompareExperiments(a, b)
	row := rowsByKey(out)["write_errors"]
	if row.Delta == nil || *row.Delta != 1 {
		t.Fatalf("delta=%v", row.Delta)
	}
	if row.DeltaPct != nil {
		t.Fatalf("delta_pct must be nil when left==0, got %v", *row.DeltaPct)
	}
}

func TestCompareSummaryDeterministic(t *testing.T) {
	left := mkExp("a", ArchMonolith, func(r *ExperimentResult) {
		r.P90LatencyUS = intp(10000)
		r.ReadErrors = intp(2)
		r.ReceiveRate = newf64(1000)
	})
	right := mkExp("b", ArchMonolith, func(r *ExperimentResult) {
		r.P90LatencyUS = intp(5000) // -50% → better
		r.ReadErrors = intp(50)     // +2400% → worse
		r.ReceiveRate = newf64(1000)
	})
	out := CompareExperiments(left, right)
	if len(out.Summary) == 0 {
		t.Fatalf("no summary")
	}
	joined := strings.Join(out.Summary, "\n")
	if !strings.Contains(joined, "Run B achieved lower P90 latency") {
		t.Fatalf("missing latency sentence: %v", out.Summary)
	}
	if !strings.Contains(joined, "Run B produced more read errors") {
		t.Fatalf("missing reliability sentence: %v", out.Summary)
	}
	// 确定性：两次结果逐字一致
	out2 := CompareExperiments(left, right)
	if strings.Join(out.Summary, "\n") != strings.Join(out2.Summary, "\n") {
		t.Fatalf("summary not deterministic: %v vs %v", out.Summary, out2.Summary)
	}
}

func TestCompareSummaryNoNotable(t *testing.T) {
	left := mkExp("a", ArchMonolith, func(r *ExperimentResult) {
		r.P90LatencyUS = intp(10000)
		r.ReadErrors = intp(2)
	})
	right := mkExp("b", ArchMonolith, func(r *ExperimentResult) {
		r.P90LatencyUS = intp(10200) // +2% (<10%)
		r.ReadErrors = intp(2)
	})
	out := CompareExperiments(left, right)
	if len(out.Summary) != 1 || !strings.Contains(out.Summary[0], "No notable difference") {
		t.Fatalf("expected no-notable summary, got %v", out.Summary)
	}
}

func TestCompareVerdictDirection(t *testing.T) {
	// 方向语义：established 高更好；p90 低更好。
	a := mkExp("a", ArchMonolith, func(r *ExperimentResult) {
		r.ConnectionsEstablished = intp(1000)
		r.P90LatencyUS = intp(1000)
	})
	b := mkExp("b", ArchMonolith, func(r *ExperimentResult) {
		r.ConnectionsEstablished = intp(1200) // +200 → 高更好 → better
		r.P90LatencyUS = intp(1200)           // +200 → 低更好 → worse
	})
	out := CompareExperiments(a, b)
	byKey := rowsByKey(out)
	if byKey["connections_established"].Verdict != "better" {
		t.Fatalf("established+: verdict=%s, want better", byKey["connections_established"].Verdict)
	}
	if byKey["p90_latency_us"].Verdict != "worse" {
		t.Fatalf("p90+: verdict=%s, want worse", byKey["p90_latency_us"].Verdict)
	}
}

func TestCompareWorkloadNoteWhenDifferent(t *testing.T) {
	left := mkExp("a", ArchMonolith, func(r *ExperimentResult) {})
	left.Workload = WorkloadConfig{Connections: 100, Rooms: 50, MessageRate: 1, Duration: "30s", Target: "ws://h"}
	right := mkExp("b", ArchMonolith, func(r *ExperimentResult) {})
	out := CompareExperiments(left, right)
	joined := strings.Join(out.Summary, "\n")
	if !strings.Contains(joined, "different workloads") {
		t.Fatalf("expected workload note when workloads differ, got: %v", out.Summary)
	}
}

func TestCompareRejectsNotCompleted(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	a, _ := m.Create(customReq("a")) // created，未完成
	b, _ := m.Create(customReq("b"))
	if _, err := m.Compare(a.ID, b.ID); err == nil || !strings.Contains(err.Error(), "requires completed") {
		t.Fatalf("compare of non-completed experiments must be rejected, got %v", err)
	}
}

func TestCompareMissingIDs(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	if _, err := m.Compare("exp-nope", "exp-nope"); err == nil {
		t.Fatalf("compare with missing id must error")
	}
}
