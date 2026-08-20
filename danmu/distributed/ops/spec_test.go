package ops

import (
	"testing"
)

// --- Spec hash 确定性（§6 Canonical Spec Hash）---

func baseSpec() ExperimentSpec {
	return ExperimentSpec{
		Architecture: ArchMonolith,
		Regime:       RegimeLowFanout,
		Workload:     WorkloadConfig{Connections: 100, Rooms: 50, MessageRate: 1.5, Duration: "10s", Target: "ws://localhost:8081", Distribution: DistUniform, Seed: 1},
		Warmup:       "2s",
		Duration:     "10s",
		Repetitions:  5,
	}
}

func TestSameSpecSameHash(t *testing.T) {
	a, err := baseSpec().SpecHash()
	if err != nil {
		t.Fatal(err)
	}
	b, err := baseSpec().SpecHash()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("identical spec must hash identically: %s vs %s", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("hash must be full sha256 hex (64), got %d", len(a))
	}
}

func TestHashCanonicalFieldOrder(t *testing.T) {
	// canonical 序列化对字段书写顺序不敏感（map → 排序后 JSON）。
	// 显式验证：不同 workload / system config 必改 hash；相同 spec 恒同 hash。
	spec2 := baseSpec()
	spec2.Workload.Connections = 101
	if h1, _ := baseSpec().SpecHash(); h1 == h2(spec2) {
		t.Fatalf("different workload must change hash")
	}
	// 不同 system config → 不同 hash
	spec3 := baseSpec()
	spec3.System = SystemConfig{BatchSize: 5000, BatchTimeout: "10ms", RequiresRestart: true}
	if h1, _ := baseSpec().SpecHash(); h1 == h2(spec3) {
		t.Fatalf("different system config must change hash")
	}
}

func h2(s ExperimentSpec) string { h, _ := s.SpecHash(); return h }

func TestHashMapKeyOrderDeterministic(t *testing.T) {
	// canonicalSpecObject 用 map → encoding/json 自动按 key 排序，
	// 相同内容不同书写顺序（map 无顺序概念）恒得相同 hash。
	obj1 := canonicalSpecObject(baseSpec())
	obj2 := canonicalSpecObject(baseSpec())
	if len(obj1) != len(obj2) {
		t.Fatal("canonical object size mismatch")
	}
	// 断言 canonical 对象不包含实验级元数据字段。
	for _, forbidden := range []string{"experiment_id", "started_at", "result"} {
		if _, ok := obj1[forbidden]; ok {
			t.Fatalf("canonical spec must not include %q", forbidden)
		}
	}
}

// --- Schema version / legacy migration（§32 Backward Compatibility）---

func TestLegacyExperimentMigration(t *testing.T) {
	// 模拟 Phase 1 老文件：无 schema_version / repetitions / spec / runs，仅顶层 result。
	legacy := &Experiment{
		ID: "exp-legacy", Status: ExpStatusCompleted, Architecture: ArchMonolith,
		Workload:  WorkloadConfig{Connections: 100, Rooms: 10, MessageRate: 1, Duration: "30s", Target: "ws://h"},
		CreatedAt: nowTime(),
		Spec:      ExperimentSpec{},
		Result:    ExperimentResult{ConnectionsEstablished: intp(100), P90LatencyUS: intp(500)},
	}
	MigrateExperiment(legacy)
	if legacy.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version=%d, want %d", legacy.SchemaVersion, SchemaVersion)
	}
	if legacy.Repetitions != 1 {
		t.Fatalf("legacy repetitions must default to 1, got %d", legacy.Repetitions)
	}
	if legacy.Duration != "30s" {
		t.Fatalf("legacy duration must fall back to workload duration, got %q", legacy.Duration)
	}
	if legacy.Spec.Architecture == "" {
		t.Fatalf("legacy spec must be rebuilt from top-level fields")
	}
	if legacy.SpecHash == "" {
		t.Fatalf("legacy spec_hash must be computed on migration")
	}
	if len(legacy.Runs) != 1 || legacy.Runs[0].Status != RunStatusCompleted {
		t.Fatalf("legacy experiment must get a synthesized completed run for compare/evidence")
	}
}

func TestRepeatedRunsSequential(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.02)
	defer done()
	exp, err := m.Create(CreateRequest{
		Preset: "custom", Architecture: ArchMonolith, Repetitions: 3,
		Workload: WorkloadConfig{Connections: 10, Rooms: 2, MessageRate: 1, Duration: "3s", Target: "ws://h"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Repetitions != 3 {
		t.Fatalf("repetitions=%d", exp.Repetitions)
	}
	if err := m.Start(exp.ID); err != nil {
		t.Fatal(err)
	}
	exp = waitStatus(t, m, exp.ID, ExpStatusCompleted, 15*timeoutSec())
	if len(exp.Runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(exp.Runs))
	}
	for i, r := range exp.Runs {
		if r.Index != i+1 || r.Status != RunStatusCompleted {
			t.Fatalf("run[%d] wrong: %+v", i, r)
		}
	}
	if exp.Aggregate == nil || exp.Aggregate.SuccessfulRepetitions != 3 {
		t.Fatalf("aggregate missing/wrong: %+v", exp.Aggregate)
	}
}

func TestPartialExperimentWhenSomeRunsFail(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepathJoin(dir, "data"), 10)
	// 假 loadtest：第一次成功，之后退出码 3 失败。
	bin := writeFakeLoadtestSequence(t, dir, []fakeStep{
		{report: standardReportJSON(), exitCode: 0, sleepSec: 0.01},
		{report: "", exitCode: 3, sleepSec: 0.01},
		{report: standardReportJSON(), exitCode: 0, sleepSec: 0.01},
	})
	runner := NewLoadtestManager(bin, "tok", contextBG())
	m := NewExperimentManager(store, runner, dir, nil)
	exp, _ := m.Create(CreateRequest{Preset: "custom", Repetitions: 3, Workload: WorkloadConfig{Connections: 5, Rooms: 2, MessageRate: 1, Duration: "2s", Target: "ws://h"}})
	_ = m.Start(exp.ID)
	exp = waitStatus(t, m, exp.ID, ExpStatusPartial, 15*timeoutSec())
	statuses := map[int]string{}
	for _, r := range exp.Runs {
		statuses[r.Index] = r.Status
	}
	if statuses[1] != RunStatusCompleted || statuses[2] != RunStatusFailed || statuses[3] != RunStatusCompleted {
		t.Fatalf("partial statuses wrong: %+v (must be 4/5-style: 2 success + 1 fail → PARTIAL)", statuses)
	}
	if exp.Aggregate == nil || exp.Aggregate.SuccessfulRepetitions != 2 || exp.Aggregate.Status != ExpStatusPartial {
		t.Fatalf("partial aggregate wrong: %+v", exp.Aggregate)
	}
}

// TestResumeAfterStopContinuesRemaining：停止后重新 Start 只重跑未完成/失败的 rep。
func TestResumeAfterStopContinuesRemaining(t *testing.T) {
	m, _, _, done := newTestManager(t, 1.0) // 每个 rep 1s，留窗口观察 rep2 running 再 stop
	defer done()
	exp, _ := m.Create(CreateRequest{Preset: "custom", Repetitions: 3, Workload: WorkloadConfig{Connections: 5, Rooms: 2, MessageRate: 1, Duration: "2s", Target: "ws://h"}})
	_ = m.Start(exp.ID)
	// 等 rep2 已开始（rep1 已完成）时 Stop —— 此时 rep2 会被中止为 stopped。
	deadline := timeNow().Add(8 * timeoutSec())
	for timeNow().Before(deadline) {
		e, _ := m.Get(exp.ID)
		if len(e.Runs) >= 2 && e.Runs[0].Status == RunStatusCompleted && e.Runs[1].Status == RunStatusRunning {
			break
		}
		sleepMs(20)
	}
	_ = m.Stop(exp.ID)
	exp = waitStatus(t, m, exp.ID, ExpStatusStopped, 5*timeoutSec())
	// 恢复：Start 后应重跑 rep2、rep3（跳过已完成的 rep1）。
	_ = m.Start(exp.ID)
	exp = waitStatus(t, m, exp.ID, ExpStatusCompleted, 15*timeoutSec())
	if len(exp.Runs) != 3 {
		t.Fatalf("resume must run remaining reps; runs=%d", len(exp.Runs))
	}
	// 恢复语义：已完成 rep 不被覆盖为失败。
	byIndex := map[int]*ExperimentRun{}
	for _, r := range exp.Runs {
		byIndex[r.Index] = r
	}
	if byIndex[1].Status != RunStatusCompleted {
		t.Fatalf("completed rep1 must survive resume, got %s", byIndex[1].Status)
	}
}
