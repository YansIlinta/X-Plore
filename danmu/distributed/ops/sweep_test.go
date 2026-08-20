package ops

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Sweep（§15,§16,§18）：Cartesian product / 上限 / stop / resume ---

func TestSweepCartesianProduct(t *testing.T) {
	req := SweepRequest{
		Regimes: []string{RegimeLowFanout, RegimeHotRoom},
		Params: []SweepParam{
			{Name: "batch_size", Values: []string{"100", "500", "1000", "2000"}},
			{Name: "batch_timeout", Values: []string{"5ms", "20ms"}},
		},
		Repetitions: 5,
	}
	configs, runs, err := req.Validate()
	if err != nil {
		t.Fatal(err)
	}
	// 8 configs × 2 regimes = 16 configurations × 5 reps = 80 runs。
	if configs != 16 {
		t.Fatalf("configs=%d, want 16 (8 param combos × 2 regimes)", configs)
	}
	if runs != 80 {
		t.Fatalf("runs=%d, want 80", runs)
	}
	sw, err := BuildSweepPlan(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(sw.Plan) != 16 {
		t.Fatalf("plan units=%d, want 16", len(sw.Plan))
	}
	// 每个 config × regime 都要出现且 config 序先于 regime。
	seen := map[string]bool{}
	for _, u := range sw.Plan {
		key := u.Regime + "/" + u.Label
		if seen[key] {
			t.Fatalf("duplicate plan unit %q", key)
		}
		seen[key] = true
		if u.Regime != RegimeLowFanout && u.Regime != RegimeHotRoom {
			t.Fatalf("unexpected regime %q", u.Regime)
		}
	}
	// 要求 config 连续（同 config 的 regime 连排）。
	for i := 1; i < len(sw.Plan); i++ {
		if sw.Plan[i].ConfigIdx == sw.Plan[i-1].ConfigIdx {
			continue
		}
		if sw.Plan[i].Regime != RegimeLowFanout {
			t.Fatalf("config change must restart at first regime (got %s at %d)", sw.Plan[i].Regime, i)
		}
	}
}

func TestSweepMaxLimits(t *testing.T) {
	// 超过 32 configs。
	req := SweepRequest{
		Regimes: []string{RegimeLowFanout, RegimeHotRoom},
		Params: []SweepParam{
			{Name: "batch_size", Values: []string{"1", "2", "3", "4", "5", "6", "7"}},
			{Name: "batch_timeout", Values: []string{"1ms", "2ms", "3ms", "4ms", "5ms"}},
		},
	}
	if _, _, err := req.Validate(); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("35 configs must be rejected: %v", err)
	}
	// 未知 regime。
	req2 := SweepRequest{Regimes: []string{"wat"}, Params: []SweepParam{{Name: "batch_size", Values: []string{"1"}}}}
	if _, _, err := req2.Validate(); err == nil || !strings.Contains(err.Error(), "unknown regime") {
		t.Fatalf("unknown regime must be rejected: %v", err)
	}
	// 未知参数名。
	req3 := SweepRequest{Params: []SweepParam{{Name: "nope", Values: []string{"1"}}}}
	if _, _, err := req3.Validate(); err == nil || !strings.Contains(err.Error(), "unknown sweep parameter") {
		t.Fatalf("unknown param must be rejected: %v", err)
	}
}

// TestSweepExecutionAndStop：顺序执行 + 运行中 Stop → stopped；已完成单元保留。
func TestSweepExecutionAndStop(t *testing.T) {
	dir := t.TempDir()
	store, err := NewExperimentStore(filepath.Join(dir, "data"), 50)
	if err != nil {
		t.Fatal(err)
	}
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, 1.0, nil) // 每 run 1s
	runner := NewLoadtestManager(bin, "tok", context.Background())
	em := NewExperimentManager(store, runner, dir, nil)
	swStore, _ := NewSweepStore(filepath.Join(dir, "data"), 20)
	sm := NewSweepManager(swStore, em)

	sw, err := sm.Create(SweepRequest{
		Regimes: []string{RegimeLowFanout}, Params: []SweepParam{{Name: "connections", Values: []string{"30", "60", "90"}}},
		Repetitions: 2, Warmup: "0s", Duration: "2s", Target: "ws://127.0.0.1:18081",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sw.MaxConfigs != SweepMaxConfigs || sw.MaxTotalRuns != SweepMaxTotalRuns {
		t.Fatalf("default caps wrong: %d/%d", sw.MaxConfigs, sw.MaxTotalRuns)
	}
	if err := sm.Start(sw.ID); err != nil {
		t.Fatal(err)
	}
	// 等第一个 config 启动后 Stop。
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := sm.Get(sw.ID)
		if len(got.Plan) > 0 && got.Plan[0].ExpID != "" && !got.Plan[0].Done {
			break
		}
		sleepMs(50)
	}
	if err := sm.Stop(sw.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// 轮询到 stopped。
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := sm.Get(sw.ID)
		if got.Status == SweepStatusStopped {
			break
		}
		sleepMs(50)
	}
	got, _ := sm.Get(sw.ID)
	if got.Status != SweepStatusStopped {
		t.Fatalf("sweep status=%s, want stopped", got.Status)
	}
}

// TestSweepResumeRecovery：停止后 Start 恢复剩余单元。
func TestSweepResumeRecovery(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 50)
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, 0.02, nil)
	runner := NewLoadtestManager(bin, "tok", context.Background())
	em := NewExperimentManager(store, runner, dir, nil)
	swStore, _ := NewSweepStore(filepath.Join(dir, "data"), 20)
	sm := NewSweepManager(swStore, em)

	sw, _ := sm.Create(SweepRequest{
		Regimes: []string{RegimeLowFanout}, Params: []SweepParam{{Name: "connections", Values: []string{"30", "60"}}},
		Repetitions: 1, Duration: "2s", Target: "ws://h",
	})
	_ = sm.Start(sw.ID)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := sm.Get(sw.ID)
		if got.Status == SweepStatusCompleted {
			break
		}
		sleepMs(50)
	}
	got, _ := sm.Get(sw.ID)
	if got.Status != SweepStatusCompleted {
		t.Fatalf("sweep status=%s, want completed", got.Status)
	}
	if len(got.Results) != 2 {
		t.Fatalf("results=%d, want 2", len(got.Results))
	}
	// restart（新 manager 模拟进程重启）→ 从已持久化的 sweep 恢复查看。
	swStore2, _ := NewSweepStore(filepath.Join(dir, "data"), 20)
	recovered, err := swStore2.Load(sw.ID)
	if err != nil {
		t.Fatalf("recover sweep after restart: %v", err)
	}
	if recovered.Status != SweepStatusCompleted || len(recovered.Results) != 2 {
		t.Fatalf("recovered sweep incomplete: %+v", recovered.Status)
	}
	if recovered.Report == nil || recovered.Report.AdaptiveGate.Verdict == "" {
		t.Fatalf("recovered sweep report missing gate verdict")
	}
}
