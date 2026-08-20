package ops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- fake loadtest runner：真实子进程（shell 脚本），让实验管理器走完整生命周期 ----

// writeFakeLoadtest 生成一个可执行的"假 loadtest"，参数化：
//
//	reportJSON  —— 写进 -output-json 的完整报告（"" = 不写文件，模拟异常）
//	exitCode    —— 进程退出码（非 0 模拟运行失败）
//	sleepSec    —— 模拟运行耗时（秒）
//	lines       —— 依次 echo 到 stdout 的秒级快照行
func writeFakeLoadtest(t *testing.T, dir, name string, reportJSON string, exitCode int, sleepSec float64, lines []string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("out=\"\"\n")
	b.WriteString("while [ \"$#\" -gt 0 ]; do\n")
	b.WriteString("  if [ \"$1\" = \"-output-json\" ]; then out=\"$2\"; shift 2; else shift; fi\n")
	b.WriteString("done\n")
	if reportJSON != "" {
		b.WriteString("REPORT=" + shellQuote(reportJSON) + "\n")
		b.WriteString("if [ -n \"$out\" ]; then printf '%s' \"$REPORT\" > \"$out\"; fi\n")
	}
	for _, line := range lines {
		b.WriteString("echo " + shellQuote(line) + "\n")
	}
	b.WriteString(fmt.Sprintf("sleep %v\n", sleepSec))
	b.WriteString(fmt.Sprintf("exit %d\n", exitCode))
	if err := os.WriteFile(path, []byte(b.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// standardReportJSON 是默认的"成功"报告（真实测量值，包含'测得0'与'nil'的区分）。
func standardReportJSON() string {
	return `{
  "summary": {
    "target_conns": 100, "success_conns": 100, "failed_conns": 0,
    "total_sent": 5000, "total_recv": 4800,
    "e2e_p50_us": 500, "e2e_p90_us": 900, "e2e_p99_us": 1500, "e2e_p999_us": 3000, "e2e_max_us": 8000
  },
  "snapshots": [
    {"Time":"00:00:01","ActiveConns":100,"WriteErrors":0,"ReadErrors":2}
  ]
}`
}

// newTestManager 构造一个跑 fake loadtest 的真实 Manager。
func newTestManager(t *testing.T, runSec float64) (*ExperimentManager, *experimentStore, *loadtestManager, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewExperimentStore(filepath.Join(dir, "data"), 200)
	if err != nil {
		t.Fatal(err)
	}
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, runSec, []string{
		"[00:00:01] conns=100/100 sendQPS=5000 recvQPS=4800 e2e_p50=500μs p90=900μs p99=1500μs errs(w=0 r=2) goroutines=10 heap=2MB",
	})
	runner := NewLoadtestManager(bin, "tok", context.Background())
	m := NewExperimentManager(store, runner, dir, nil)
	return m, store, runner, func() {}
}

func customReq(name string) CreateRequest {
	return CreateRequest{
		Name: name, Preset: "custom",
		Workload: WorkloadConfig{Connections: 100, Rooms: 10, MessageRate: 1, Duration: "5s", Target: "ws://localhost:8081"},
	}
}

// waitStatus 轮询直到实验进入 want 状态（或超时）。
func waitStatus(t *testing.T, m *ExperimentManager, id, want string, timeout time.Duration) *Experiment {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exp, err := m.Get(id)
		if err == nil && exp.Status == want {
			return exp
		}
		time.Sleep(20 * time.Millisecond)
	}
	exp, _ := m.Get(id)
	t.Fatalf("experiment %s did not reach %q in %v (last: %+v)", id, want, timeout, exp)
	return nil
}

// waitObserverEvent 轮询直到 observer 收到包含 sub 的事件。
func waitObserverEvent(t *testing.T, o *fakeObserver, sub string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range o.snapshotEvents() {
			if strings.Contains(ev.Message, sub) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("observer never saw event containing %q: %+v", sub, o.snapshotEvents())
}

// ---- 创建与校验 ----

func TestCreatePresetLowFanout(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	exp, err := m.Create(CreateRequest{Preset: "low-fanout"})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Status != ExpStatusCreated {
		t.Fatalf("status=%s", exp.Status)
	}
	if exp.Workload.Connections != 2000 || exp.Workload.Rooms != 1000 {
		t.Fatalf("preset workload not applied: %+v", exp.Workload)
	}
	if err := ValidateExperimentID(exp.ID); err != nil {
		t.Fatalf("generated id invalid: %v", err)
	}
}

func TestCreateExplicitWorkload(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	exp, err := m.Create(CreateRequest{
		Preset:       "hot-room",
		Architecture: ArchDistributed,
		Workload: WorkloadConfig{
			Connections: 7, Rooms: 3, MessageRate: 0.5,
			Duration: "10s", Target: "ws://localhost:9999",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Workload.Connections != 7 || exp.Workload.Rooms != 3 {
		t.Fatalf("explicit workload ignored: %+v", exp.Workload)
	}
	if exp.Architecture != ArchDistributed {
		t.Fatalf("arch=%s", exp.Architecture)
	}
}

func TestCreateValidation(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	cases := []struct {
		name string
		req  CreateRequest
		want string
	}{
		{"bad arch", CreateRequest{Architecture: "k8s"}, "architecture"},
		{"bad preset", CreateRequest{Preset: "wat", Architecture: ArchMonolith}, "unknown preset"},
		{"bad duration", CreateRequest{Preset: "custom", Workload: WorkloadConfig{Connections: 10, Rooms: 2, MessageRate: 1, Duration: "soon", Target: "ws://h"}}, "duration"},
		{"bad target scheme", CreateRequest{Preset: "custom", Workload: WorkloadConfig{Connections: 10, Rooms: 2, MessageRate: 1, Duration: "10s", Target: "http://h"}}, "ws://"},
		{"bad conns", CreateRequest{Preset: "custom", Workload: WorkloadConfig{Connections: 0, Rooms: 2, MessageRate: 1, Duration: "10s", Target: "ws://h"}}, "connections"},
	}
	for _, c := range cases {
		if _, err := m.Create(c.req); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got err=%v, want contains %q", c.name, err, c.want)
		}
	}
}

func TestValidateExperimentIDRejectsTraversal(t *testing.T) {
	for _, id := range []string{"../x", "a/b", "a\\b", ".", "..", "exp x", "/etc/passwd", "exp..", "a//b", ""} {
		if err := ValidateExperimentID(id); err == nil {
			t.Errorf("id %q must be rejected", id)
		}
	}
	store, err := NewExperimentStore(t.TempDir(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&Experiment{ID: "../evil", Name: "x"}); err == nil {
		t.Fatalf("store must reject traversal id on Save")
	}
	if _, err := store.Load("../evil"); err == nil {
		t.Fatalf("store must reject traversal id on Load")
	}
}

// ---- 状态机 ----

func TestStateMachineHappyPath(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.05)
	defer done()
	exp, _ := m.Create(customReq("happy"))
	if err := m.Start(exp.ID); err != nil {
		t.Fatal(err)
	}
	exp = waitStatus(t, m, exp.ID, ExpStatusCompleted, 5*time.Second)
	if exp.StartedAt == nil || exp.FinishedAt == nil || exp.FinishedAt.Before(*exp.StartedAt) {
		t.Fatalf("timestamps wrong: %+v", exp)
	}
	if exp.Environment == nil {
		t.Fatalf("environment not captured")
	}
	r := exp.Result
	if r.ConnectionsEstablished == nil || *r.ConnectionsEstablished != 100 {
		t.Fatalf("established=%v", r.ConnectionsEstablished)
	}
	if r.P90LatencyUS == nil || *r.P90LatencyUS != 900 {
		t.Fatalf("p90=%v", r.P90LatencyUS)
	}
	if r.Drops != nil {
		t.Fatalf("drops must be N/A (nil), got %d (loadtest does not measure it)", *r.Drops)
	}
	if r.WriteErrors == nil || *r.WriteErrors != 0 {
		t.Fatalf("write_errors must be measured 0, got %v", r.WriteErrors) // 真实测得的 0
	}
	if r.ReadErrors == nil || *r.ReadErrors != 2 {
		t.Fatalf("read_errors=%v", r.ReadErrors)
	}
	if r.SendRate == nil || *r.SendRate <= 0 {
		t.Fatalf("send_rate=%v", r.SendRate)
	}
	if m.ActiveID() != "" {
		t.Fatalf("active not released after completion")
	}
}

func TestStateMachineTransitions(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.05)
	defer done()
	exp, _ := m.Create(customReq("trans"))
	// created → start（合法）
	if err := m.Start(exp.ID); err != nil {
		t.Fatal(err)
	}
	// running → 再 start（非法）
	if err := m.Start(exp.ID); err == nil {
		t.Fatalf("start on running must fail")
	}
	// running → stop（合法）
	if err := m.Stop(exp.ID); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, exp.ID, ExpStatusStopped, 5*time.Second)
	// stopped → start（重新跑同一实验，允许）
	if err := m.Start(exp.ID); err != nil {
		t.Fatalf("restart after stopped should work: %v", err)
	}
	waitStatus(t, m, exp.ID, ExpStatusCompleted, 5*time.Second)
}

func TestStopOnNotRunning(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.01)
	defer done()
	exp, _ := m.Create(customReq("notrun"))
	if err := m.Stop(exp.ID); err == nil {
		t.Fatalf("stop on created must fail")
	}
}

func TestDuplicateConcurrentStart(t *testing.T) {
	m, _, _, done := newTestManager(t, 0.2)
	defer done()
	a, _ := m.Create(customReq("a"))
	b, _ := m.Create(customReq("b"))
	if err := m.Start(a.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(b.ID); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("second start must conflict, got %v", err)
	}
	if m.ActiveID() != a.ID {
		t.Fatalf("active=%s, want %s", m.ActiveID(), a.ID)
	}
	// 结束后重获单例
	waitStatus(t, m, a.ID, ExpStatusCompleted, 5*time.Second)
	if err := m.Start(b.ID); err != nil {
		t.Fatalf("start after completion should work: %v", err)
	}
	waitStatus(t, m, b.ID, ExpStatusCompleted, 5*time.Second)
}

func TestRunnerFailureMarksFailed(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	bin := writeFakeLoadtest(t, dir, "loadtest", "", 3, 0.01, nil) // 退出码 3，无报告
	runner := NewLoadtestManager(bin, "tok", context.Background())
	m := NewExperimentManager(store, runner, dir, nil)
	exp, _ := m.Create(customReq("fail"))
	if err := m.Start(exp.ID); err != nil {
		t.Fatal(err)
	}
	exp = waitStatus(t, m, exp.ID, ExpStatusFailed, 5*time.Second)
	if exp.Error == "" {
		t.Fatalf("failed experiment should carry error")
	}
}

func TestStartWhenBinaryMissing(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	runner := NewLoadtestManager(filepath.Join(dir, "nope"), "tok", context.Background())
	m := NewExperimentManager(store, runner, dir, nil)
	exp, _ := m.Create(customReq("missing"))
	err := m.Start(exp.ID)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing binary must fail start, got %v", err)
	}
	exp, _ = m.Get(exp.ID)
	if exp.Status != ExpStatusFailed {
		t.Fatalf("status=%s, want failed", exp.Status)
	}
}

// ---- 持久化 / 恢复 / 损坏容忍 ----

func TestPersistenceRecoveryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 200)
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, 0.02, nil)
	runner := NewLoadtestManager(bin, "tok", context.Background())
	m := NewExperimentManager(store, runner, dir, nil)
	exp, _ := m.Create(customReq("persist"))
	_ = m.Start(exp.ID)
	waitStatus(t, m, exp.ID, ExpStatusCompleted, 5*time.Second)
	id := exp.ID

	store2, _ := NewExperimentStore(filepath.Join(dir, "data"), 200)
	m2 := NewExperimentManager(store2, NewLoadtestManager(bin, "tok", context.Background()), dir, nil)
	recovered, err := m2.Get(id)
	if err != nil {
		t.Fatalf("recover after restart: %v", err)
	}
	if recovered.Status != ExpStatusCompleted || recovered.Result.ConnectionsEstablished == nil || *recovered.Result.ConnectionsEstablished != 100 {
		t.Fatalf("recovered experiment incomplete: %+v", recovered)
	}
	list, _ := m2.List()
	if len(list) != 1 || list[0].ID != id {
		t.Fatalf("list after restart: %+v", list)
	}
}

func TestStoreCorruptedFileTolerated(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 200)
	good := &Experiment{ID: newExperimentID(), Name: "good", Status: ExpStatusCompleted, CreatedAt: time.Now().UTC()}
	if err := store.Save(good); err != nil {
		t.Fatal(err)
	}
	badID := newExperimentID()
	if err := os.WriteFile(filepath.Join(store.Dir(), badID+".json"), []byte("{ not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	all, err := store.List()
	if err != nil {
		t.Fatalf("List must tolerate corrupted file: %v", err)
	}
	ids := map[string]bool{}
	for _, e := range all {
		ids[e.ID] = true
	}
	if !ids[good.ID] {
		t.Fatalf("good experiment missing after corrupted file: %v", all)
	}
	if ids[badID] {
		t.Fatalf("corrupted experiment must be skipped")
	}
	if _, err := store.Load(badID); err == nil {
		t.Fatalf("Load of corrupted must error")
	}
}

func TestStoreBoundedHistory(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	for i := 0; i < 25; i++ {
		exp := &Experiment{ID: newExperimentID(), Name: fmt.Sprintf("e%d", i), Status: ExpStatusCompleted, CreatedAt: time.Now().UTC().Add(-time.Duration(i) * time.Second)}
		if err := store.Save(exp); err != nil {
			t.Fatal(err)
		}
	}
	all, _ := store.List()
	if len(all) != 10 {
		t.Fatalf("bounded history len=%d, want 10", len(all))
	}
	if all[0].Name != "e0" {
		t.Fatalf("list not newest-first: %s", all[0].Name)
	}
}

func TestStoreAtomicNoTempLeftBehind(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	exp := &Experiment{ID: newExperimentID(), Name: "x", Status: ExpStatusCreated, CreatedAt: time.Now().UTC()}
	for i := 0; i < 20; i++ {
		if err := store.Save(exp); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(store.Dir())
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Fatalf("temp file left: %s", e.Name())
		}
	}
}

// ---- 分布式附加观测（fake observer）----

type fakeObserver struct {
	mu     sync.Mutex
	snap   Snapshot
	traces []Trace
	events []Event
}

func newFakeObserver(snap Snapshot, traces []Trace) *fakeObserver {
	return &fakeObserver{snap: snap, traces: traces}
}
func (f *fakeObserver) Snapshot() Snapshot   { return f.snap }
func (f *fakeObserver) Traces(n int) []Trace { return f.traces }
func (f *fakeObserver) AddEvent(level, kind, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, Event{TS: time.Now(), Level: level, Kind: kind, Message: message})
}
func (f *fakeObserver) snapshotEvents() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Event, len(f.events))
	copy(out, f.events)
	return out
}

func distSnapshot() Snapshot {
	lag := int64(42)
	return Snapshot{
		EtcdUp: true,
		Health: healthHealthy,
		Services: []Service{
			{Name: "comet", Instances: []Instance{{HTTPAddr: "h1", Healthy: true}, {HTTPAddr: "h2", Healthy: false}}},
			{Name: "logic", Instances: []Instance{{HTTPAddr: "h3", Healthy: true}}},
			{Name: "job", Instances: []Instance{{HTTPAddr: "h4", Healthy: true}}},
		},
		Kafka: KafkaInfo{Available: true, Lag: map[string]*int64{"danmu-job": &lag}},
	}
}

func TestDistributedSnapshotCapture(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, 0.02, nil)
	runner := NewLoadtestManager(bin, "tok", context.Background())
	obs := newFakeObserver(distSnapshot(), []Trace{
		{MsgID: "t1", Complete: true},
		{MsgID: "t2", Complete: false},
		{MsgID: "t3", Complete: true},
	})
	m := NewExperimentManager(store, runner, dir, obs)
	exp, _ := m.Create(CreateRequest{Name: "dist", Preset: "custom", Architecture: ArchDistributed, Workload: WorkloadConfig{Connections: 10, Rooms: 2, MessageRate: 1, Duration: "5s", Target: "ws://h"}})
	_ = m.Start(exp.ID)
	exp = waitStatus(t, m, exp.ID, ExpStatusCompleted, 5*time.Second)

	if exp.Result.ServiceSnapshot == nil {
		t.Fatalf("distributed service snapshot missing")
	}
	d := exp.Result.ServiceSnapshot
	if d.CometTotal != 2 || d.CometHealthy != 1 || d.LogicTotal != 1 || d.JobTotal != 1 {
		t.Fatalf("service counts wrong: %+v", d)
	}
	if exp.Result.KafkaAvailable == nil || !*exp.Result.KafkaAvailable {
		t.Fatalf("kafka available not recorded")
	}
	if exp.Result.KafkaLag == nil || *exp.Result.KafkaLag != 42 {
		t.Fatalf("kafka lag=%v", exp.Result.KafkaLag)
	}
	if exp.Result.TraceSamples == nil || *exp.Result.TraceSamples != 3 {
		t.Fatalf("trace samples=%v", exp.Result.TraceSamples)
	}
	if exp.Result.TraceCompletion == nil {
		t.Fatalf("trace completion nil")
	}
	waitObserverEvent(t, obs, "started", 3*time.Second)
}

func TestMonolithHasNoDistributedFields(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewExperimentStore(filepath.Join(dir, "data"), 10)
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, 0.02, nil)
	runner := NewLoadtestManager(bin, "tok", context.Background())
	obs := newFakeObserver(distSnapshot(), nil)
	m := NewExperimentManager(store, runner, dir, obs)
	exp, _ := m.Create(CreateRequest{Name: "mono", Preset: "custom", Architecture: ArchMonolith, Workload: WorkloadConfig{Connections: 10, Rooms: 2, MessageRate: 1, Duration: "5s", Target: "ws://h"}})
	_ = m.Start(exp.ID)
	exp = waitStatus(t, m, exp.ID, ExpStatusCompleted, 5*time.Second)
	if exp.Result.ServiceSnapshot != nil {
		t.Fatalf("monolith must not fabricate a distributed snapshot, got %+v", exp.Result.ServiceSnapshot)
	}
	if exp.Result.KafkaAvailable != nil || exp.Result.KafkaLag != nil || exp.Result.TraceSamples != nil {
		t.Fatalf("monolith must keep distributed fields N/A, got %+v", exp.Result)
	}
}
