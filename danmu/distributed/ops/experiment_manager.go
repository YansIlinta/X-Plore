package ops

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ExperimentObserver 是实验管理器对 Ops 旁路观测面的只读依赖。*Collector 天然满足；
// 测试注入替换品即可。实验层绝不触碰消息主链，只读旁路观测快照。
type ExperimentObserver interface {
	Snapshot() Snapshot
	AddEvent(level, kind, message string)
	Traces(n int) []Trace
}

// CreateRequest 是 POST /api/experiments 的入参。
// Workload 缺省/全零时：有 preset 用 preset，否则用 custom 默认值；
// 前端总是把 preset 预填后的完整 workload 发来，因此该契约是确定性的。
type CreateRequest struct {
	Name         string         `json:"name"`
	Architecture string         `json:"architecture"`
	Preset       string         `json:"preset"`
	Workload     WorkloadConfig `json:"workload"`
}

// ExperimentManager 是 "Realtime Systems Lab" 的编排层：
//
//	Create / Start / Stop / finalize（onDone 回调）→ 持久化。
//
// 单例保证：全局同时只有一个实验可以 running（底层 loadtest 子进程是单例），
// 冲突 start 返回 409 语义错误。它只执行 loadtest（明确实现的 runner），
// 绝不接受任意 shell command。
type ExperimentManager struct {
	store  *experimentStore
	runner *loadtestManager
	repo   string             // 采集 git 元数据的仓库目录；空=跳过 git
	obs    ExperimentObserver // 可为 nil；nil 时分布式附加观测跳过
	token  string

	mu      sync.Mutex
	active  string // 当前绑定到 runner 的实验 id；"" = 无
	outcome string // 本次运行的预期收尾（completed | failed | stopped）

	evidence *EvidenceService
}

// NewExperimentManager 构造实验管理器。obs 可为 nil（无旁路观测）。
func NewExperimentManager(store *experimentStore, runner *loadtestManager, repoDir string, obs ExperimentObserver) *ExperimentManager {
	m := &ExperimentManager{store: store, runner: runner, repo: repoDir, obs: obs, evidence: NewEvidenceService(store, repoDir)}
	if runner != nil {
		runner.onDone = m.finishRun
	}
	return m
}

// TenInt 生成实验 ID：exp-<unixsec>-<4字节hex>。只含 [a-z0-9-]，防目录穿越。
func newExperimentID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 退化：用微秒做后缀（仍满足 ID 字符集与唯一性要求）
		return fmt.Sprintf("exp-%d-%d", time.Now().Unix(), time.Now().UnixNano()%1e7)
	}
	return fmt.Sprintf("exp-%d-%s", time.Now().Unix(), hex.EncodeToString(b[:]))
}

// Create 校验并创建（status=created），立即持久化。
func (m *ExperimentManager) Create(req CreateRequest) (*Experiment, error) {
	preset := req.Preset
	if preset == "" {
		preset = "custom"
	}
	p, err := PresetByName(preset)
	if err != nil {
		return nil, err
	}

	wl := p.Workload
	if !workloadZero(req.Workload) {
		wl = req.Workload
	}
	// 先严格校验用户的显式输入（conns=0 之类要明确报错，不能先被夹取掩盖掉）。
	if wl.Duration == "" {
		wl.Duration = "30s"
	}
	if wl.Target == "" {
		wl.Target = p.Workload.Target
	}
	if err := wl.Validate(); err != nil {
		return nil, err
	}
	// 再做防御性夹取，保住 loadtest 的安全区间（正常输入不会触发）。
	wl.Connections = clampInt(wl.Connections, 1, 100000)
	wl.Rooms = clampInt(wl.Rooms, 1, 10000)
	wl.MessageRate = clampFloat(wl.MessageRate, 0, 10000)

	arch := req.Architecture
	if arch == "" {
		arch = p.Architecture
	}
	if err := ValidateArchitecture(arch); err != nil {
		return nil, err
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = fmt.Sprintf("%s %s", p.Label, time.Now().Format("2006-01-02 15:04:05"))
	}

	exp := &Experiment{
		ID:           newExperimentID(),
		Name:         name,
		Architecture: arch,
		Preset:       preset,
		Status:       ExpStatusCreated,
		Workload:     wl,
		CreatedAt:    time.Now().UTC(),
	}
	if err := m.store.Save(exp); err != nil {
		return nil, err
	}
	m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s created (preset=%s arch=%s workload=%+v)", exp.ID, preset, arch, wl))
	return exp, nil
}

// workloadZero 判断 create 请求是否没带有效 workload（走 preset/默认）。
func workloadZero(w WorkloadConfig) bool {
	return w.Connections == 0 && w.Rooms == 0 && w.MessageRate == 0 &&
		w.Duration == "" && w.Target == ""
}

// Get 读取单个实验。
func (m *ExperimentManager) Get(id string) (*Experiment, error) {
	if err := ValidateExperimentID(id); err != nil {
		return nil, err
	}
	return m.store.Load(id)
}

// List 列出最近的历史实验（含 running）。
func (m *ExperimentManager) List() ([]*Experiment, error) {
	return m.store.List()
}

// ActiveID 返回当前正在运行的实验 id（无则空串）。只读。
func (m *ExperimentManager) ActiveID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// Running 判断指定实验当前是否正在运行（用于前端展示/轮询）。
func (m *ExperimentManager) Running(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active == id
}

// Live 返回正在运行实验的实时快照（latest / elapsed）。未运行返回 nil。
func (m *ExperimentManager) Live() map[string]any {
	st := m.runner.Status()
	running, _ := st["running"].(bool)
	if !running {
		return nil
	}
	latest, _ := st["latest"].(map[string]any)
	return map[string]any{
		"running":    true,
		"params":     st["params"],
		"started_at": st["started_at"],
		"elapsed_s":  st["elapsed_s"],
		"latest":     latest,
	}
}

// Start 启动实验：校验状态机 → 独占 runner → 登记 active → 采集环境 → 同步持久化状态。
func (m *ExperimentManager) Start(id string) error {
	exp, err := m.store.Load(id)
	if err != nil {
		return err
	}
	if err := exp.CanStart(); err != nil {
		return err
	}

	m.mu.Lock()
	if m.active != "" {
		m.mu.Unlock()
		return fmt.Errorf("cannot start experiment %s: experiment %s is already running; stop it first", id, m.active)
	}
	// 双保险：runner 级单例（loadtestManager.Start 内部仍会再查一次）
	m.mu.Unlock()

	now := time.Now().UTC()
	env := captureEnvironment(m.repo)
	exp.Status = ExpStatusRunning
	exp.StartedAt = &now
	exp.FinishedAt = nil
	exp.Result = ExperimentResult{}
	exp.Error = ""
	exp.Environment = env

	m.mu.Lock()
	m.active = id
	m.outcome = ExpStatusCompleted
	m.mu.Unlock()
	// 先持久化 running 状态；若 Save 失败仍继续尝试启动（启动本身不依赖磁盘成功）
	if err := m.store.Save(exp); err != nil {
		log.Printf("[ops] experiment %s: persist running state failed: %v", id, err)
	}

	if err := m.runner.Start(workloadToParams(exp.Workload)); err != nil {
		m.mu.Lock()
		m.active = ""
		m.outcome = ""
		m.mu.Unlock()
		exp.Status = ExpStatusFailed
		exp.Error = "start: " + err.Error()
		if serr := m.store.Save(exp); serr != nil {
			log.Printf("[ops] experiment %s: persist failed state failed: %v", id, serr)
		}
		m.audit(eventError, "experiment", fmt.Sprintf("experiment %s start failed: %v", id, err))
		return err
	}

	m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s started (workload=%+v)", id, exp.Workload))
	return nil
}

// Stop 终止正在运行的实验（用户主动停）。状态由 finishRun 依据 outcome 落为 stopped。
func (m *ExperimentManager) Stop(id string) error {
	m.mu.Lock()
	if m.active != id {
		active := m.active
		m.mu.Unlock()
		if active == "" {
			return fmt.Errorf("no experiment is running")
		}
		return fmt.Errorf("experiment %s is not the running experiment (%s is)", id, active)
	}
	m.outcome = ExpStatusStopped
	m.mu.Unlock()

	m.runner.Stop()
	m.audit(eventWarning, "experiment", fmt.Sprintf("experiment %s stop requested", id))
	return nil
}

// finishRun 是由 loadtestManager 在子进程结束后（锁外）回调的收尾：
// 把报告映射进 Result、采集分布式附加观测、落状态、持久化、释放 active。
func (m *ExperimentManager) finishRun(report map[string]any, runErr string) {
	m.mu.Lock()
	id := m.active
	outcome := m.outcome
	if id == "" {
		m.mu.Unlock()
		return
	}
	m.active = ""
	m.outcome = ""
	m.mu.Unlock()

	exp, err := m.store.Load(id)
	if err != nil {
		log.Printf("[ops] experiment %s finish: reload failed: %v", id, err)
		return
	}

	now := time.Now().UTC()
	exp.FinishedAt = &now
	status := outcome
	if status != ExpStatusStopped && runErr != "" {
		status = ExpStatusFailed
		exp.Error = runErr
	}
	exp.Status = status

	if status == ExpStatusCompleted {
		res, notes := resultFromLoadtestReport(report, exp.Workload, exp)
		if report == nil {
			notes = append(notes, "loadtest did not produce a report; metrics unavailable")
		}
		exp.Result = res
		exp.Result.Notes = notes
		if exp.Architecture == ArchDistributed {
			distReachable, snapshot := m.captureDistributed(exp)
			exp.Result.ServiceSnapshot = snapshot
			if distReachable {
				appendDistributedTraces(&exp.Result, m.allTraces())
			}
		}
	} else if status == ExpStatusFailed {
		exp.Result = ExperimentResult{Notes: []string{"experiment did not complete: " + exp.Error}}
	} else { // stopped
		exp.Result = ExperimentResult{Notes: []string{"experiment stopped by user before completion"}}
	}

	if err := m.store.Save(exp); err != nil {
		log.Printf("[ops] experiment %s finish: persist failed: %v", id, err)
	}
	m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s finished with status %s", id, status))
}

// captureDistributed 抓取一次分布式实验结束时的服务面快照。
// 返回 (是否有可观测分布式服务, 快照)；无服务时快照其余字段为空、由调用方置 N/A。
func (m *ExperimentManager) captureDistributed(exp *Experiment) (bool, *DistributedSnapshot) {
	if m.obs == nil {
		return false, &DistributedSnapshot{FreeText: "no ops observer; distributed snapshot unavailable"}
	}
	s := m.obs.Snapshot()
	d := &DistributedSnapshot{EtcdUp: s.EtcdUp, Health: s.Health}
	reachable := false
	for _, svc := range s.Services {
		switch svc.Name {
		case "comet":
			d.CometTotal += len(svc.Instances)
			for _, it := range svc.Instances {
				if it.Healthy {
					d.CometHealthy++
				}
			}
			reachable = true
		case "logic":
			d.LogicTotal += len(svc.Instances)
			reachable = true
		case "job":
			d.JobTotal += len(svc.Instances)
			reachable = true
		}
	}
	if !reachable {
		d.FreeText = "no distributed services observed by this ops backend"
	}
	// Kafka lag
	if s.Kafka.Available {
		exp.Result.KafkaAvailable = newbool(true)
		total := int64(0)
		seen := 0
		for _, v := range s.Kafka.Lag {
			if v != nil {
				total += *v
				seen++
			}
		}
		if seen > 0 {
			exp.Result.KafkaLag = newi64(total)
		}
	} else if s.Kafka.Err != "" {
		exp.Result.Notes = append(exp.Result.Notes, "kafka: "+s.Kafka.Err)
	}
	exp.Result.EtcdUp = newbool(s.EtcdUp)
	return reachable, d
}

func (m *ExperimentManager) allTraces() []Trace {
	if m.obs == nil {
		return nil
	}
	return m.obs.Traces(traceMaxKept)
}

// appendDistributedTraces 把旁路汇聚的 trace 作为代表样本存入实验（有界，最多 5 条）。
func appendDistributedTraces(res *ExperimentResult, traces []Trace) {
	if len(traces) == 0 {
		res.TraceSamples = newi64(0)
		return
	}
	samples := len(traces)
	complete := 0
	for _, t := range traces {
		if t.Complete {
			complete++
		}
	}
	res.TraceSamples = newi64(int64(samples))
	if samples > 0 {
		rate := float64(complete) / float64(samples)
		res.TraceCompletion = newf64(rate)
	}
	// 代表样本：只取最新的少量完整链路为 owned 快照。
	var reps []Trace
	for i := len(traces) - 1; i >= 0 && len(reps) < 5; i-- {
		if traces[i].Complete {
			c := traces[i]
			reps = append(reps, c)
		}
	}
	if len(reps) == 0 && len(traces) > 0 {
		reps = []Trace{traces[len(traces)-1]}
	}
	res.RepresentativeTraces = reps
}

func (m *ExperimentManager) audit(level, kind, message string) {
	if m.obs != nil {
		m.obs.AddEvent(level, kind, message)
	}
}

// ---- 双向映射：workload ↔ loadtest 参数 ----

// workloadToParams 把实验 workload 映射为 loadtestManager.Start 接受的参数。
// 全部字段都落在 loadtest 已支持的 flag 空间内。
func workloadToParams(w WorkloadConfig) map[string]any {
	return map[string]any{
		"server":   w.Target,
		"conns":    w.Connections,
		"rooms":    w.Rooms,
		"rate":     w.MessageRate,
		"duration": w.Duration,
	}
}

// resultFromLoadtestReport 把 loadtest --output-json 报告映射为 ExperimentResult。
// 严格区分"测得 0"与"没测（nil→N/A）"：
//   - connections/latency/totals 来自 summary（真实测得）
//   - write/read errors 由每秒快照的累计计数聚合；无快照 → nil（N/A）
//   - drops 当前 loadtest 不测量 → 恒 nil + note
func resultFromLoadtestReport(report map[string]any, _ WorkloadConfig, exp *Experiment) (ExperimentResult, []string) {
	var r ExperimentResult
	var notes []string
	summary, _ := report["summary"].(map[string]any)

	getI := func(k string) *int64 {
		if summary == nil {
			return nil
		}
		v, ok := summary[k]
		if !ok {
			return nil
		}
		f, ok := toFloat64(v)
		if !ok {
			return nil
		}
		return newi64(int64(f))
	}

	r.ConnectionsRequested = getI("target_conns")
	r.ConnectionsEstablished = getI("success_conns")
	r.ConnectionsFailed = getI("failed_conns")
	r.MessagesSent = getI("total_sent")
	r.MessagesReceived = getI("total_recv")
	r.P50LatencyUS = getI("e2e_p50_us")
	r.P90LatencyUS = getI("e2e_p90_us")
	r.P99LatencyUS = getI("e2e_p99_us")
	r.MaxLatencyUS = getI("e2e_max_us")

	we, re := reportSnapshotErrorTotals(report)
	if we != nil {
		r.WriteErrors = we
	} else {
		notes = append(notes, "write_errors: no per-second snapshots captured; N/A")
	}
	if re != nil {
		r.ReadErrors = re
	} else {
		notes = append(notes, "read_errors: no per-second snapshots captured; N/A")
	}
	// drops：loadtest 不测量投递丢失（dropCount 从未递增），诚实 N/A，不做估算。
	notes = append(notes, "drops: not measured by loadtest (no per-message delivery-loss counter); N/A")

	// 速率：真实耗时 + 真实总计；起点/终点缺失或时间为零 → N/A。
	if r.MessagesSent != nil && exp.StartedAt != nil && exp.FinishedAt != nil {
		el := exp.FinishedAt.Sub(*exp.StartedAt).Seconds()
		if el > 0 {
			r.SendRate = newf64(float64(*r.MessagesSent) / el)
		}
	}
	if r.MessagesReceived != nil && exp.StartedAt != nil && exp.FinishedAt != nil {
		el := exp.FinishedAt.Sub(*exp.StartedAt).Seconds()
		if el > 0 {
			r.ReceiveRate = newf64(float64(*r.MessagesReceived) / el)
		}
	}
	return r, notes
}

// reportSnapshotErrorTotals 从 loadtest 报告的 snapshots[] 取最后的累计错误计数。
// 快照是每秒打点的累计值（loadtest Snapshot.WriteErrors/ReadErrors 字段），
// 取最后一个即最终总计；无快照返回 (nil, nil) 表示无法测量。
func reportSnapshotErrorTotals(report map[string]any) (*int64, *int64) {
	arr, ok := report["snapshots"].([]any)
	if !ok || len(arr) == 0 {
		return nil, nil
	}
	last, ok := arr[len(arr)-1].(map[string]any)
	if !ok {
		return nil, nil
	}
	we, ok1 := last["WriteErrors"]
	re, ok2 := last["ReadErrors"]
	var w, r *int64
	if ok1 {
		if f, ok := toFloat64(we); ok {
			w = newi64(int64(f))
		}
	}
	if ok2 {
		if f, ok := toFloat64(re); ok {
			r = newi64(int64(f))
		}
	}
	return w, r
}

// toFloat64 兼容 json 解码后的数字形态。
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// ---- API 面：对比 / 报告 / 证据 / 预设 / 旧 loadtest 兼容 ----

// Compare 对比两个历史实验（任意架构；行级 N/A 规则见 compare.go）。
// 只允许对比已完结（completed）的实验——拿未完结的实验比会得到误导性的
// "无差异"（因为它的指标全为 N/A）。
func (m *ExperimentManager) Compare(leftID, rightID string) (*CompareResult, error) {
	left, err := m.Get(leftID)
	if err != nil {
		return nil, fmt.Errorf("left experiment: %w", err)
	}
	right, err := m.Get(rightID)
	if err != nil {
		return nil, fmt.Errorf("right experiment: %w", err)
	}
	for _, x := range []struct {
		ref string
		e   *Experiment
	}{
		{"left", left}, {"right", right},
	} {
		if x.e.Status != ExpStatusCompleted {
			return nil, fmt.Errorf("compare requires completed experiments; %s experiment %s is %s",
				x.ref, x.e.ID, x.e.Status)
		}
	}
	return CompareExperiments(left, right), nil
}

// Report 生成单个实验的报告视图：完整记录 + 支撑该实验的 claim 列表 + 可复现说明。
func (m *ExperimentManager) Report(id string) (map[string]any, error) {
	exp, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	claims := m.Evidence()
	var linked []Claim
	for _, c := range claims {
		if c.ExperimentID != nil && *c.ExperimentID == id {
			linked = append(linked, c)
		}
	}
	return map[string]any{
		"experiment": exp,
		"claims":     linked,
	}, nil
}

// Evidence 返回全部 claim 的当前状态（见 evidence.go）。
func (m *ExperimentManager) Evidence() []Claim {
	if m.evidence == nil {
		return nil
	}
	return m.evidence.List()
}

// Presets 返回 preset 模板（含默认 workload）。
func (m *ExperimentManager) Presets() []Preset {
	out := make([]Preset, len(ExperimentPresets))
	copy(out, ExperimentPresets)
	return out
}

// LegacyStatus 兼容旧的 /api/loadtest/status 响应形态（新前端已不用，保留 API 契约）。
func (m *ExperimentManager) LegacyStatus() map[string]any {
	st := m.runner.Status()
	id := m.ActiveID()
	var active any
	if id != "" {
		if exp, err := m.Get(id); err == nil {
			active = map[string]any{
				"id": exp.ID, "name": exp.Name, "architecture": exp.Architecture,
				"preset": exp.Preset, "status": exp.Status,
			}
		}
	}
	st["active_experiment"] = active
	return st
}

// LegacyStart 兼容旧的 POST /api/loadtest/start：把旧参数形态映射为一个 custom 实验并启动。
func (m *ExperimentManager) LegacyStart(params map[string]any) error {
	server, _ := params["server"].(string)
	if server == "" {
		server = "ws://localhost:8080"
	}
	duration, _ := params["duration"].(string)
	if duration == "" {
		duration = "30s"
	}
	wl := WorkloadConfig{
		Connections: numOr(params["conns"], 1000),
		Rooms:       numOr(params["rooms"], 10),
		MessageRate: floatOr(params["rate"], 1),
		Duration:    duration,
		Target:      server,
	}
	exp, err := m.Create(CreateRequest{
		Name:         "adhoc " + time.Now().Format("2006-01-02 15:04:05"),
		Preset:       "custom",
		Architecture: ArchMonolith,
		Workload:     wl,
	})
	if err != nil {
		return err
	}
	return m.Start(exp.ID)
}

// LegacyStop 兼容旧 POST /api/loadtest/stop：停止当前运行实验（无则空转成功）。
func (m *ExperimentManager) LegacyStop() error {
	id := m.ActiveID()
	if id == "" {
		return nil
	}
	return m.Stop(id)
}
