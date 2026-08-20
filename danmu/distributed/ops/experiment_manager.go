package ops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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
// 向后兼容：老客户端（name/architecture/preset/workload）行为不变；
// Phase 1.5 新增 regime / warmup / duration / repetitions / system_config / delivery_check。
type CreateRequest struct {
	Name         string         `json:"name"`
	Architecture string         `json:"architecture"`
	Preset       string         `json:"preset"`
	Workload     WorkloadConfig `json:"workload"`

	Regime      string       `json:"regime,omitempty"`
	Warmup      string       `json:"warmup,omitempty"` // 例 "3s"
	Duration    string       `json:"duration,omitempty"`
	Repetitions int          `json:"repetitions,omitempty"`
	System      SystemConfig `json:"system_config,omitempty"`
	ConfigLabel string       `json:"config_label,omitempty"`
	SweepID     string       `json:"sweep_id,omitempty"`

	// DeliveryCheck 默认 true：实验压测总是开启 loadtest -delivery-check（序列缺口投递核算）。
	DeliveryCheck *bool `json:"delivery_check,omitempty"`
}

// NewExperiment 是实验管理器在 Phase 1.5 对"重复可执行实验"的编排层：
//
//	Create → Start（顺序执行全部 repetition）→ 每个 run 收尾落 Runs → 计算 Aggregate
//
// 单例保证：全局同时只有一个 run 在跑（底层 loadtest 子进程是单例），
// 冲突 start 返回 409 语义错误。它只执行 loadtest（明确实现的 runner），
// 绝不接受任意 shell command。
type ExperimentManager struct {
	store  *experimentStore
	runner *loadtestManager
	repo   string             // 采集 git 元数据的仓库目录；空=跳过 git
	obs    ExperimentObserver // 可为 nil；nil 时分布式附加观测跳过
	token  string
	server *ServerProcessManager // 可为 nil；非 nil 时 sweep 可拉起受控 monolith 进程

	mu        sync.Mutex
	active    string // 当前绑定到 runner 的 experiment id；"" = 无
	activeRun int    // 当前 run index（1-based）；0 = 无
	outcome   string // 本次 run 的预期收尾（RunStatusCompleted | RunStatusStopped）

	resSampler *ResourceSampler // 当前 run 的 server 资源采样器；run 结束即停止

	evidence *EvidenceService
}

// NewExperimentManager 构造实验管理器。obs 可为 nil（无旁路观测）；
// server 可为 nil（不拉起任何被控 server 进程，system-config sweep 不可用）。
func NewExperimentManager(store *experimentStore, runner *loadtestManager, repoDir string, obs ExperimentObserver) *ExperimentManager {
	return NewExperimentManagerFull(store, runner, repoDir, obs, nil, "")
}

// NewExperimentManagerFull 是完整构造（含可选的被控 server 进程管理器与观测 token）。
// token 用于向目标 server 的 /api/v1/stats 做资源采样鉴权；空串则不带头。
func NewExperimentManagerFull(store *experimentStore, runner *loadtestManager, repoDir string, obs ExperimentObserver, server *ServerProcessManager, token string) *ExperimentManager {
	m := &ExperimentManager{store: store, runner: runner, repo: repoDir, obs: obs, server: server, token: token, evidence: NewEvidenceService(store, repoDir)}
	if runner != nil {
		runner.onDone = m.finishRun
	}
	return m
}

// TenInt 生成实验 ID：exp-<unixsec>-<4字节hex>。只含 [a-z0-9-]，防目录穿越。
func newExperimentID() string {
	return "exp-" + newExperimentIDSuffix()
}

// newExperimentIDSuffix 生成 "<unixsec>-<hex>" 后缀；rand 失败时退化为微秒。
func newExperimentIDSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d-%d", time.Now().Unix(), time.Now().UnixNano()%1e7)
	}
	return fmt.Sprintf("%d-%s", time.Now().Unix(), hex.EncodeToString(b[:]))
}

// resolveSpecAndMeta 把 CreateRequest 归一化为 (spec, name)。确定性：
// explicit workload（非全零） > regime 默认 > preset 默认 > custom 默认。
func (m *ExperimentManager) resolveSpecAndMeta(req CreateRequest) (ExperimentSpec, string, error) {
	arch := strings.TrimSpace(req.Architecture)
	if arch == "" {
		arch = ArchMonolith
	}
	if err := ValidateArchitecture(arch); err != nil {
		return ExperimentSpec{}, "", err
	}

	// 1) 基 workload：regime > preset > custom。
	var wl WorkloadConfig
	if req.Regime != "" {
		if !KnownRegime(req.Regime) {
			return ExperimentSpec{}, "", fmt.Errorf("unknown workload regime %q", req.Regime)
		}
		for _, r := range Regimes("") {
			if r.Name == req.Regime {
				wl = r.Workload
				if arch == "" {
					arch = ArchMonolith
				}
				break
			}
		}
	} else if req.Preset != "" && req.Preset != "custom" {
		p, err := PresetByName(req.Preset)
		if err != nil {
			return ExperimentSpec{}, "", err
		}
		wl = p.Workload
		if arch == "" {
			arch = p.Architecture
		}
	} else {
		wl = WorkloadConfig{Connections: 100, Rooms: 10, MessageRate: 1, Duration: "30s", Target: "ws://localhost:8081", Distribution: DistUniform, Seed: 1}
	}
	// 2) explicit workload 覆盖（非全零）。
	if !workloadZero(req.Workload) {
		wl = req.Workload
	}
	// 3) 归一化。
	if wl.Duration == "" {
		wl.Duration = "30s"
	}
	if wl.Target == "" {
		wl.Target = "ws://localhost:8081"
	}
	if wl.Distribution == "" {
		wl.Distribution = DistUniform
	}
	if wl.Seed == 0 {
		wl.Seed = 1
	}
	if err := wl.Validate(); err != nil {
		return ExperimentSpec{}, "", err
	}
	// 安全夹取（防御性，正常输入不触发）。
	wl.Connections = clampInt(wl.Connections, 1, 100000)
	wl.Rooms = clampInt(wl.Rooms, 1, 10000)
	wl.MessageRate = clampFloat(wl.MessageRate, 0, 10000)

	// 4) 测量窗 duration：explicit req.Duration > workload.Duration。
	duration := wl.Duration
	if req.Duration != "" {
		duration = req.Duration
	}
	if _, err := time.ParseDuration(duration); err != nil {
		return ExperimentSpec{}, "", fmt.Errorf("duration %q is not a valid Go duration: %v", duration, err)
	}
	wl.Duration = duration // legacy 字段与测量窗一致

	warmup := req.Warmup
	if warmup != "" {
		if _, err := time.ParseDuration(warmup); err != nil {
			return ExperimentSpec{}, "", fmt.Errorf("warmup %q is not a valid Go duration: %v", warmup, err)
		}
	}
	reps := DefaultExpRepetitions(req.Repetitions)

	if err := req.System.Validate(); err != nil {
		return ExperimentSpec{}, "", err
	}
	sys := req.System
	if !sys.Empty() {
		sys.RequiresRestart = true // 当前所有系统参数都是 startup config，需重启
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = defaultExpName(req.Regime, req.Preset, sys, time.Now())
	}

	spec := ExperimentSpec{
		Architecture: arch,
		Regime:       req.Regime,
		ConfigLabel:  req.ConfigLabel,
		Workload:     wl,
		System:       sys,
		Warmup:       warmup,
		Duration:     duration,
		Repetitions:  reps,
	}
	return spec, name, nil
}

// defaultExpName 生成实验默认名（确定性规则）。
func defaultExpName(regime, preset string, sys SystemConfig, now time.Time) string {
	if regime != "" {
		return fmt.Sprintf("%s %s", regime, now.Format("2006-01-02 15:04:05"))
	}
	if preset != "" {
		label := preset
		if p, err := PresetByName(preset); err == nil {
			label = p.Label
		}
		return fmt.Sprintf("%s %s", label, now.Format("2006-01-02 15:04:05"))
	}
	if !sys.Empty() {
		return fmt.Sprintf("config %s", sys.Label())
	}
	return fmt.Sprintf("custom %s", now.Format("2006-01-02 15:04:05"))
}

// SystemConfig.Label 是人类可读的配置标签。
func (s SystemConfig) Label() string {
	if s.BatchSize > 0 && s.BatchTimeout != "" && s.Workers > 0 {
		return fmt.Sprintf("bs=%d bt=%s w=%d", s.BatchSize, s.BatchTimeout, s.Workers)
	}
	parts := []string{}
	if s.BatchSize > 0 {
		parts = append(parts, "bs="+strconv.Itoa(s.BatchSize))
	}
	if s.BatchTimeout != "" {
		parts = append(parts, "bt="+s.BatchTimeout)
	}
	if s.Workers > 0 {
		parts = append(parts, "w="+strconv.Itoa(s.Workers))
	}
	if len(parts) == 0 {
		return "default"
	}
	return strings.Join(parts, " ")
}

// Create 校验并创建（status=created），立即持久化；spec 定格并计算 SpecHash。
func (m *ExperimentManager) Create(req CreateRequest) (*Experiment, error) {
	spec, name, err := m.resolveSpecAndMeta(req)
	if err != nil {
		return nil, err
	}
	hash, err := spec.SpecHash()
	if err != nil {
		return nil, err
	}

	exp := &Experiment{
		ID:            newExperimentID(),
		Name:          name,
		Architecture:  spec.Architecture,
		Preset:        req.Preset,
		Status:        ExpStatusCreated,
		Workload:      spec.Workload,
		CreatedAt:     time.Now().UTC(),
		SchemaVersion: SchemaVersion,
		Regime:        spec.Regime,
		ConfigLabel:   spec.ConfigLabel,
		Warmup:        spec.Warmup,
		Duration:      spec.Duration,
		Repetitions:   spec.Repetitions,
		Spec:          spec,
		SpecHash:      hash,
		SystemConfig:  spec.System,
		SweepID:       req.SweepID,
	}
	if err := m.store.Save(exp); err != nil {
		return nil, err
	}
	m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s created (regime=%s rep=%d hash=%s)", exp.ID, spec.Regime, spec.Repetitions, ShortHash(hash)))
	return exp, nil
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

// ActiveRun 返回当前正在跑的 repetition index（1-based）；无则 0。
func (m *ExperimentManager) ActiveRun() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeRun
}

// Running 判断指定实验当前是否正在运行（用于前端展示/轮询）。
func (m *ExperimentManager) Running(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active == id
}

// Live 返回正在运行实验的实时快照（latest / elapsed / rep）。未运行返回 nil。
func (m *ExperimentManager) Live() map[string]any {
	st := m.runner.Status()
	running, _ := st["running"].(bool)
	if !running {
		return nil
	}
	latest, _ := st["latest"].(map[string]any)
	m.mu.Lock()
	rep := m.activeRun
	m.mu.Unlock()
	return map[string]any{
		"running":     true,
		"params":      st["params"],
		"started_at":  st["started_at"],
		"elapsed_s":   st["elapsed_s"],
		"repetition":  rep,
		"repetitions": st["repetitions"],
		"latest":      latest,
	}
}

// planStartRuns 算出本次 Start 要执行的 repetition 序列：
//
//	无 runs 或实验已是 completed/created —— 全新全量执行（1..N，覆盖旧历史，兼容 legacy 重跑）
//	partial / failed / stopped —— 恢复：跳过已完成 run，重试未成功/未开始的 run
func planStartRuns(exp *Experiment) ([]int, bool /*fresh*/) {
	reps := DefaultExpRepetitions(exp.Repetitions)
	if len(exp.Runs) == 0 || exp.Status == ExpStatusCreated || exp.Status == ExpStatusCompleted {
		all := make([]int, reps)
		for i := 1; i <= reps; i++ {
			all[i-1] = i
		}
		return all, true
	}
	// 恢复模式：completed 跳过，其余重跑。
	byIndex := map[int]*ExperimentRun{}
	for _, r := range exp.Runs {
		byIndex[r.Index] = r
	}
	var todo []int
	for i := 1; i <= reps; i++ {
		if r, ok := byIndex[i]; ok && r.Status == RunStatusCompleted {
			continue
		}
		todo = append(todo, i)
	}
	if len(todo) == 0 {
		// 全完成但 status 不是 completed（理论不出现）→ 全量重跑
		all := make([]int, reps)
		for i := 1; i <= reps; i++ {
			all[i-1] = i
		}
		return all, true
	}
	return todo, false
}

// Start 启动/恢复实验：状态机校验 → 独占 runner → 逐 run 顺序执行。
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
	m.mu.Unlock()

	todo, fresh := planStartRuns(exp)
	if fresh {
		// 全新执行：清空旧 run 历史（覆盖语义，与 legacy 一致）。
		exp.Runs = nil
		exp.Aggregate = nil
		exp.Error = ""
	}
	// 首个 run 的环境快照。
	if exp.StartedAt == nil {
		now := time.Now().UTC()
		exp.StartedAt = &now
		exp.Environment = captureEnvironment(m.repo)
	}
	if err := m.store.Save(exp); err != nil {
		log.Printf("[ops] experiment %s: persist pre-run state failed: %v", id, err)
	}

	// 系统配置需要被控 server 进程时，先拉起（首 rep 前一次性）。
	if !exp.SystemConfig.Empty() {
		if m.server == nil {
			return fmt.Errorf("experiment %s requests system_config (%s) but no controlled server process manager is configured", id, exp.SystemConfig.Label())
		}
		if err := m.server.Ensure(exp.Workload.Target, exp.SystemConfig); err != nil {
			exp.Status = ExpStatusFailed
			exp.Error = "server ensure: " + err.Error()
			_ = m.store.Save(exp)
			return err
		}
	}

	return m.beginRun(id, todo[0])
}

// Stop 终止正在运行的实验（用户主动停）。当前 run 记为 stopped，剩余不继续。
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
	m.outcome = RunStatusStopped
	m.mu.Unlock()

	m.runner.Stop()
	m.audit(eventWarning, "experiment", fmt.Sprintf("experiment %s stop requested", id))
	return nil
}

// beginRun 发起一个 repetition 的 loadtest 子进程。调用方需保证 runner 空闲。
func (m *ExperimentManager) beginRun(id string, runIdx int) error {
	exp, err := m.store.Load(id)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	run := m.findOrCreateRun(exp, runIdx)
	run.StartedAt = &now
	run.FinishedAt = nil
	run.Status = RunStatusRunning
	run.Error = ""
	run.Environment = captureEnvironment(m.repo)
	exp.Status = ExpStatusRunning
	exp.Error = ""
	if err := m.store.Save(exp); err != nil {
		log.Printf("[ops] experiment %s: persist run-running state failed: %v", id, err)
	}

	// 启动资源采样（server 目标）。
	sampler := StartResourceSampler(context.Background(), exp.Workload.Target, m.token, ResourceSampleInterval)

	m.mu.Lock()
	m.active = id
	m.activeRun = runIdx
	m.outcome = RunStatusCompleted
	m.resSampler = sampler
	m.mu.Unlock()

	if err := m.runner.Start(workloadToRunParams(exp.Spec)); err != nil {
		m.mu.Lock()
		m.active = ""
		m.activeRun = 0
		m.outcome = ""
		s := m.resSampler
		m.resSampler = nil
		m.mu.Unlock()
		if s != nil {
			s.Stop()
		}
		// runner.Start 失败：本次 run 记 failed，实验 finalize（若这是唯一 rep 则 failed）。
		sampler.Stop()
		run = m.findOrCreateRun(exp, runIdx)
		run.Status = RunStatusFailed
		run.Error = "start: " + err.Error()
		run.FinishedAt = ptrTime(time.Now().UTC())
		exp.Status = ExpStatusRunning // finalize 会重算
		_ = m.store.Save(exp)
		m.finalizeExperiment(id, exp)
		m.audit(eventError, "experiment", fmt.Sprintf("experiment %s rep %d start failed: %v", id, runIdx, err))
		return err
	}

	m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s rep %d/%d started", id, runIdx, exp.Repetitions))
	return nil
}

// finishRun 是 loadtestManager 在子进程结束后（锁外）回调的收尾：
// 记录当前 run → 若还有剩余 repetition 且非 stopped → 继续下一个；否则 finalize。
func (m *ExperimentManager) finishRun(report map[string]any, runErr string) {
	m.mu.Lock()
	expID := m.active
	runIdx := m.activeRun
	outcome := m.outcome
	sampler := m.resSampler
	if expID == "" {
		m.mu.Unlock()
		return
	}
	// 先释放单例，让下一个 rep 的 start 可复用 runner。
	m.active = ""
	m.activeRun = 0
	m.outcome = ""
	m.resSampler = nil
	m.mu.Unlock()

	exp, err := m.store.Load(expID)
	if err != nil {
		log.Printf("[ops] experiment %s finish: reload failed: %v", expID, err)
		if sampler != nil {
			sampler.Stop()
		}
		return
	}

	if sampler != nil {
		sampler.Stop()
	}

	run := m.findOrCreateRun(exp, runIdx)
	now := time.Now().UTC()
	run.FinishedAt = &now

	switch {
	case outcome == RunStatusStopped:
		run.Status = RunStatusStopped
		run.Result = ExperimentResult{Notes: []string{fmt.Sprintf("repetition %d stopped by user", runIdx)}}
	case runErr != "":
		run.Status = RunStatusFailed
		run.Error = runErr
		run.Result = ExperimentResult{Notes: []string{fmt.Sprintf("repetition %d did not complete: %s", runIdx, runErr)}}
	case report == nil:
		run.Status = RunStatusFailed
		run.Error = "loadtest did not produce a JSON report"
		run.Result = ExperimentResult{Notes: []string{"loadtest did not produce a report"}}
	default:
		run.Status = RunStatusCompleted
		res, notes := resultFromLoadtestReport(report, exp.Spec, run, exp)
		run.Result = res
		run.Result.Notes = notes
		if exp.Architecture == ArchDistributed {
			distReachable, snapshot := m.captureDistributed(exp, &run.Result)
			run.Result.ServiceSnapshot = snapshot
			if distReachable {
				appendDistributedTraces(&run.Result, m.allTraces())
			}
		}
	}

	if sampler != nil {
		run.Resource = sampler.Summary()
	}

	// 计算是否还有剩余 repetition；有则继续（除非用户 stop）。
	exp.Status = ExpStatusRunning

	remaining := m.remainingRuns(exp)
	if remaining && outcome != RunStatusStopped {
		if err := m.store.Save(exp); err != nil {
			log.Printf("[ops] experiment %s finish: persist rep state failed: %v", expID, err)
		}
		next := m.nextTodoIndex(exp)
		m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s rep %d finished (%s); next rep %d", expID, runIdx, run.Status, next))
		if err := m.beginRun(expID, next); err != nil {
			// beginRun 已把本次失败落盘并 finalize；这里仅记录。
			m.audit(eventError, "experiment", fmt.Sprintf("experiment %s next rep %d begin failed: %v", expID, next, err))
		}
		return
	}

	// 全部完成或 stopped → finalize。
	m.finalizeExperiment(expID, exp)
}

// remainingRuns 判断实验是否还有未开始的 repetition（自动续跑语义：
// 会话中途已失败的 rep 不自动重试——它会成为 PARTIAL 状态；用户下次显式 Start 才重试）。
func (m *ExperimentManager) remainingRuns(exp *Experiment) bool {
	return m.nextTodoIndex(exp) > 0
}

// nextTodoIndex 返回下一个"还没尝试过"的 repetition index（不在 Runs 里）。
// 会话自动续跑只推进到未开始的 rep；已失败/已停止的 rep 留给下次 Start 恢复语义重试。
func (m *ExperimentManager) nextTodoIndex(exp *Experiment) int {
	reps := DefaultExpRepetitions(exp.Repetitions)
	attempted := map[int]bool{}
	for _, r := range exp.Runs {
		attempted[r.Index] = true
	}
	for i := 1; i <= reps; i++ {
		if !attempted[i] {
			return i
		}
	}
	return 0
}

// finalizeExperiment 计算聚合、设置代表结果与最终状态、持久化。
func (m *ExperimentManager) finalizeExperiment(id string, exp *Experiment) {
	exp.Status = DeriveExperimentStatus(exp.Runs)
	now := time.Now().UTC()
	exp.FinishedAt = &now

	if !exp.SystemConfig.Empty() && m.server != nil {
		m.server.Release()
	}

	agg := BuildExperimentAggregate(exp, 42)
	if agg != nil {
		exp.Aggregate = agg
		res := RepresentativeResult(agg)
		res.Notes = []string{
			"result shown is the experiment aggregate representative: latency/scale = median, throughput/rate = mean, errors = max",
		}
		// 携带最近一个成功 run 的分布式旁路观测（run 级数据；供实验视图/对比使用）。
		carryDistributedFromLastSuccessful(&res, successfulRuns(exp))
		exp.Result = res
	} else {
		exp.Result = ExperimentResult{Notes: []string{"no successful repetitions; no statistical aggregate available"}}
	}

	if exp.Status == ExpStatusCompleted {
		m.audit(eventInfo, "experiment", fmt.Sprintf("experiment %s completed (%d/%d reps success)", id, RunSuccessCount(exp.Runs), DefaultExpRepetitions(exp.Repetitions)))
	}
	// 顶层 Error 简述最近一次失败（仅当实验未完全成功时）。
	if exp.Status != ExpStatusCompleted {
		var lastErr string
		for _, r := range exp.Runs {
			if r.Status == RunStatusFailed && r.Error != "" {
				lastErr = r.Error
			}
		}
		if lastErr != "" {
			exp.Error = lastErr
		}
	}
	if err := m.store.Save(exp); err != nil {
		log.Printf("[ops] experiment %s finish: persist finalized state failed: %v", id, err)
	}
}

// carryDistributedFromLastSuccessful 把最近一个成功 run 的分布式旁路观测带到代表结果，
// 以便实验/对比/证据视图无需回查单个 run。monolith（无分布式字段）时保持 N/A。
func carryDistributedFromLastSuccessful(res *ExperimentResult, runs []*ExperimentRun) {
	if len(runs) == 0 {
		return
	}
	last := runs[len(runs)-1]
	r := last.Result
	res.KafkaAvailable = r.KafkaAvailable
	res.KafkaLag = r.KafkaLag
	res.EtcdUp = r.EtcdUp
	res.TraceSamples = r.TraceSamples
	res.TraceCompletion = r.TraceCompletion
	res.ServiceSnapshot = r.ServiceSnapshot
	if len(r.RepresentativeTraces) > 0 {
		res.RepresentativeTraces = r.RepresentativeTraces
	}
}

// findOrCreateRun 按 index 取 run；不存在则创建（原地追加到 exp.Runs）。
func (m *ExperimentManager) findOrCreateRun(exp *Experiment, idx int) *ExperimentRun {
	for _, r := range exp.Runs {
		if r.Index == idx {
			return r
		}
	}
	run := &ExperimentRun{ID: NewRunID(), Index: idx, Status: RunStatusRunning}
	exp.Runs = append(exp.Runs, run)
	return run
}

// workloadZero 判断 create 请求是否没带有效 workload（走 preset/regime 默认）。
func workloadZero(w WorkloadConfig) bool {
	return w.Connections == 0 && w.Rooms == 0 && w.MessageRate == 0 &&
		w.Duration == "" && w.Target == ""
}

// captureDistributed 抓取一次分布式实验结束时的服务面快照。
// 返回 (是否有可观测分布式服务, 快照)；无服务时快照其余字段为空、由调用方置 N/A。
func (m *ExperimentManager) captureDistributed(exp *Experiment, res *ExperimentResult) (bool, *DistributedSnapshot) {
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
		res.KafkaAvailable = newbool(true)
		total := int64(0)
		seen := 0
		for _, v := range s.Kafka.Lag {
			if v != nil {
				total += *v
				seen++
			}
		}
		if seen > 0 {
			res.KafkaLag = newi64(total)
		}
	} else if s.Kafka.Err != "" {
		res.Notes = append(res.Notes, "kafka: "+s.Kafka.Err)
	}
	res.EtcdUp = newbool(s.EtcdUp)
	return reachable, d
}

func (m *ExperimentManager) allTraces() []Trace {
	if m.obs == nil {
		return nil
	}
	return m.obs.Traces(traceMaxKept)
}

// appendDistributedTraces 把旁路汇聚的 trace 作为代表样本存入结果（有界，最多 5 条）。
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

// ---- 双向映射：ExperimentSpec ↔ loadtest 参数 ----

// workloadToRunParams 把实验 spec 映射为 loadtestManager.Start 接受的参数。
// 全部字段都落在 loadtest 已支持的 flag 空间内（含 Phase 1.5 新增 warmup/dist/seed/delivery-check）。
func workloadToRunParams(spec ExperimentSpec) map[string]any {
	w := spec.Workload
	ramp := "2s"
	if wu := spec.WarmupParsed(); wu > 0 {
		if wu < 5*time.Second {
			ramp = spec.Warmup
		} else {
			ramp = "5s"
		}
	}
	return map[string]any{
		"server":         w.Target,
		"conns":          w.Connections,
		"rooms":          w.Rooms,
		"rate":           w.MessageRate,
		"duration":       spec.Duration,
		"warmup":         spec.Warmup,
		"ramp":           ramp,
		"dist":           w.DistributionKind(),
		"zipf_s":         w.ZipfS,
		"seed":           w.Seed,
		"delivery_check": true,
	}
}

// resultFromLoadtestReport 把 loadtest --output-json 报告映射为 ExperimentResult（run 级）。
// 严格区分"测得 0"与"没测（nil→N/A）"：
//   - connections/latency/totals 来自 summary（真实测得）
//   - write/read errors 由每秒快照的累计计数聚合；无快照 → nil（N/A）
//   - drops：-delivery-check 开启且测到缺口时 = MissingDeliveries；否则 N/A + note
func resultFromLoadtestReport(report map[string]any, spec ExperimentSpec, run *ExperimentRun, exp *Experiment) (ExperimentResult, []string) {
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

	// 投递核算（-delivery-check）：真实缺口来自 loadtest 的 seq 连续性跟踪。
	if d, ok := report["delivery"].(map[string]any); ok {
		if obs, ok := toFloat64(d["observed_deliveries"]); ok {
			r.ExpectedDeliveries = newi64(int64(obs))
		}
		if miss, ok := toFloat64(d["missing_deliveries"]); ok {
			r.MissingDeliveries = newi64(int64(miss))
			// Drops = 真实的投递缺口（缺失的按连接投递次数）。
			r.Drops = r.MissingDeliveries
		}
		if rate, ok := toFloat64(d["delivery_rate"]); ok && d["delivery_rate"] != nil {
			r.DeliveryRate = newf64(rate)
		}
		if r.Drops == nil && r.MissingDeliveries == nil {
			notes = append(notes, "drops: delivery-check did not produce sequence-gap data; N/A")
		}
	} else {
		notes = append(notes, "drops: not measured by loadtest in this mode (no per-message delivery-loss counter); N/A")
	}

	// 测量窗口：Warmup / Measurement 分离。
	if ms, ok := report["measurement"].(map[string]any); ok {
		if s, ok := ms["start"].(string); ok {
			if t, err := parseRFC3339(s); err == nil {
				run.MeasurementStart = &t
			}
		}
		if e, ok := ms["end"].(string); ok {
			if t, err := parseRFC3339(e); err == nil {
				run.MeasurementEnd = &t
			}
		}
		if w, ok := ms["warmup"].(string); ok {
			run.WarmupDuration = w
		}
		if d, ok := ms["measurement"].(string); ok {
			run.MeasurementDuration = d
		}
	}

	// 房间热度诊断（loadtest 依真实分配上报）。
	if rs, ok := report["room_stats"].(map[string]any); ok {
		if d := diagnosticsFromReport(rs); d != nil {
			run.WorkloadDiagnostics = d
		}
	}

	// 速率：测量窗真实耗时 + 真实总计；窗口缺失或时间为零 → N/A。
	if r.MessagesSent != nil && run.MeasurementStart != nil && run.MeasurementEnd != nil {
		el := run.MeasurementEnd.Sub(*run.MeasurementStart).Seconds()
		if el > 0 {
			r.SendRate = newf64(float64(*r.MessagesSent) / el)
		}
	}
	if r.MessagesReceived != nil && run.MeasurementStart != nil && run.MeasurementEnd != nil {
		el := run.MeasurementEnd.Sub(*run.MeasurementStart).Seconds()
		if el > 0 {
			r.ReceiveRate = newf64(float64(*r.MessagesReceived) / el)
		}
	}
	// legacy 兜底：没有窗口信息时用 run 起止（近似）。
	if r.SendRate == nil && r.MessagesSent != nil && run.StartedAt != nil && run.FinishedAt != nil {
		el := run.FinishedAt.Sub(*run.StartedAt).Seconds()
		if el > 0 {
			r.SendRate = newf64(float64(*r.MessagesSent) / el)
		}
	}
	if r.ReceiveRate == nil && r.MessagesReceived != nil && run.StartedAt != nil && run.FinishedAt != nil {
		el := run.FinishedAt.Sub(*run.StartedAt).Seconds()
		if el > 0 {
			r.ReceiveRate = newf64(float64(*r.MessagesReceived) / el)
		}
	}
	_ = exp // 保留签名兼容
	return r, notes
}

// diagnosticsFromReport 从 loadtest room_stats 解析诊断。
func diagnosticsFromReport(rs map[string]any) *WorkloadDiagnostics {
	dist, _ := rs["distribution"].(string)
	if dist == "" {
		dist = DistUniform
	}
	d := &WorkloadDiagnostics{Distribution: dist}
	f := func(k string, p *float64) {
		if v, ok := toFloat64(rs[k]); ok {
			*p = v
		}
	}
	f("largest_room_share", &d.LargestRoomShare)
	f("top_10_percent_room_share", &d.Top10PercentRoomShare)
	f("mean_room_size", &d.MeanRoomSize)
	if v, ok := toFloat64(rs["rooms"]); ok {
		d.Rooms = int(v)
	}
	if v, ok := toFloat64(rs["connections"]); ok {
		d.Connections = int(v)
	}
	if v, ok := toFloat64(rs["max_room_size"]); ok {
		d.MaxRoomSize = int(v)
	}
	if v, ok := toFloat64(rs["min_room_size"]); ok {
		d.MinRoomSize = int(v)
	}
	if sizes, ok := rs["room_sizes"].([]any); ok {
		for _, s := range sizes {
			if fv, ok := toFloat64(s); ok {
				d.RoomSizes = append(d.RoomSizes, int(fv))
			}
			if len(d.RoomSizes) >= 200 {
				break
			}
		}
	}
	return d
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

// parseRFC3339 解析 loadtest 报告里的时间戳。
func parseRFC3339(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}

func ptrTime(t time.Time) *time.Time { return &t }

// ---- API 面：对比 / 报告 / 证据 / 预设 / 旧 loadtest 兼容 ----

// Compare 对比两个历史实验（任意架构；行级 N/A 规则见 compare.go）。
// 只允许对比已完结（completed / partial）的实验。
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
		if x.e.Status != ExpStatusCompleted && x.e.Status != ExpStatusPartial {
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

// RegimeInfos 返回 workload regime 默认模板。
func (m *ExperimentManager) RegimeInfos(target string) []RegimeInfo {
	return Regimes(target)
}

// LegacyStatus 兼容旧的 /api/loadtest/status 响应形态（新前端已不用，保留 API 契约）。
func (m *ExperimentManager) LegacyStatus() map[string]any {
	st := m.runner.Status()
	id := m.ActiveID()
	rep := m.ActiveRun()
	var active any
	if id != "" {
		if exp, err := m.Get(id); err == nil {
			active = map[string]any{
				"id": exp.ID, "name": exp.Name, "architecture": exp.Architecture,
				"preset": exp.Preset, "status": exp.Status, "repetition": rep,
				"repetitions": DefaultExpRepetitions(exp.Repetitions),
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
		Connections:  numOr(params["conns"], 1000),
		Rooms:        numOr(params["rooms"], 10),
		MessageRate:  floatOr(params["rate"], 1),
		Duration:     duration,
		Target:       server,
		Distribution: DistUniform,
		Seed:         1,
	}
	exp, err := m.Create(CreateRequest{
		Name:         "adhoc " + time.Now().Format("2006-01-02 15:04:05"),
		Preset:       "custom",
		Architecture: ArchMonolith,
		Workload:     wl,
		Repetitions:  1,
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
