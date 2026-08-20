package ops

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// --- Sweep 执行器（Phase 1.5）---
//
// 顺序执行 sweep 的每个单元（config × regime = 一个 Experiment）；
// 每步：创建实验 → 启动 → 轮询至终态 → 记录实验结果 → 继续。
// 目标是让 Sweep → Experiment → Runs 全链路可追溯。
//
// 不并行 benchmark：避免资源争用与不可归属的干扰。

// SweepManager 管理 sweep 生命周期。依赖 ExperimentManager（唯一 runner 状态机）。
type SweepManager struct {
	store *sweepStore
	em    *ExperimentManager

	mu      sync.Mutex
	active  string // 当前在跑的 sweep id；"" = 无
	stopReq bool
	stopCh  chan struct{} // 停止信号（每次 Start 复用？不——每次 run 独立）
	wakup   chan struct{} // 由 Stop/Start 唤醒 poll 循环
}

// NewSweepManager 构造 sweep 管理器。
func NewSweepManager(store *sweepStore, em *ExperimentManager) *SweepManager {
	return &SweepManager{store: store, em: em, stopCh: make(chan struct{}), wakup: make(chan struct{}, 1)}
}

// SweepObjective 是 report 生成时使用的排名目标（可配置；默认见 SensibleRankObjective）。
var SweepObjective = SensibleRankObjective()

// Create 校验并创建 sweep（status=created），立即持久化。
func (m *SweepManager) Create(req SweepRequest) (*Sweep, error) {
	sw, err := BuildSweepPlan(req)
	if err != nil {
		return nil, err
	}
	if err := m.store.Save(sw); err != nil {
		return nil, err
	}
	return sw, nil
}

// Get 读取单个 sweep。
func (m *SweepManager) Get(id string) (*Sweep, error) {
	if err := ValidateSweepID(id); err != nil {
		return nil, err
	}
	return m.store.Load(id)
}

// List 列出历史 sweeps。
func (m *SweepManager) List() ([]*Sweep, error) { return m.store.List() }

// ActiveID 返回当前在跑的 sweep id；无则空串。
func (m *SweepManager) ActiveID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// Running 判断指定 sweep 是否在跑。
func (m *SweepManager) Running(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active == id
}

// Start 开始（或恢复）sweep：后台 goroutine 顺序执行所有未完成单元。
func (m *SweepManager) Start(id string) error {
	if err := ValidateSweepID(id); err != nil {
		return err
	}
	m.mu.Lock()
	if m.active != "" && m.active != id {
		m.mu.Unlock()
		return fmt.Errorf("sweep %s is already running; cannot start %s", m.active, id)
	}
	if m.active == id {
		m.mu.Unlock()
		return fmt.Errorf("sweep %s is already running", id)
	}
	sw, err := m.store.Load(id)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if sw.Status == SweepStatusRunning {
		m.mu.Unlock()
		return fmt.Errorf("sweep %s is already running", id)
	}
	if sw.Status == SweepStatusCompleted || (len(sw.Plan) > 0 && sw.completedUnits() == len(sw.Plan)) {
		m.mu.Unlock()
		return fmt.Errorf("sweep %s is already completed", id)
	}
	sw.Status = SweepStatusRunning
	now := time.Now().UTC()
	sw.StartedAt = &now
	sw.FinishedAt = nil
	if err := m.store.Save(sw); err != nil {
		m.mu.Unlock()
		return err
	}
	m.active = id
	m.stopReq = false
	m.mu.Unlock()

	go m.runSweep(id)
	return nil
}

// Stop 停止 sweep：停止当前实验，标记 sweep stopped（已完成单元保留，可再 Start 恢复剩余）。
func (m *SweepManager) Stop(id string) error {
	if err := ValidateSweepID(id); err != nil {
		return err
	}
	m.mu.Lock()
	if m.active != id {
		m.mu.Unlock()
		if m.active == "" {
			return fmt.Errorf("no sweep is running")
		}
		return fmt.Errorf("sweep %s is not the running sweep (%s is)", id, m.active)
	}
	m.stopReq = true
	// 唤醒 poll 循环
	select {
	case m.wakup <- struct{}{}:
	default:
	}
	m.mu.Unlock()

	// 停止当前实验（若有）
	if expID := m.currentUnitExperiment(id); expID != "" {
		if err := m.em.Stop(expID); err == nil {
			return nil
		}
	}
	m.auditSweep(id, "stop requested")
	return nil
}

// currentUnitExperiment 返回当前执行单元的 experiment id（只读）。
func (m *SweepManager) currentUnitExperiment(sweepID string) string {
	sw, err := m.store.Load(sweepID)
	if err != nil {
		return ""
	}
	m.mu.Lock()
	for i := range sw.Plan {
		if !sw.Plan[i].Done && sw.Plan[i].ExpID != "" {
			id := sw.Plan[i].ExpID
			m.mu.Unlock()
			return id
		}
	}
	m.mu.Unlock()
	return ""
}

// runSweep 在后台顺序执行 sweep。
func (m *SweepManager) runSweep(id string) {
	defer func() {
		m.mu.Lock()
		m.active = ""
		m.mu.Unlock()
	}()

	for {
		select {
		case <-m.wakup:
		default:
		}
		m.mu.Lock()
		stopped := m.stopReq
		m.mu.Unlock()
		if stopped {
			m.finishSweep(id, SweepStatusStopped)
			return
		}

		sw, err := m.store.Load(id)
		if err != nil {
			log.Printf("[ops] sweep %s load failed: %v", id, err)
			return
		}
		pending := sw.pendingUnits()
		if len(pending) == 0 {
			m.finishSweep(id, SweepStatusCompleted)
			return
		}
		unit := pending[0]

		// 确保该单元的实验已创建并启动。
		if err := m.ensureUnitStarted(sw, unit); err != nil {
			log.Printf("[ops] sweep %s unit (%s/%s) ensure failed: %v", id, unit.Label, unit.Regime, err)
			unit.Done = true
			unit.Status = "failed"
			m.appendUnitResult(sw, unit, "", "ensure failed: "+err.Error())
			_ = m.store.Save(sw)
			continue
		}

		// 轮询该实验至终态。
		terminal := false
		var lastExp *Experiment
		for !terminal {
			select {
			case <-m.wakup:
			case <-time.After(250 * time.Millisecond):
			}
			m.mu.Lock()
			stopped = m.stopReq
			m.mu.Unlock()
			if stopped {
				terminal = true
				break
			}
			exp, err2 := m.em.Get(unit.ExpID)
			if err2 != nil {
				terminal = true
				break
			}
			lastExp = exp
			switch exp.Status {
			case ExpStatusCompleted, ExpStatusPartial, ExpStatusFailed, ExpStatusStopped:
				terminal = true
			}
		}

		sw, _ = m.store.Load(id)
		// 找最新单元指针（重载后 Plan 对象已变）
		u := m.unitByExp(sw, unit.ExpID)
		if u == nil {
			u = unit
		}
		u.Done = true
		u.Status = statusOfExperiment(lastExp)
		resStr := ""
		if lastExp != nil {
			resStr = lastExp.ID
		}
		_ = resStr
		m.appendUnitResult(sw, u, expIDOf(lastExp), "")
		_ = m.store.Save(sw)
	}
}

func expIDOf(e *Experiment) string {
	if e == nil {
		return ""
	}
	return e.ID
}

func statusOfExperiment(e *Experiment) string {
	if e == nil {
		return "unknown"
	}
	return e.Status
}

func (m *SweepManager) unitByExp(sw *Sweep, expID string) *SweepUnit {
	if expID == "" {
		return nil
	}
	for i := range sw.Plan {
		if sw.Plan[i].ExpID == expID {
			return &sw.Plan[i]
		}
	}
	return nil
}

// ensureUnitStarted 为该单元创建实验（若未创建）并启动（若未启动）。
func (m *SweepManager) ensureUnitStarted(sw *Sweep, unit *SweepUnit) error {
	if unit.ExpID != "" {
		exp, err := m.em.Get(unit.ExpID)
		if err != nil {
			return err
		}
		switch exp.Status {
		case ExpStatusCreated:
			return m.em.Start(exp.ID)
		case ExpStatusRunning:
			return nil // 已在跑，poll 等它
		default:
			return nil // 已终态，poll 会立即收尾
		}
	}
	exp, err := m.createSweepExperiment(sw, unit)
	if err != nil {
		return err
	}
	unit.ExpID = exp.ID
	if err := m.store.Save(sw); err != nil {
		return err
	}
	return m.em.Start(exp.ID)
}

// createSweepExperiment 依据 (config, regime) 创建一个 Experiment。
func (m *SweepManager) createSweepExperiment(sw *Sweep, unit *SweepUnit) (*Experiment, error) {
	wl, ok := regimeWorkload(unit.Regime)
	if !ok {
		return nil, fmt.Errorf("unknown regime %q", unit.Regime)
	}
	cfg := m.configOf(sw, unit.ConfigIdx)
	if cfg == nil {
		return nil, fmt.Errorf("config index %d not found", unit.ConfigIdx)
	}
	// 应用 config 级 workload 覆盖。
	for k, v := range cfg.WorkloadOverrides {
		switch k {
		case "connections", "rooms", "message_rate", "zipf_s", "distribution":
			if err := applyWorkloadOverride(&wl, k, v); err != nil {
				return nil, err
			}
		}
	}
	// 应用 sweep 级全局 workload 覆盖（缩规模/换风格）。
	for k, v := range sw.WorkloadOverrides {
		switch k {
		case "connections", "rooms", "message_rate", "zipf_s", "distribution":
			if err := applyWorkloadOverride(&wl, k, v); err != nil {
				return nil, err
			}
		}
	}
	sys := cfg.System
	if !sys.Empty() {
		sys.RequiresRestart = true
	}
	wl.Target = sw.Target
	name := fmt.Sprintf("%s / %s", regimeLabel(unit.Regime), cfg.Label)
	req := CreateRequest{
		Name:         name,
		Regime:       unit.Regime,
		Architecture: sw.Architecture,
		Warmup:       sw.Warmup,
		Duration:     sw.Duration,
		Repetitions:  sw.Repetitions,
		Workload:     wl,
		System:       sys,
		ConfigLabel:  cfg.Label,
		SweepID:      sw.ID,
	}
	exp, err := m.em.Create(req)
	if err != nil {
		return nil, fmt.Errorf("create sweep experiment: %w", err)
	}
	return exp, nil
}

func (m *SweepManager) configOf(sw *Sweep, idx int) *SweepConfig {
	// 从 Plan 里取第一个同 idx 的单元，读 label。
	seen := map[int]struct {
		label string
		sys   SystemConfig
		ov    map[string]string
	}{}
	for i := range sw.Plan {
		u := &sw.Plan[i]
		_, ok := seen[u.ConfigIdx]
		if !ok {
			seen[u.ConfigIdx] = struct {
				label string
				sys   SystemConfig
				ov    map[string]string
			}{label: u.Label, sys: SystemConfig{}, ov: map[string]string{}}
		}
	}
	// 从 Params 重建 combo 以拿到 sys/overrides。
	return m.rebuildConfig(sw, idx)
}

// rebuildConfig 从 sweep 声明重新生成某个 index 的配置（确定性：与 BuildSweepPlan 顺序一致）。
func (m *SweepManager) rebuildConfig(sw *Sweep, idx int) *SweepConfig {
	combos := cartesian(sw.Params)
	if idx < 1 || idx > len(combos) {
		return nil
	}
	c := combos[idx-1]
	return &SweepConfig{
		Label:             c.label(),
		Index:             idx,
		System:            c.system,
		WorkloadOverrides: c.overrides,
	}
}

// appendUnitResult 把单元的 Experiment 结果记录进 sweep 的 results（供 report 用）。
func (m *SweepManager) appendUnitResult(sw *Sweep, unit *SweepUnit, expID, errStr string) {
	r := SweepConfigResult{
		Regime: unit.Regime, Config: unit.Label, ConfigIdx: unit.ConfigIdx,
		ExpID: expID, Error: errStr,
	}
	if expID != "" {
		if exp, err := m.em.Get(expID); err == nil {
			r.Status = exp.Status
			r.Repetitions = DefaultExpRepetitions(exp.Repetitions)
			r.SuccessReps = RunSuccessCount(exp.Runs)
			if exp.Aggregate != nil {
				r.Throughput = exp.Aggregate.Metrics["receive_rate"]
				r.P99 = exp.Aggregate.Metrics["p99_latency_us"]
				r.P90 = exp.Aggregate.Metrics["p90_latency_us"]
				r.DeliveryRate = exp.Aggregate.Metrics["delivery_rate"]
				r.CPU = exp.Aggregate.Metrics["cpu_pct"]
			}
		}
	}
	// 替换同 (regime, config) 的旧结果
	for i := range sw.Results {
		if sw.Results[i].Regime == r.Regime && sw.Results[i].ConfigIdx == r.ConfigIdx {
			sw.Results[i] = r
			return
		}
	}
	sw.Results = append(sw.Results, r)
}

// finishSweep 收尾：计算跨 regime 报告并落盘终态。
func (m *SweepManager) finishSweep(id, status string) {
	sw, err := m.store.Load(id)
	if err != nil {
		return
	}
	sw.Status = status
	sw.touchFinished()
	// 构建 report（成功单元 >=1 个才生成）。
	objective := SweepObjective
	tunable := sweepHasSystemParam(sw)
	report := BuildCrossRegimeReport(sw.Results, objective, sw.Regimes, tunable)
	if report != nil {
		sw.Report = report
		// 把 best-per-regime 标记回 results 并补全 config index / experiment id。
		for rg, bc := range report.BestPerRegime {
			for i := range sw.Results {
				if sw.Results[i].Regime == rg && sw.Results[i].Config == bc.Config {
					sw.Results[i].Best = true
					bc.ConfigIdx = sw.Results[i].ConfigIdx
					bc.ExpID = sw.Results[i].ExpID
					report.BestPerRegime[rg] = bc
				}
			}
		}
	} else if len(sw.Results) == 0 {
		sw.Report = &SweepReport{GeneratedAt: time.Now().UTC(), BestPerRegime: map[string]BestConfig{}}
		sw.Report.AdaptiveGate.Verdict = "NOT YET JUSTIFIED"
		sw.Report.AdaptiveGate.Evidence = []string{"no completed experiment units; no cross-regime analysis possible"}
	}
	if err := m.store.Save(sw); err != nil {
		log.Printf("[ops] sweep %s persist finish failed: %v", id, err)
	}
	m.auditSweep(id, fmt.Sprintf("sweep finished with status %s", status))
}

// sweepHasSystemParam 判断 sweep 是否扫描了系统参数（Condition D）。
func sweepHasSystemParam(sw *Sweep) bool {
	if sw == nil {
		return false
	}
	for _, p := range sw.Params {
		if systemParam(p.Name) {
			return true
		}
	}
	return false
}

// Report 重新生成并返回 sweep 报告视图。
func (m *SweepManager) Report(id string) (map[string]any, error) {
	sw, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sweep":  sw,
		"report": sw.Report,
		"active": m.Running(id),
	}, nil
}

func (m *SweepManager) auditSweep(id, message string) {
	m.em.audit(eventInfo, "sweep", "sweep "+id+": "+message)
}

func regimeWorkload(regime string) (WorkloadConfig, bool) {
	for _, r := range Regimes("") {
		if r.Name == regime {
			return r.Workload, true
		}
	}
	return WorkloadConfig{}, false
}

func regimeLabel(regime string) string {
	switch regime {
	case RegimeLowFanout:
		return "Low Fanout"
	case RegimeHotRoom:
		return "Hot Room"
	case RegimeSkewedHotRoom:
		return "Skewed Hot Room"
	case RegimeHighRate:
		return "High Rate"
	}
	return regime
}

// applyWorkloadOverride 把字符串覆盖应用到 workload。
func applyWorkloadOverride(wl *WorkloadConfig, key, val string) error {
	switch key {
	case "connections", "rooms":
		v, err := parseInt(val)
		if err != nil {
			return fmt.Errorf("invalid %s %q", key, val)
		}
		if key == "connections" {
			wl.Connections = v
		} else {
			wl.Rooms = v
		}
	case "message_rate", "zipf_s":
		var v float64
		if _, err := fmt.Sscanf(val, "%g", &v); err != nil {
			return fmt.Errorf("invalid %s %q", key, val)
		}
		if key == "message_rate" {
			wl.MessageRate = v
		} else {
			wl.ZipfS = v
		}
	case "distribution":
		switch val {
		case DistUniform, DistHotRoom, DistZipf:
			wl.Distribution = val
		default:
			return fmt.Errorf("invalid distribution %q", val)
		}
	}
	return nil
}

var _ = strings.TrimSpace
