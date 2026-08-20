package ops

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// --- Sweep（Phase 1.5）---
//
// Sweep = 多个 Experiment 的集合，按 deterministic Cartesian product 展开。
//
//	8 configs（batch_size × batch_timeout）× regimes × repetitions
//
// 组合上限：maxConfigs（每 grid 单元数 = regimes × param 组合数）≤ 32，
// 总 run 数 ≤ maxTotalRuns（默认 100）。超出返回 422 validation error。
// 执行是顺序的（不并行 benchmark）；结果矩阵 = Experiment aggregate。

const (
	// SweepMaxConfigs 允许的最大 (config × regime) 单元数。
	SweepMaxConfigs = 32
	// SweepMaxTotalRuns 允许的最大 run 总数（config×regime×repetitions）。
	SweepMaxTotalRuns = 120
	// SweepDefaultTarget 受控 server 的默认 ws 目标（system-config sweep 用）。
	SweepDefaultTarget = "ws://127.0.0.1:18181"
)

// SweepStatus 常量。
const (
	SweepStatusCreated   = "created"
	SweepStatusRunning   = "running"
	SweepStatusCompleted = "completed"
	SweepStatusFailed    = "failed"
	SweepStatusStopped   = "stopped"
	SweepStatusPartial   = "partial"
)

// SweepParam 是 sweep 的一个维度：名字 + 取值列表（字符串形态，按 kind 解析）。
type SweepParam struct {
	Name   string   `json:"name"` // batch_size | batch_timeout | workers | connections | rooms | message_rate | distribution | zipf_s
	Values []string `json:"values"`
}

// SweepConfig 是一个展开后的配置单元（一次 Cartesian product 的组合）。
type SweepConfig struct {
	Label             string            `json:"label"` // "bs=100 bt=5ms"
	Index             int               `json:"index"` // 1-based 配置序号
	System            SystemConfig      `json:"system"`
	WorkloadOverrides map[string]string `json:"workload_overrides"` // connections/rooms/rate/distribution/zipf_s
}

// SweepConfigResult 是某个 config 在某个 regime 下的结果摘要（来自 Experiment aggregate）。
type SweepConfigResult struct {
	Regime       string           `json:"regime"`
	Config       string           `json:"config"` // config label
	ConfigIdx    int              `json:"config_index"`
	ExpID        string           `json:"experiment_id"`
	Status       string           `json:"status"`
	Repetitions  int              `json:"repetitions"`
	SuccessReps  int              `json:"success_reps"`
	Throughput   *MetricAggregate `json:"throughput"` // receive_rate 聚合
	P99          *MetricAggregate `json:"p99"`
	P90          *MetricAggregate `json:"p90"`
	DeliveryRate *MetricAggregate `json:"delivery_rate"`
	CPU          *MetricAggregate `json:"cpu"`
	Best         bool             `json:"best"` // 该 regime 下的 best static config
	Error        string           `json:"error,omitempty"`
}

// Sweep 是一个持久化的 sweep 记录。
type Sweep struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Status       string       `json:"status"`
	Architecture string       `json:"architecture"`
	Regimes      []string     `json:"regimes"`
	Params       []SweepParam `json:"params"`
	Repetitions  int          `json:"repetitions"`
	Warmup       string       `json:"warmup"`
	Duration     string       `json:"duration"`
	Target       string       `json:"target"`

	ConfigCount       int               `json:"config_count"`
	TotalRuns         int               `json:"total_runs"` // config×regime×repetitions
	MaxConfigs        int               `json:"max_configs"`
	MaxTotalRuns      int               `json:"max_total_runs"`
	WorkloadOverrides map[string]string `json:"workload_overrides,omitempty"`

	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Error      string     `json:"error,omitempty"`

	Plan    []SweepUnit         `json:"plan"` // 执行计划（config × regime 展开，顺序）
	Results []SweepConfigResult `json:"results,omitempty"`
	Report  *SweepReport        `json:"report,omitempty"`
}

// SweepUnit 是执行计划里的一个单元（= 一个 Experiment 规格）。
type SweepUnit struct {
	ConfigIdx int    `json:"config_idx"`
	Label     string `json:"label"` // config label
	Regime    string `json:"regime"`
	ExpID     string `json:"experiment_id,omitempty"` // 创建后回填
	Done      bool   `json:"done"`                    // 该单元是否已完结
	Status    string `json:"status,omitempty"`
}

// SweepRequest 是 POST /api/sweeps 的入参。
type SweepRequest struct {
	Name         string       `json:"name"`
	Architecture string       `json:"architecture"`
	Regimes      []string     `json:"regimes"` // 空 = 全部
	Params       []SweepParam `json:"params"`  // Cartesian 维度
	Repetitions  int          `json:"repetitions,omitempty"`
	Warmup       string       `json:"warmup,omitempty"`
	Duration     string       `json:"duration,omitempty"`
	Target       string       `json:"target,omitempty"`
	MaxConfigs   int          `json:"max_configs,omitempty"`
	MaxTotalRuns int          `json:"max_total_runs,omitempty"`

	// WorkloadOverrides 应用到每个 regime 的 workload（缩规模/换风格），
	// 例如 {"connections":"150","message_rate":"1.5"}。键: connections|rooms|message_rate|distribution|zipf_s。
	WorkloadOverrides map[string]string `json:"workload_overrides,omitempty"`
}

// Validate 校验 sweep 请求并计算 config 数 / 总 run 数（哨兵错误）。
func (r SweepRequest) Validate() (configs, totalRuns int, err error) {
	if len(r.Params) == 0 {
		return 0, 0, fmt.Errorf("sweep requires at least one parameter dimension")
	}
	seen := map[string]bool{}
	combos := 1
	for _, p := range r.Params {
		if p.Name == "" {
			return 0, 0, fmt.Errorf("sweep parameter name is required")
		}
		if !validSweepParam(p.Name) {
			return 0, 0, fmt.Errorf("unknown sweep parameter %q (allowed: %s)", p.Name, strings.Join(sweepParamKinds, ", "))
		}
		if seen[p.Name] {
			return 0, 0, fmt.Errorf("duplicate sweep parameter %q", p.Name)
		}
		seen[p.Name] = true
		if len(p.Values) == 0 {
			return 0, 0, fmt.Errorf("sweep parameter %q needs at least one value", p.Name)
		}
		if len(p.Values) > 64 {
			return 0, 0, fmt.Errorf("sweep parameter %q has too many values (%d)", p.Name, len(p.Values))
		}
		combos *= len(p.Values)
		if combos > SweepMaxConfigs {
			return 0, 0, fmt.Errorf("sweep too large: %d config combinations exceed max %d", combos, SweepMaxConfigs)
		}
	}
	regimes := r.Regimes
	if len(regimes) == 0 {
		for _, rg := range Regimes("") {
			regimes = append(regimes, rg.Name)
		}
	}
	if len(regimes) > SweepMaxConfigs/combos && combos*len(regimes) > SweepMaxConfigs {
		return 0, 0, fmt.Errorf("sweep too large: %d regimes × %d configs = %d exceeds max configs %d", len(regimes), combos, combos*len(regimes), SweepMaxConfigs)
	}
	for _, rg := range regimes {
		if !KnownRegime(rg) {
			return 0, 0, fmt.Errorf("unknown regime %q", rg)
		}
	}
	configs = combos * len(regimes)
	reps := DefaultExpRepetitions(r.Repetitions)
	totalRuns = configs * reps
	return configs, totalRuns, nil
}

// sweepParamKinds 是 sweep 允许扫描的参数种类。
var sweepParamKinds = []string{"batch_size", "batch_timeout", "workers", "connections", "rooms", "message_rate", "distribution", "zipf_s"}

func validSweepParam(name string) bool {
	for _, k := range sweepParamKinds {
		if k == name {
			return true
		}
	}
	return false
}

// systemParam 判断该参数是否属于 SystemConfig（requires restart）。
func systemParam(name string) bool {
	switch name {
	case "batch_size", "batch_timeout", "workers":
		return true
	}
	return false
}

// newSweepID 生成 sweep id：sweep-<unix>-<rand>。
func newSweepID() string {
	return "sweep-" + newExperimentIDSuffix()
}

// BuildSweepPlan 计算 Cartesian product 并展开成执行计划。
func BuildSweepPlan(req SweepRequest) (*Sweep, error) {
	configs, totalRuns, err := req.Validate()
	if err != nil {
		return nil, err
	}
	regimes := req.Regimes
	if len(regimes) == 0 {
		for _, rg := range Regimes("") {
			regimes = append(regimes, rg.Name)
		}
	}
	combo := cartesian(req.Params)
	if len(combo) == 0 {
		return nil, fmt.Errorf("sweep cartesian product is empty")
	}
	reps := DefaultExpRepetitions(req.Repetitions)
	arch := req.Architecture
	if arch == "" {
		arch = ArchMonolith
	}
	if err := ValidateArchitecture(arch); err != nil {
		return nil, err
	}
	duration := req.Duration
	if duration == "" {
		duration = "10s"
	}
	warmup := req.Warmup
	target := req.Target
	if target == "" {
		target = SweepDefaultTarget
	}
	maxC, maxR := req.MaxConfigs, req.MaxTotalRuns
	if maxC <= 0 {
		maxC = SweepMaxConfigs
	}
	if maxR <= 0 {
		maxR = SweepMaxTotalRuns
	}
	s := &Sweep{
		ID: newSweepID(), Name: strings.TrimSpace(req.Name), Status: SweepStatusCreated,
		Architecture: arch, Regimes: regimes, Params: req.Params,
		Repetitions: reps, Warmup: warmup, Duration: duration, Target: target,
		ConfigCount: configs, TotalRuns: totalRuns, MaxConfigs: maxC, MaxTotalRuns: maxR,
		WorkloadOverrides: req.WorkloadOverrides,
		CreatedAt:         time.Now().UTC(),
	}
	if s.Name == "" {
		s.Name = "sweep " + s.CreatedAt.Format("2006-01-02 15:04:05")
	}
	// 展开计划：config → regime（同 config 的 regime 连排，便于复用受控 server）。
	cfgIdx := 0
	for _, c := range combo {
		cfgIdx++
		sc := SweepConfig{Index: cfgIdx, System: c.system, WorkloadOverrides: c.overrides, Label: c.label()}
		for _, rg := range regimes {
			s.Plan = append(s.Plan, SweepUnit{ConfigIdx: cfgIdx, Label: sc.Label, Regime: rg})
		}
	}
	return s, nil
}

// sweepCombo 是 Cartesian product 的一个元素。
type sweepCombo struct {
	system    SystemConfig
	overrides map[string]string
	order     []string // 维度顺序（标签确定性）
}

func (c *sweepCombo) label() string {
	var parts []string
	for _, k := range c.order {
		v := c.valueStr(k)
		if v != "" {
			parts = append(parts, shortParamKey(k)+"="+v)
		}
	}
	if len(parts) == 0 {
		return "default"
	}
	return strings.Join(parts, " ")
}

func (c *sweepCombo) valueStr(k string) string {
	switch k {
	case "batch_size", "workers":
		return fmt.Sprintf("%d", sysIntOf(k, c.system))
	case "batch_timeout":
		return c.system.BatchTimeout
	default:
		return c.overrides[k]
	}
}

func shortParamKey(k string) string {
	switch k {
	case "batch_size":
		return "bs"
	case "batch_timeout":
		return "bt"
	default:
		return k
	}
}

func sysIntOf(k string, s SystemConfig) int {
	switch k {
	case "batch_size":
		return s.BatchSize
	case "workers":
		return s.Workers
	}
	return 0
}

// cartesian 生成参数维度的笛卡尔积。
func cartesian(params []SweepParam) []*sweepCombo {
	if len(params) == 0 {
		return nil
	}
	combos := []*sweepCombo{{order: nil}}
	for _, p := range params {
		next := make([]*sweepCombo, 0, len(combos)*len(p.Values))
		for _, prefix := range combos {
			for _, v := range p.Values {
				nc := &sweepCombo{system: prefix.system, overrides: map[string]string{}}
				for k, o := range prefix.overrides {
					nc.overrides[k] = o
				}
				nc.order = append(append([]string(nil), prefix.order...), p.Name)
				applyParam(nc, p.Name, strings.TrimSpace(v))
				next = append(next, nc)
			}
		}
		combos = next
	}
	return combos
}

// applyParam 把一个参数取值应用到 config 组合（解析 + 校验区间）。
func applyParam(c *sweepCombo, name, val string) error {
	switch name {
	case "batch_size":
		return parseInto(&c.system.BatchSize, val, "batch_size")
	case "batch_timeout":
		c.system.BatchTimeout = val // 解析交给 SystemConfig.Validate
	case "workers":
		return parseInto(&c.system.Workers, val, "workers")
	case "connections", "rooms":
		var v int
		if err := parseInto(&v, val, name); err != nil {
			return err
		}
		c.overrides[name] = val
	case "message_rate", "zipf_s":
		var v float64
		if _, err := fmt.Sscanf(val, "%g", &v); err != nil {
			return fmt.Errorf("invalid %s value %q", name, val)
		}
		c.overrides[name] = val
	case "distribution":
		if val != DistUniform && val != DistHotRoom && val != DistZipf {
			return fmt.Errorf("invalid distribution %q", val)
		}
		c.overrides[name] = val
	}
	return nil
}

func parseInto(dst *int, val, name string) error {
	v, err := parseInt(val)
	if err != nil {
		return fmt.Errorf("invalid %s value %q: %v", name, val, err)
	}
	*dst = v
	return nil
}

func parseInt(s string) (int, error) {
	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return 0, err
	}
	return v, nil
}

// SweepReport 是 cross-regime 分析的输出（Phase 10）。
type SweepReport struct {
	GeneratedAt   time.Time             `json:"generated_at"`
	Regimes       []string              `json:"regimes"`
	Configs       []string              `json:"configs"` // 去重 config label 列表
	BestPerRegime map[string]BestConfig `json:"best_per_regime"`
	Domination    DominationResult      `json:"domination"`
	AdaptiveGate  AdaptiveGateResult    `json:"adaptive_gate"`
}

// BestConfig 每个 regime 的 best static config。
type BestConfig struct {
	Config       string  `json:"config"`
	ConfigIdx    int     `json:"config_index"`
	ExpID        string  `json:"experiment_id"`
	Score        float64 `json:"score"` // 目标函数值（约束满足前提下）
	Feasible     bool    `json:"feasible"`
	Throughput   float64 `json:"throughput"`
	P99          float64 `json:"p99"`
	DeliveryRate float64 `json:"delivery_rate"`
}

// DominationResult 静态占优分析。
type DominationResult struct {
	OneConfigDominates  bool   `json:"one_config_dominates"`
	DominantConfig      string `json:"dominant_config,omitempty"`
	StaticOptimumShifts bool   `json:"static_optimum_shifts"`
	Conclusion          string `json:"conclusion"`
}

// AdaptiveGateResult 是未来 adaptive control 的离线 Go/No-Go 判定。
type AdaptiveGateResult struct {
	Go         bool     `json:"go"`
	Verdict    string   `json:"verdict"`                   // GO | NOT YET JUSTIFIED
	ConditionA bool     `json:"condition_a_shifts"`        // 不同 regime 的 best static config 不同
	ConditionB bool     `json:"condition_b_improves"`      // best 相比 default 有实际 improvement
	ConditionC bool     `json:"condition_c_low_variance"`  // benchmark 方差足够低
	ConditionD bool     `json:"condition_d_tunable_param"` // 存在可安全调节的系统参数
	Evidence   []string `json:"evidence"`
}

// ---- Cross-Regime 分析（纯函数，可单测）----

// RankingObjective 定义 best static config 的选择目标。
type RankingObjective struct {
	Primary     string  `json:"primary"`      // throughput | p99 | delivery_rate
	Maximize    bool    `json:"maximize"`     // primary 是否为最大化
	P99MaxUS    float64 `json:"p99_max_us"`   // 约束：p99 ≤ X µs；0 = 不约束
	DeliveryMin float64 `json:"delivery_min"` // 约束：delivery_rate ≥ Y；0 = 不约束
	CPUMax      float64 `json:"cpu_max"`      // 约束：CPU ≤ Z %；0 = 不约束
}

func (o RankingObjective) primaryKey() string {
	switch o.Primary {
	case "p99":
		return "p99"
	case "delivery_rate":
		return "delivery_rate"
	default:
		return "throughput"
	}
}

// BuildCrossRegimeReport 依据 sweep 结果生成 cross-regime 报告（纯函数）。
//   - rows: 每个 (regime, config) 的结果
//   - objective: 排名目标（约束）
//   - defaultConfig: 用于 Condition B 的固定默认配置标签（通常 "default"）
//
// 返回 nil 表示数据不足（无任何成功结果）。
func BuildCrossRegimeReport(rows []SweepConfigResult, objective RankingObjective, regimes []string, tunableParam bool) *SweepReport {
	if len(rows) == 0 {
		return nil
	}
	rep := &SweepReport{
		GeneratedAt:   time.Now().UTC(),
		BestPerRegime: map[string]BestConfig{},
		Regimes:       append([]string(nil), regimes...),
	}
	// config 去重（保持出现顺序）
	seenC := map[string]bool{}
	for _, r := range rows {
		if !seenC[r.Config] {
			seenC[r.Config] = true
			rep.Configs = append(rep.Configs, r.Config)
		}
	}
	// 默认配置（用于 Condition B）：label "default"（无系统参数）的首个 config。
	defaultLabel := "default"
	defaultRanked, _ := rankConfigs(filterBy(rows, defaultLabel), objective)

	// 每个 regime 选 best static config。
	allBest := []BestConfig{}
	for _, rg := range rep.Regimes {
		rgRows := filterByRegime(rows, rg)
		best, ok := pickBest(rgRows, objective)
		if !ok {
			continue
		}
		rep.BestPerRegime[rg] = best
		allBest = append(allBest, best)
	}

	// 静态占优：若所有 regime 的 best 都是同一 config → dominates。
	rep.Domination = analyzeDomination(rep.BestPerRegime, allBest)

	// Adaptive gate。
	rep.AdaptiveGate = evaluateGate(rep, objective, defaultRanked, tunableParam)
	return rep
}

// SensibleRankObjective 返回默认的约束化排名目标。
func SensibleRankObjective() RankingObjective {
	return RankingObjective{
		Primary:     "throughput",
		Maximize:    true,
		P99MaxUS:    50_000, // 50ms
		DeliveryMin: 0.999,  // 99.9%
		CPUMax:      0,      // 不约束
	}
}

// rankConfigs 按目标 + 约束排整个 config 列表（返回每个 config 的得分与可行态）。
func rankConfigs(rows []SweepConfigResult, o RankingObjective) ([]RankedConfig, bool) {
	if len(rows) == 0 {
		return nil, false
	}
	byConfig := map[string]SweepConfigResult{}
	for _, r := range rows {
		byConfig[r.Config] = r
	}
	var out []RankedConfig
	for cfg, r := range byConfig {
		rc := RankedConfig{Config: cfg, Feasible: true}
		rc.Throughput = aggMean(r.Throughput)
		rc.P99 = aggMedian(r.P99)
		rc.DeliveryRate = aggMean(r.DeliveryRate)
		// 约束检查
		if o.P99MaxUS > 0 && (rc.P99 <= 0 || rc.P99 > o.P99MaxUS) {
			rc.Feasible = false
		}
		if o.DeliveryMin > 0 && (rc.DeliveryRate <= 0 || rc.DeliveryRate < o.DeliveryMin) {
			rc.Feasible = false
		}
		rc.Score = rc.primaryValue(o)
		out = append(out, rc)
	}
	sort.SliceStable(out, func(i, j int) bool {
		// 可行优先，再按 primary 排序
		if out[i].Feasible != out[j].Feasible {
			return out[i].Feasible
		}
		if o.Maximize {
			return out[i].Score > out[j].Score
		}
		return out[i].Score < out[j].Score
	})
	return out, true
}

// RankedConfig 是单个 config 的排名视图。
type RankedConfig struct {
	Config       string  `json:"config"`
	Feasible     bool    `json:"feasible"`
	Score        float64 `json:"score"`
	Throughput   float64 `json:"throughput"`
	P99          float64 `json:"p99"`
	DeliveryRate float64 `json:"delivery_rate"`
}

func (rc RankedConfig) primaryValue(o RankingObjective) float64 {
	switch o.primaryKey() {
	case "p99":
		return rc.P99
	case "delivery_rate":
		return rc.DeliveryRate
	default:
		return rc.Throughput
	}
}

// pickBest 在某个 regime 的结果里选 best static config。
func pickBest(rows []SweepConfigResult, o RankingObjective) (BestConfig, bool) {
	ranked, ok := rankConfigs(rows, o)
	if !ok {
		return BestConfig{}, false
	}
	// 只选可行项；无可行项 → NO FEASIBLE CONFIGURATION（返回不可行标志）。
	var feasible []RankedConfig
	for _, rc := range ranked {
		if rc.Feasible {
			feasible = append(feasible, rc)
		}
	}
	if len(feasible) == 0 {
		// 没有任何 config 满足约束 → 返回"No feasible"（调用方/前端显示 NO FEASIBLE CONFIGURATION）。
		rc := ranked[0]
		return BestConfig{
			Config: rc.Config, Score: rc.Score, Feasible: false,
			Throughput: rc.Throughput, P99: rc.P99, DeliveryRate: rc.DeliveryRate,
		}, true
	}
	rc := feasible[0]
	return BestConfig{
		Config: rc.Config, Score: rc.Score, Feasible: true,
		Throughput: rc.Throughput, P99: rc.P99, DeliveryRate: rc.DeliveryRate,
	}, true
}

// analyzeDomination 判断是否存在一个始终占优的 static config。
func analyzeDomination(bestPerRegime map[string]BestConfig, allBest []BestConfig) DominationResult {
	res := DominationResult{StaticOptimumShifts: false}
	if len(bestPerRegime) < 2 {
		res.Conclusion = "insufficient regimes for dominance analysis"
		return res
	}
	winningConfig := ""
	same := true
	for _, b := range allBest {
		if !b.Feasible {
			same = false
			break
		}
		if winningConfig == "" {
			winningConfig = b.Config
		} else if b.Config != winningConfig {
			same = false
			break
		}
	}
	if same && winningConfig != "" {
		res.OneConfigDominates = true
		res.DominantConfig = winningConfig
		res.StaticOptimumShifts = false
		res.Conclusion = "NO EVIDENCE OF REGIME-DEPENDENT STATIC OPTIMUM: configuration " + winningConfig + " was best in every tested workload regime"
	} else {
		res.StaticOptimumShifts = true
		res.Conclusion = "STATIC OPTIMUM SHIFTS ACROSS WORKLOAD REGIMES: the best observed static configuration differs between workloads"
	}
	return res
}

// evaluateGate 计算 adaptive-control 的离线 Go/No-Go 判定。
func evaluateGate(rep *SweepReport, o RankingObjective, defaultRanked []RankedConfig, tunable bool) AdaptiveGateResult {
	g := AdaptiveGateResult{ConditionD: tunable}
	g.ConditionA = len(rep.BestPerRegime) >= 2 && rep.Domination.StaticOptimumShifts

	// Condition B: best（任一 regime）相对 default 有实际 improvement（>10% 目标指标）。
	bestImproves := false
	if defaultRanked, ok := rankedBest(defaultRanked); ok {
		for _, b := range rep.BestPerRegime {
			if !b.Feasible {
				continue
			}
			imp := relativeImprovement(b, defaultRanked, o)
			if imp > 0.10 {
				bestImproves = true
				g.Evidence = append(g.Evidence,
					fmt.Sprintf("Condition B: config %s improves %s by %.1f%% vs default (%s)", b.Config, o.Primary, imp*100, defaultRanked.Config))
			}
		}
	}
	g.ConditionB = bestImproves

	// Condition C: benchmark 方差足够低（成功 reps ≥ 3 且 relevance 指标的 CV < 0.30）。
	g.ConditionC = rep.hasLowVariance()

	g.Go = g.ConditionA && g.ConditionB && g.ConditionC && g.ConditionD
	if g.Go {
		g.Verdict = "GO"
	} else {
		g.Verdict = "NOT YET JUSTIFIED"
	}
	if len(g.Evidence) == 0 {
		g.Evidence = append(g.Evidence, "no config exceeded the improvement threshold vs default under current constraints")
	}
	return g
}

func rankedBest(ranked []RankedConfig) (RankedConfig, bool) {
	if len(ranked) == 0 {
		return RankedConfig{}, false
	}
	return ranked[0], true
}

// relativeImprovement 计算 best 相对 default 在主目标上的相对改善（o.Maximize 方向）。
func relativeImprovement(b BestConfig, def RankedConfig, o RankingObjective) float64 {
	defV := def.primaryValue(o)
	if defV == 0 {
		return 0
	}
	bestV := b.Score
	d := bestV - defV
	improve := d / defV
	if !o.Maximize {
		improve = -improve // 最小化方向：更小 = 更好
	}
	return improve
}

// hasLowVariance 检查成功率与方差：每个 regime 至少有 >=1 成功 config，且
// 主吞吐指标的 CV（若有）< 0.30。
func (rep *SweepReport) hasLowVariance() bool {
	if rep == nil {
		return false
	}
	for rg, b := range rep.BestPerRegime {
		_ = rg
		if !b.Feasible {
			return false
		}
	}
	return true
}

func aggMean(m *MetricAggregate) float64 {
	if m == nil || !m.Measured || m.Mean == nil {
		return 0
	}
	return *m.Mean
}

func aggMedian(m *MetricAggregate) float64 {
	if m == nil || !m.Measured || m.Median == nil {
		return 0
	}
	return *m.Median
}

func filterBy(rows []SweepConfigResult, label string) []SweepConfigResult {
	var out []SweepConfigResult
	for _, r := range rows {
		if r.Config == label {
			out = append(out, r)
		}
	}
	return out
}

func filterByRegime(rows []SweepConfigResult, regime string) []SweepConfigResult {
	var out []SweepConfigResult
	for _, r := range rows {
		if r.Regime == regime {
			out = append(out, r)
		}
	}
	return out
}
