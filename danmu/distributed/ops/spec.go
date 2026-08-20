package ops

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// --- Phase 1.5 领域模型：Run / Experiment / Sweep ---
//
//   Run        —— 一次真实执行（一个 workload 跑一次），见 run.go
//   Experiment —— 一个需要重复执行的实验规格（spec 不可变）+ 它的全部 Run
//   Sweep      —— 多个 Experiment 的集合（deterministic Cartesian product），见 sweep.go
//   Regime     —— 一个 workload regime，本质是一份 workload spec（见 regimes.go）
//
// 本文件定义 ExperimentSpec 及其 canonical SpecHash。

// SchemaVersion 是持久化 schema 版本。Phase 1 老文件（无该字段）按 0 处理，
// 加载时通过 MigrateExperiment 提升到当前版本并补齐默认语义。
const SchemaVersion = 1

// ExperimentSpec 是"一个实验要重复跑什么"的不可变规格。一旦执行开始，
// spec 不得再被修改；结果永远不覆盖原始 spec。
type ExperimentSpec struct {
	Architecture string         `json:"architecture"`
	Regime       string         `json:"regime,omitempty"`       // workload regime 标签；非空时按 regime 语义
	ConfigLabel  string         `json:"config_label,omitempty"` // 人类可读的 system config 标签（sweep 展示用）
	Workload     WorkloadConfig `json:"workload"`
	System       SystemConfig   `json:"system,omitempty"` // server-side 配置（可 sweep 的系统参数）
	Warmup       string         `json:"warmup"`           // Go duration；"" = 无 warm-up
	Duration     string         `json:"duration"`         // 测量窗口时长
	Repetitions  int            `json:"repetitions"`      // 重复次数（1..maxRepetitions）
}

const (
	// MaxRepetitions 单实验允许的最大重复次数（防无限制）。
	MaxRepetitions = 20
	// DefaultRepetitions 默认重复次数。
	DefaultRepetitions = 3
	// MinRepetitions 最少重复次数。
	MinRepetitions = 1
)

// SystemConfig 描述可选的 server-side 系统配置。
// 当前代码真实存在的可调参数：batch_size / batch_timeout / workers
// （monolith/server/worker.go 的编译期常量），全部 require restart。
// 0 / "" 表示"使用 server 默认值"（不传对应 argv）。
type SystemConfig struct {
	BatchSize    int    `json:"batch_size,omitempty"`    // 0 = server 默认
	BatchTimeout string `json:"batch_timeout,omitempty"` // Go duration；"" = server 默认
	Workers      int    `json:"workers,omitempty"`       // 0 = server 默认（默认 NumCPU()*2）

	// RequiresRestart 是生成/校验时填写的语义标记：这些系统参数只属于
	// server startup config，改它们必须重启被控 server 进程，绝不是 runtime tunable。
	RequiresRestart bool `json:"requires_restart"`
}

// Empty 报告该 SystemConfig 是否等价于"不施加任何系统配置"（用默认）。
func (s SystemConfig) Empty() bool {
	return s.BatchSize == 0 && s.BatchTimeout == "" && s.Workers == 0
}

// Validate 校验 SystemConfig 的取值区间（仅当显式设置时）。
func (s SystemConfig) Validate() error {
	if s.BatchSize < 0 || s.BatchSize > 1_000_000 {
		return fmt.Errorf("batch_size must be in [0, 1000000], got %d", s.BatchSize)
	}
	if s.Workers < 0 || s.Workers > 1024 {
		return fmt.Errorf("workers must be in [0, 1024], got %d", s.Workers)
	}
	if s.BatchTimeout != "" {
		if _, err := time.ParseDuration(s.BatchTimeout); err != nil {
			return fmt.Errorf("batch_timeout %q is not a valid Go duration: %v", s.BatchTimeout, err)
		}
		bt, _ := time.ParseDuration(s.BatchTimeout)
		if bt < 0 {
			return fmt.Errorf("batch_timeout must be non-negative, got %s", s.BatchTimeout)
		}
	}
	return nil
}

// DefaultExpRepetitions 归一化重复次数：0 → 默认 3（对老 scheme 亦成立）。
func DefaultExpRepetitions(n int) int {
	if n <= 0 {
		return DefaultRepetitions
	}
	return n
}

// Validate 校验一套完整 ExperimentSpec。返回哨兵错误（供 API 400/422 判断）。
func (s ExperimentSpec) Validate() error {
	if err := ValidateArchitecture(s.Architecture); err != nil {
		return err
	}
	if err := s.Workload.Validate(); err != nil {
		return err
	}
	if err := s.System.Validate(); err != nil {
		return err
	}
	if s.Duration == "" {
		return fmt.Errorf("spec duration is required (measurement window)")
	}
	if _, err := time.ParseDuration(s.Duration); err != nil {
		return fmt.Errorf("spec duration %q is not a valid Go duration: %v", s.Duration, err)
	}
	if s.Warmup != "" {
		if _, err := time.ParseDuration(s.Warmup); err != nil {
			return fmt.Errorf("spec warmup %q is not a valid Go duration: %v", s.Warmup, err)
		}
	}
	if s.Repetitions < MinRepetitions || s.Repetitions > MaxRepetitions {
		return fmt.Errorf("repetitions must be in [%d, %d], got %d", MinRepetitions, MaxRepetitions, s.Repetitions)
	}
	if s.Regime != "" && !KnownRegime(s.Regime) {
		return fmt.Errorf("unknown workload regime %q", s.Regime)
	}
	return nil
}

// DurationParsed 解析测量窗口时长（已校验，不报错）。
func (s ExperimentSpec) DurationParsed() time.Duration {
	d, _ := time.ParseDuration(s.Duration)
	return d
}

// WarmupParsed 解析 warm-up 时长（"" → 0）。
func (s ExperimentSpec) WarmupParsed() time.Duration {
	if s.Warmup == "" {
		return 0
	}
	d, _ := time.ParseDuration(s.Warmup)
	return d
}

// canonicalSpecObject 把 spec 展开成 map[string]any，作为 canonical JSON 的来源。
// encoding/json 对 map key 按字典序排序 ⇒ 序列化结果与字段书写顺序无关，
// 相同 spec 恒得相同字节；不含 experiment id / timestamps / 测量结果 / 随机元数据。
func canonicalSpecObject(s ExperimentSpec) map[string]any {
	// 手动枚举字段，避免隐式依赖 struct 声明顺序（当前顺序稳定，但显式更安全）。
	return map[string]any{
		"architecture": s.Architecture,
		"regime":       s.Regime,
		"config_label": s.ConfigLabel,
		"duration":     s.Duration,
		"repetitions":  s.Repetitions,
		"warmup":       s.Warmup,
		"workload": map[string]any{
			"connections":  s.Workload.Connections,
			"distribution": s.Workload.Distribution,
			"duration":     s.Workload.Duration, // fallback（旧 spec 用 Workload.Duration 时保留语义）
			"message_rate": s.Workload.MessageRate,
			"rooms":        s.Workload.Rooms,
			"seed":         s.Workload.Seed,
			"target":       s.Workload.Target,
			"zipf_s":       s.Workload.ZipfS,
		},
		"system": map[string]any{
			"batch_size":       s.System.BatchSize,
			"batch_timeout":    s.System.BatchTimeout,
			"requires_restart": s.System.RequiresRestart,
			"workers":          s.System.Workers,
		},
	}
}

// SpecHash 返回 spec 的稳定 canonical hash：SHA-256(canonical JSON)，hex 编码。
// 用途：判断两个实验是否真的使用同一配置（同 spec 必同 hash，任意 workload /
// system 配置改变必改 hash）。
func (s ExperimentSpec) SpecHash() (string, error) {
	data, err := json.Marshal(canonicalSpecObject(s))
	if err != nil {
		return "", fmt.Errorf("canonical spec marshal: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ShortHash 返回 hash 的前 12 位（UI 展示用）；空/非法输入返回空串。
func ShortHash(h string) string {
	if len(h) < 12 {
		return h
	}
	return h[:12]
}

// SpecFromExperiment 从已有 Experiment 的字段构建其 spec（迁移/展示用）。
// 优先使用已保存的 exp.Spec；否则从顶层字段重建（legacy 兼容）。
func SpecFromExperiment(e *Experiment) ExperimentSpec {
	if e != nil && e.Spec.Architecture != "" {
		return e.Spec
	}
	if e == nil {
		return ExperimentSpec{}
	}
	w := e.Workload
	if w.Duration == "" {
		w.Duration = e.Duration
	}
	if w.Distribution == "" {
		w.Distribution = "uniform"
	}
	return ExperimentSpec{
		Architecture: e.Architecture,
		Regime:       e.Regime,
		ConfigLabel:  e.ConfigLabel,
		Workload:     w,
		System:       e.SystemConfig,
		Warmup:       e.Warmup,
		Duration:     e.Duration,
		Repetitions:  DefaultExpRepetitions(e.Repetitions),
	}
}

// sortedKeys 工具：返回 map 的字典序 key（保持代码内不使用乱序遍历）。
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
