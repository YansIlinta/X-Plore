package ops

import (
	"fmt"
	"strings"
	"time"
)

// Experiment 是一次可复现的 "Realtime Systems Lab" 运行单元。
// 它只描述"要跑什么、跑没跑、跑完测得什么"，不直接执行任何业务逻辑——
// 实际压测由仓库既有的 loadtest 二进制（子进程）执行，本文件只是它的旁路记录层。
//
// 数据真实性约定（与 Collector 一致）：未测量的字段必须为 null（前端渲染 N/A），
// 绝不把"没测"伪装成"测得 0"。0 表示真实测量值为零，null 表示没有测量。

// 实验状态机（Phase 1.5：多跑多 Run）：
//
//	created ──start──► running ──(全部成功)──► completed
//	                          ├─(部分成功)──► partial
//	                          ├─(全部失败)──► failed
//	                          └─(用户 stop)──► stopped
//	completed / partial / failed / stopped ──start──► running（重新执行同一规格）
//
// 任意时刻全局最多只有一个 run 在跑（loadtest 子进程是单例，见 loadtestManager）。
const (
	ExpStatusCreated   = "created"
	ExpStatusRunning   = "running"
	ExpStatusCompleted = "completed"
	ExpStatusPartial   = "partial"
	ExpStatusFailed    = "failed"
	ExpStatusStopped   = "stopped"
)

// 支持的架构标签。它们是记录性元数据：决定"结束后采集哪些旁路观测"（分布式才有
// Kafka/trace/topology）、以及 Compare 只能比较语义相同的架构。
const (
	ArchMonolith    = "monolith"
	ArchDistributed = "distributed"
)

// ExpStatuses 是对外的合法状态集合。
var ExpStatuses = []string{
	ExpStatusCreated,
	ExpStatusRunning,
	ExpStatusCompleted,
	ExpStatusPartial,
	ExpStatusFailed,
	ExpStatusStopped,
}

// WorkloadConfig 只包含当前 loadtest 真实支持的参数。
// Target 是逗号分隔的 ws:// 或 wss:// 地址（与 loadtest --server 语义一致）。
// Distribution / ZipfS / Seed 是 Phase 1.5 新增的房间热度分布参数：
//   - Distribution: uniform | hot_room | zipf
//   - ZipfS:        zipf 分布的 s 参数（s>1 越集中）
//   - Seed:         deterministic random seed（相同 seed 产出相同分配）
type WorkloadConfig struct {
	Connections int     `json:"connections"`
	Rooms       int     `json:"rooms"`
	MessageRate float64 `json:"message_rate"` // 每连接每秒消息数
	Duration    string  `json:"duration"`     // Go duration 字符串，如 "60s"（legacy：也用作测量窗）
	Target      string  `json:"target"`       // 如 "ws://localhost:8081" 或 "ws://a:8080,ws://b:8080"

	Distribution string  `json:"distribution,omitempty"` // uniform | hot_room | zipf
	ZipfS        float64 `json:"zipf_s,omitempty"`       // zipf s 参数
	Seed         int64   `json:"seed,omitempty"`         // deterministic seed
}

// DistributionKind 返回已归一化的分布名（空 → uniform）。
func (w WorkloadConfig) DistributionKind() string {
	if w.Distribution == "" {
		return DistUniform
	}
	return w.Distribution
}

// Validate 校验 workload 是否可被 loadtest 执行。返回哨兵原因（供 API 422）。
func (w WorkloadConfig) Validate() error {
	if w.Connections < 1 || w.Connections > 100000 {
		return fmt.Errorf("connections must be in [1, 100000], got %d", w.Connections)
	}
	if w.Rooms < 1 || w.Rooms > 10000 {
		return fmt.Errorf("rooms must be in [1, 10000], got %d", w.Rooms)
	}
	if w.MessageRate < 0 || w.MessageRate > 10000 {
		return fmt.Errorf("message_rate must be in [0, 10000], got %v", w.MessageRate)
	}
	if w.Duration == "" {
		return fmt.Errorf("duration is required (e.g. 30s)")
	}
	if _, err := time.ParseDuration(w.Duration); err != nil {
		return fmt.Errorf("duration %q is not a valid Go duration: %v", w.Duration, err)
	}
	if strings.TrimSpace(w.Target) == "" {
		return fmt.Errorf("target is required (ws:// or wss:// URL, comma separated)")
	}
	for _, u := range strings.Split(w.Target, ",") {
		u = strings.TrimSpace(u)
		if !strings.HasPrefix(u, "ws://") && !strings.HasPrefix(u, "wss://") {
			return fmt.Errorf("target %q must start with ws:// or wss://", u)
		}
	}
	switch w.DistributionKind() {
	case DistUniform, DistHotRoom, DistZipf:
	default:
		return fmt.Errorf("distribution must be uniform, hot_room or zipf, got %q", w.Distribution)
	}
	if w.DistributionKind() == DistZipf && (w.ZipfS <= 0 || w.ZipfS > 5) {
		return fmt.Errorf("zipf_s must be in (0, 5], got %v", w.ZipfS)
	}
	if w.Rooms < 2 && w.DistributionKind() == DistHotRoom {
		return fmt.Errorf("hot_room distribution requires at least 2 rooms (got %d)", w.Rooms)
	}
	return nil
}

// DurationParsed 返回解析后的时长（Create 已通过 Validate，这里不报错）。
func (w WorkloadConfig) DurationParsed() time.Duration {
	d, _ := time.ParseDuration(w.Duration)
	return d
}

// ExperimentResult 是跑完一次实验后记录的测量结果。
// 全部字段用指针：nil == 未测量/不支持（N/A），非 nil == 真实测量值（可能为 0）。
type ExperimentResult struct {
	// 连接层（loadtest --output-json summary 直接给出）
	ConnectionsRequested   *int64 `json:"connections_requested"`
	ConnectionsEstablished *int64 `json:"connections_established"`
	ConnectionsFailed      *int64 `json:"connections_failed"`

	// 吞吐层
	MessagesSent     *int64 `json:"messages_sent"`
	MessagesReceived *int64 `json:"messages_received"`

	// 可靠性层
	// drops 当前 loadtest 不测量（dropCount 从未递增），恒为 null，见 EXPERIMENTS.md 已知限制。
	// Phase 1.5：当 loadtest 以 -delivery-check 模式运行且能可靠计算投递缺口时，
	// MissingDeliveries / ExpectedDeliveries / DeliveryRate 会被真实填写，
	// Drops 反映为 MissingDeliveries（真实的投递缺口），否则保持 null。
	Drops              *int64   `json:"drops"`
	MissingDeliveries  *int64   `json:"missing_deliveries,omitempty"`  // 观测到的按连接投递缺口（seq 跳跃求和）
	ExpectedDeliveries *int64   `json:"expected_deliveries,omitempty"` // 应投递的按连接投递次数 = observed+missing
	DeliveryRate       *float64 `json:"delivery_rate,omitempty"`       // observed/expected（0~1）；未测 → null
	WriteErrors        *int64   `json:"write_errors"`                  // 由 loadtest 每帧快照累计计数聚合
	ReadErrors         *int64   `json:"read_errors"`

	// 延迟层（微秒；loadtest HDR Histogram）
	P50LatencyUS *int64 `json:"p50_latency_us"`
	P90LatencyUS *int64 `json:"p90_latency_us"`
	P99LatencyUS *int64 `json:"p99_latency_us"`
	MaxLatencyUS *int64 `json:"max_latency_us"`

	// 速率（msg/s；由测量总计 + 真实耗时推导）
	SendRate    *float64 `json:"send_rate"`
	ReceiveRate *float64 `json:"receive_rate"`

	// --- 分布式附加观测（仅 architecture=distributed 且旁路 Collector 可观测时记录，
	// --- 否则全为 null/整个 ServiceSnapshot 为 null，不制造假数据） ---
	KafkaAvailable  *bool                `json:"kafka_available"`
	KafkaLag        *int64               `json:"kafka_lag"`
	EtcdUp          *bool                `json:"etcd_up"`
	TraceSamples    *int64               `json:"trace_samples"`         // 结束时汇聚到的 trace 条数
	TraceCompletion *float64             `json:"trace_completion_rate"` // 完整链路占比（0~1）；无可观测数据为 null
	ServiceSnapshot *DistributedSnapshot `json:"service_snapshot"`

	// RepresentativeTraces 只存少量完整链路作为 owned 快照（旁路汇聚的参考样本）。
	RepresentativeTraces []Trace `json:"representative_traces,omitempty"`

	// Notes 记录诚实边界：哪些字段为什么是 N/A。不占指标位置。
	Notes []string `json:"notes,omitempty"`
}

// DistributedSnapshot 是一次分布式实验结束时旁路 Collector 抓到的服务面快照。
type DistributedSnapshot struct {
	CometTotal   int    `json:"comet_total"`
	CometHealthy int    `json:"comet_healthy"`
	LogicTotal   int    `json:"logic_total"`
	JobTotal     int    `json:"job_total"`
	EtcdUp       bool   `json:"etcd_up"`
	Health       string `json:"health"` // healthy | degraded | critical
	FreeText     string `json:"free_text,omitempty"`
}

// EnvironmentSnapshot 是复现一次性能数字所需的最小环境信息。
// 无法可靠获得的字段一律 null（不猜），目标是让数字可回答：
// "这个数字在哪个版本、什么机器、什么 workload 下产生？"
type EnvironmentSnapshot struct {
	GitCommit   *string `json:"git_commit"` // 无 repo / git 失败 → null
	GitDirty    *bool   `json:"git_dirty"`  // 工作区是否有未提交修改；未知 → null
	GoVersion   string  `json:"go_version"` // runtime.Version()
	OS          string  `json:"os"`
	Arch        string  `json:"arch"`
	CPUCores    int     `json:"cpu_cores"`
	MemoryBytes *int64  `json:"memory_bytes"` // 读不到（非 Linux /meminfo 不可达）→ null
	Hostname    *string `json:"hostname"`     // os.Hostname 失败 → null
}

// Experiment 是一个持久化的实验记录。ID 由 Manager 生成，只允许
// [A-Za-z0-9_-]，不允许任何路径分隔符（防目录穿越）。
//
// Phase 1.5：Experiment 从一个单 Run 提升为"可重复执行的实验容器"。
//   - Spec        —— 不可变的实验规格（执行开始后不再改动）
//   - SpecHash    —— canonical hash，判断两个实验是否同配置
//   - Repetitions —— 期望的重复次数；老文件（0）视为 1
//   - Runs        —— 每次实际执行的 Run 记录（顺序追加）
//   - Aggregate   —— 对成功 repetitions 的统计聚合（成功 reps≥1 才有）
//   - Result      —— 兼容旧层（Compare/Evidence）的"代表结果"：
//     老实验 = 那次运行的原始结果；新实验 = Aggregate 的代表值
//     （各 latency 用 median、throughput 用 mean、errors 取 max）。
type Experiment struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	Architecture string               `json:"architecture"`
	Preset       string               `json:"preset"` // low-fanout | hot-room | custom（legacy；新实验可空）
	Status       string               `json:"status"`
	Workload     WorkloadConfig       `json:"workload"`
	CreatedAt    time.Time            `json:"created_at"`
	StartedAt    *time.Time           `json:"started_at"`  // 未开始 → null
	FinishedAt   *time.Time           `json:"finished_at"` // 未结束 → null
	Environment  *EnvironmentSnapshot `json:"environment"` // 开始第一个 run 时采集；未开始 → null
	Result       ExperimentResult     `json:"result"`
	Error        string               `json:"error,omitempty"`

	// --- Phase 1.5 新增字段（全部向后兼容；老文件自动迁移补齐） ---
	SchemaVersion int                  `json:"schema_version,omitempty"` // 老文件 0
	Regime        string               `json:"regime,omitempty"`         // workload regime 标签
	ConfigLabel   string               `json:"config_label,omitempty"`   // system config 标签
	Warmup        string               `json:"warmup,omitempty"`         // Go duration；"" = 无 warm-up
	Duration      string               `json:"duration,omitempty"`       // 测量窗口；老文件回退到 Workload.Duration
	Repetitions   int                  `json:"repetitions,omitempty"`    // 老文件 0 → 视为 1
	Spec          ExperimentSpec       `json:"spec,omitempty"`
	SpecHash      string               `json:"spec_hash,omitempty"`
	SystemConfig  SystemConfig         `json:"system_config,omitempty"`
	SweepID       string               `json:"sweep_id,omitempty"` // 属于哪个 sweep（无则空）
	Runs          []*ExperimentRun     `json:"runs,omitempty"`
	Aggregate     *ExperimentAggregate `json:"aggregate,omitempty"`
}

// CanStart 判断一个实验是否允许被 start。
func (e *Experiment) CanStart() error {
	switch e.Status {
	case ExpStatusRunning:
		return fmt.Errorf("experiment %s is already running", e.ID)
	case ExpStatusCreated, ExpStatusCompleted, ExpStatusFailed, ExpStatusStopped, ExpStatusPartial:
		return nil
	}
	return fmt.Errorf("experiment %s has unknown status %q", e.ID, e.Status)
}

// CanStop 判断一个实验是否允许被 stop。
func (e *Experiment) CanStop() error {
	if e.Status != ExpStatusRunning {
		return fmt.Errorf("experiment %s is not running (status=%s)", e.ID, e.Status)
	}
	return nil
}

// ValidateExperimentID 拒绝任何目录穿越 / 非法文件名。Manager 生成的都是
// "exp-<unix>-<rand>"，API 路径中的 id 也需过这里。
func ValidateExperimentID(id string) error {
	if id == "" {
		return fmt.Errorf("experiment id is required")
	}
	if len(id) > 64 {
		return fmt.Errorf("experiment id too long")
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '-' && r != '_' {
			return fmt.Errorf("experiment id %q contains invalid characters", id)
		}
	}
	if id == "." || id == ".." {
		return fmt.Errorf("experiment id %q not allowed", id)
	}
	return nil
}

// ValidateArchitecture 只接受两个已知架构标签。
func ValidateArchitecture(arch string) error {
	switch arch {
	case ArchMonolith, ArchDistributed:
		return nil
	}
	return fmt.Errorf("architecture must be %q or %q, got %q", ArchMonolith, ArchDistributed, arch)
}

// newi64 / newf64 / newbool 构造指针，用于把真实测量值写入 Result。
func newi64(v int64) *int64     { return &v }
func newf64(v float64) *float64 { return &v }
func newbool(v bool) *bool      { return &v }
