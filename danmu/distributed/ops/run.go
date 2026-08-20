package ops

import (
	"fmt"
	"time"
)

// Run 状态。与实验状态相比，Run 只有单次执行自身的结局：
//
//	running → completed / failed / stopped
const (
	RunStatusRunning   = "running"
	RunStatusCompleted = "completed"
	RunStatusFailed    = "failed"
	RunStatusStopped   = "stopped"
)

// ExperimentRun 是"一次真实执行"的记录。与 Experiment 的关系：
//
//	Experiment.Spec ──(重复执行)──► [Run 1, Run 2, ..., Run N]
//
// 每个 Run 保留自己的 environment、measurement window、result、resource 与 error。
// 它绝不覆盖 Experiment.Spec（spec 在创建时已定格并哈希）。
type ExperimentRun struct {
	ID         string     `json:"id"`
	Index      int        `json:"index"`  // 1-based repetition number
	Status     string     `json:"status"` // running | completed | failed | stopped
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`

	// 有效观测窗口（warm-up 与 measurement 分离）：
	// warm-up 段的数据不进任何最终统计；measurement 段才计入。
	MeasurementStart    *time.Time `json:"measurement_start"`              // warm-up 结束 / 测量开始
	MeasurementEnd      *time.Time `json:"measurement_end"`                // 测量结束
	WarmupDuration      string     `json:"warmup_duration,omitempty"`      // 如 "3s"
	MeasurementDuration string     `json:"measurement_duration,omitempty"` // 如 "10s"

	Environment *EnvironmentSnapshot `json:"environment"` // 本次 run 开始时采集

	// WorkloadDiagnostics 是本 run 的"实际房间热度诊断"（由 loadtest 依真实分配上报）。
	WorkloadDiagnostics *WorkloadDiagnostics `json:"workload_diagnostics,omitempty"`

	// Resource 是实验期间采样的 server-side 资源汇总（min/mean/peak，有界 samples）。
	Resource *ResourceSummary `json:"resource,omitempty"`

	Result ExperimentResult `json:"result"`
	Error  string           `json:"error,omitempty"`
}

// NewRunID 生成 run id：run-<unix>-<4字节hex>。
func NewRunID() string {
	return "run-" + newExperimentIDSuffix()
}

// RunSuccessCount 统计成功（completed）的 run 数。
func RunSuccessCount(runs []*ExperimentRun) int {
	n := 0
	for _, r := range runs {
		if r.Status == RunStatusCompleted {
			n++
		}
	}
	return n
}

// RunFailureCount 统计失败/中止的 run 数（failed + stopped）。
func RunFailureCount(runs []*ExperimentRun) int {
	n := 0
	for _, r := range runs {
		if r.Status == RunStatusFailed || r.Status == RunStatusStopped {
			n++
		}
	}
	return n
}

// DeriveExperimentStatus 依据 Runs 推导实验状态（确定性、存储驱动）：
//
//	无 runs              → created
//	存在 running run     → running
//	存在 stopped run     → stopped（用户曾中止；已完成 rep 数据仍保留）
//	全部 completed       → completed
//	无 completed 且全 fail→ failed
//	部分完成（completed + failed）→ partial
//
// 老实验（无 runs）保留其顶层 Status 字段（由调用方处理）。
func DeriveExperimentStatus(runs []*ExperimentRun) string {
	if len(runs) == 0 {
		return ExpStatusCreated
	}
	for _, r := range runs {
		if r.Status == RunStatusRunning {
			return ExpStatusRunning
		}
	}
	for _, r := range runs {
		if r.Status == RunStatusStopped {
			return ExpStatusStopped
		}
	}
	suc := RunSuccessCount(runs)
	if suc == len(runs) {
		return ExpStatusCompleted
	}
	if suc == 0 {
		return ExpStatusFailed
	}
	return ExpStatusPartial
}

// ValidateRunStatus 校验 run 状态值。
func ValidateRunStatus(s string) error {
	switch s {
	case RunStatusRunning, RunStatusCompleted, RunStatusFailed, RunStatusStopped:
		return nil
	}
	return fmt.Errorf("unknown run status %q", s)
}
