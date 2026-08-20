package ops

import (
	"math/rand"
	"time"
)

// --- Experiment 聚合（Phase 1.5）---
//
// 对 experiment 的全部成功 repetition 的每个数值指标计算：
// count / mean / median / min / max / stddev / CV / 95% bootstrap CI。
// 只聚合实测值；某指标在成功 run 中部分未测 → samples=实测数，total=成功数。
// 全部成功 run 都未测某指标 → Measured=false，相关字段全 null（N/A，绝不填 0）。

// AggStability 是确定性稳定度判据文本。
const (
	AggStable   = "stable"   // 主稳定性指标 CV < 0.10
	AggModerate = "moderate" // CV < 0.30
	AggVariable = "variable" // CV >= 0.30
	AggNA       = "n/a"      // 无数据
)

// MetricAggregate 是单个数值指标在多个 repetition 上的统计聚合。
type MetricAggregate struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Unit     string `json:"unit"`
	Group    string `json:"group"`
	TotalRep int    `json:"total_rep"` // 参与聚合的成功 run 数
	Samples  int    `json:"samples"`   // 其中实测到该指标的 run 数
	Measured bool   `json:"measured"`  // samples > 0

	Mean     *float64 `json:"mean,omitempty"`
	Median   *float64 `json:"median,omitempty"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	StdDev   *float64 `json:"stddev,omitempty"`
	CV       *float64 `json:"cv,omitempty"`
	CI95Low  *float64 `json:"ci95_low,omitempty"`
	CI95High *float64 `json:"ci95_high,omitempty"`
	CIStatus string   `json:"ci_status,omitempty"` // "ok" | "insufficient_samples"
}

// ExperimentAggregate 是一个 Experiment 的统计聚合视图。
type ExperimentAggregate struct {
	GeneratedAt           time.Time                   `json:"generated_at"`
	SuccessfulRepetitions int                         `json:"successful_repetitions"`
	FailedRepetitions     int                         `json:"failed_repetitions"`
	StoppedRepetitions    int                         `json:"stopped_repetitions"`
	TotalRepetitions      int                         `json:"total_repetitions"`
	Status                string                      `json:"status"` // completed | partial
	Metrics               map[string]*MetricAggregate `json:"metrics"`
	Stability             string                      `json:"stability"`
	StabilityNote         string                      `json:"stability_note"`
}

// aggMetricDef 描述一个参与聚合的指标及其取值来源。
type aggMetricDef struct {
	Key     string
	Label   string
	Unit    string
	Group   string
	primary bool // 稳定度主指标（选 p90）
	// 取值函数：从 run 提取该指标的实测值；nil = 该 run 未测。
	pick func(r *ExperimentRun) *float64
}

// runResultMetric 从某个 run 的 result 中按 compare key 取数值（nil = N/A）。
func runResultMetric(r *ExperimentRun, key string) *float64 {
	if r == nil {
		return nil
	}
	return resultMetricFrom(&r.Result, key)
}

// resultMetricFrom 是 resultMetric 的底层实现（不依赖 *Experiment）。
func resultMetricFrom(r *ExperimentResult, key string) *float64 {
	if r == nil {
		return nil
	}
	switch key {
	case "connections_requested":
		return i2f(r.ConnectionsRequested)
	case "connections_established":
		return i2f(r.ConnectionsEstablished)
	case "connections_failed":
		return i2f(r.ConnectionsFailed)
	case "messages_sent":
		return i2f(r.MessagesSent)
	case "messages_received":
		return i2f(r.MessagesReceived)
	case "write_errors":
		return i2f(r.WriteErrors)
	case "read_errors":
		return i2f(r.ReadErrors)
	case "drops":
		return i2f(r.Drops)
	case "missing_deliveries":
		return i2f(r.MissingDeliveries)
	case "expected_deliveries":
		return i2f(r.ExpectedDeliveries)
	case "delivery_rate":
		return r.DeliveryRate
	case "p50_latency_us":
		return i2f(r.P50LatencyUS)
	case "p90_latency_us":
		return i2f(r.P90LatencyUS)
	case "p99_latency_us":
		return i2f(r.P99LatencyUS)
	case "max_latency_us":
		return i2f(r.MaxLatencyUS)
	case "send_rate":
		return r.SendRate
	case "receive_rate":
		return r.ReceiveRate
	case "kafka_lag":
		return i2f(r.KafkaLag)
	case "trace_completion_rate":
		return r.TraceCompletion
	}
	return nil
}

// aggregateMetricDefs 是参与聚合的指标清单（含资源类，取值来自 run.Resource）。
var aggregateMetricDefs = []aggMetricDef{
	{Key: "p50_latency_us", Label: "P50 latency", Unit: "µs", Group: "latency", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "p50_latency_us") }},
	{Key: "p90_latency_us", Label: "P90 latency", Unit: "µs", Group: "latency", primary: true, pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "p90_latency_us") }},
	{Key: "p99_latency_us", Label: "P99 latency", Unit: "µs", Group: "latency", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "p99_latency_us") }},
	{Key: "max_latency_us", Label: "Max latency", Unit: "µs", Group: "latency", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "max_latency_us") }},

	{Key: "send_rate", Label: "Send rate", Unit: "msg/s", Group: "throughput", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "send_rate") }},
	{Key: "receive_rate", Label: "Receive rate", Unit: "msg/s", Group: "throughput", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "receive_rate") }},

	{Key: "connections_established", Label: "Connections established", Unit: "conns", Group: "scale", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "connections_established") }},
	{Key: "write_errors", Label: "Write errors", Unit: "", Group: "reliability", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "write_errors") }},
	{Key: "read_errors", Label: "Read errors", Unit: "", Group: "reliability", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "read_errors") }},

	{Key: "delivery_rate", Label: "Delivery rate", Unit: "rate", Group: "reliability", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "delivery_rate") }},
	{Key: "missing_deliveries", Label: "Missing deliveries", Unit: "msgs", Group: "reliability", pick: func(r *ExperimentRun) *float64 { return runResultMetric(r, "missing_deliveries") }},

	{Key: "cpu_pct", Label: "Server CPU", Unit: "%", Group: "resource", pick: func(r *ExperimentRun) *float64 {
		if r.Resource != nil {
			return r.Resource.CPUPercentMean
		}
		return nil
	}},
	{Key: "rss_mb", Label: "Server RSS", Unit: "MB", Group: "resource", pick: func(r *ExperimentRun) *float64 {
		if r.Resource != nil {
			return r.Resource.RSSMean
		}
		return nil
	}},
	{Key: "heap_mb", Label: "Server heap", Unit: "MB", Group: "resource", pick: func(r *ExperimentRun) *float64 {
		if r.Resource != nil {
			return r.Resource.HeapMean
		}
		return nil
	}},
	{Key: "goroutines", Label: "Server goroutines", Unit: "", Group: "resource", pick: func(r *ExperimentRun) *float64 {
		if r.Resource != nil {
			return r.Resource.GoroutinesMean
		}
		return nil
	}},
}

// successfulRuns 返回全部成功（completed）run。
func successfulRuns(exp *Experiment) []*ExperimentRun {
	if exp == nil {
		return nil
	}
	out := make([]*ExperimentRun, 0, len(exp.Runs))
	for _, r := range exp.Runs {
		if r.Status == RunStatusCompleted {
			out = append(out, r)
		}
	}
	return out
}

// BuildExperimentAggregate 对一个 Experiment 的成功 repetition 计算统计聚合。
// 返回 nil 当没有成功 run（此时 exp.Result 不应被替换）。
func BuildExperimentAggregate(exp *Experiment, seed int64) *ExperimentAggregate {
	if exp == nil {
		return nil
	}
	runs := successfulRuns(exp)
	if len(runs) == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	agg := &ExperimentAggregate{
		GeneratedAt:           time.Now().UTC(),
		SuccessfulRepetitions: len(runs),
		FailedRepetitions:     RunFailureCount(exp.Runs),
		TotalRepetitions:      len(exp.Runs),
		Metrics:               map[string]*MetricAggregate{},
	}
	// stopped 单独计数
	for _, r := range exp.Runs {
		if r.Status == RunStatusStopped {
			agg.StoppedRepetitions++
		}
	}
	if len(runs) == len(exp.Runs) {
		agg.Status = ExpStatusCompleted
	} else {
		agg.Status = ExpStatusPartial
	}

	for _, def := range aggregateMetricDefs {
		values := make([]float64, 0, len(runs))
		for _, r := range runs {
			if v := def.pick(r); v != nil {
				values = append(values, *v)
			}
		}
		m := aggregateValues(values, rng)
		m.Key = def.Key
		m.Label = def.Label
		m.Unit = def.Unit
		m.Group = def.Group
		m.TotalRep = len(runs)
		m.Samples = len(values)
		m.Measured = len(values) > 0
		agg.Metrics[def.Key] = &m
	}

	// 稳定度：主指标 = p90 latency 的 CV（tail latency 是 adaptive 最依赖的信号）。
	if m := agg.Metrics["p90_latency_us"]; m != nil && m.Measured && m.CV != nil {
		cv := *m.CV
		switch {
		case cv < 0.10:
			agg.Stability = AggStable
		case cv < 0.30:
			agg.Stability = AggModerate
		default:
			agg.Stability = AggVariable
		}
		agg.StabilityNote = "stability keyed on p90 latency CV"
	} else {
		agg.Stability = AggNA
		agg.StabilityNote = "p90 latency not measured across repetitions; stability n/a"
	}
	return agg
}

// RepresentativeResult 把聚合结果映射为一个 ExperimentResult（兼容 Compare / Evidence）。
//
// 语义（文档化）：
//   - latency / scale 类取 median
//   - throughput / rate 类取 mean
//   - errors 取 max（最差情况）
//   - 未测指标保持 nil（N/A）
func RepresentativeResult(agg *ExperimentAggregate) ExperimentResult {
	var r ExperimentResult
	if agg == nil {
		return r
	}
	take := func(key string) *float64 {
		m := agg.Metrics[key]
		if m == nil || !m.Measured {
			return nil
		}
		return m.Median
	}
	meanTake := func(key string) *float64 {
		m := agg.Metrics[key]
		if m == nil || !m.Measured {
			return nil
		}
		return m.Mean
	}
	maxTake := func(key string) *float64 {
		m := agg.Metrics[key]
		if m == nil || !m.Measured {
			return nil
		}
		return m.Max
	}

	r.ConnectionsEstablished = int64ptrOf(take("connections_established"))
	r.P50LatencyUS = int64ptrOf(take("p50_latency_us"))
	r.P90LatencyUS = int64ptrOf(take("p90_latency_us"))
	r.P99LatencyUS = int64ptrOf(take("p99_latency_us"))
	r.MaxLatencyUS = int64ptrOf(take("max_latency_us"))
	r.SendRate = meanTake("send_rate")
	r.ReceiveRate = meanTake("receive_rate")
	r.WriteErrors = int64ptrOf(maxTake("write_errors"))
	r.ReadErrors = int64ptrOf(maxTake("read_errors"))
	r.DeliveryRate = meanTake("delivery_rate")
	r.MissingDeliveries = int64ptrOf(maxTake("missing_deliveries"))
	r.Drops = r.MissingDeliveries
	// 自身字段级的 notes 由调用方补（注明 representative 语义）。
	return r
}

func int64ptrOf(p *float64) *int64 {
	if p == nil {
		return nil
	}
	v := int64(*p)
	return &v
}

// aggMetric 按 key 查询聚合指标。
func (a *ExperimentAggregate) Metric(key string) *MetricAggregate {
	if a == nil {
		return nil
	}
	return a.Metrics[key]
}
