package ops

import (
	"fmt"
	"strings"
)

// Compare 是 "Realtime Systems Lab" 的对比视图：挑两个历史实验，逐指标对比。
//
// 核心约定：
//   - 只有语义相同的指标才计算 delta；一侧为 null（N/A）时该行 delta 也是 N/A，
//     绝不拿"没测"去算百分比。
//   - Delta = Run B − Run A（展示为右减左）。
//   - Verdict 依据指标方向：latency/errors/drops/kafka_lag 低更好，
//     throughput/established/trace-completion 高更好；无方向（如 requested）不判。
//   - Summary 是规则化、确定性的文本，绝不调用任何语言模型。

type compareMetricSpec struct {
	Key       string
	Label     string
	Unit      string
	Group     string  // scale | latency | throughput | reliability | distributed
	Direction string  // "" | lower_better | higher_better
	Floor     float64 // 判 "notable" 的最小绝对差（避免 0→1 被 % 放大成噪音）
}

var compareMetrics = []compareMetricSpec{
	{Key: "connections_requested", Label: "Connections requested", Unit: "conns", Group: "scale", Direction: ""},
	{Key: "connections_established", Label: "Connections established", Unit: "conns", Group: "scale", Direction: "higher_better", Floor: 1},
	{Key: "connections_failed", Label: "Connections failed", Unit: "conns", Group: "reliability", Direction: "lower_better", Floor: 1},
	{Key: "messages_sent", Label: "Messages sent", Unit: "msgs", Group: "throughput", Direction: "", Floor: 1},
	{Key: "messages_received", Label: "Messages received", Unit: "msgs", Group: "throughput", Direction: "higher_better", Floor: 1},
	{Key: "drops", Label: "Drops", Unit: "msgs", Group: "reliability", Direction: "lower_better", Floor: 1},
	{Key: "missing_deliveries", Label: "Missing deliveries", Unit: "msgs", Group: "reliability", Direction: "lower_better", Floor: 1},
	{Key: "delivery_rate", Label: "Delivery rate", Unit: "rate", Group: "reliability", Direction: "higher_better", Floor: 0.001},
	{Key: "write_errors", Label: "Write errors", Unit: "", Group: "reliability", Direction: "lower_better", Floor: 1},
	{Key: "read_errors", Label: "Read errors", Unit: "", Group: "reliability", Direction: "lower_better", Floor: 1},
	{Key: "p50_latency_us", Label: "P50 latency", Unit: "µs", Group: "latency", Direction: "lower_better", Floor: 100},
	{Key: "p90_latency_us", Label: "P90 latency", Unit: "µs", Group: "latency", Direction: "lower_better", Floor: 100},
	{Key: "p99_latency_us", Label: "P99 latency", Unit: "µs", Group: "latency", Direction: "lower_better", Floor: 100},
	{Key: "max_latency_us", Label: "Max latency", Unit: "µs", Group: "latency", Direction: "lower_better", Floor: 100},
	{Key: "send_rate", Label: "Send rate", Unit: "msg/s", Group: "throughput", Direction: "higher_better", Floor: 1},
	{Key: "receive_rate", Label: "Receive rate", Unit: "msg/s", Group: "throughput", Direction: "higher_better", Floor: 1},
	{Key: "kafka_lag", Label: "Kafka lag", Unit: "msgs", Group: "distributed", Direction: "lower_better", Floor: 1},
	{Key: "trace_completion_rate", Label: "Trace completion", Unit: "rate", Group: "distributed", Direction: "higher_better", Floor: 0.05},
}

// CompareRow 是 CompareResult 里的一行。
type CompareRow struct {
	Metric    string   `json:"metric"`
	Label     string   `json:"label"`
	Unit      string   `json:"unit"`
	Group     string   `json:"group"`
	Direction string   `json:"direction"`
	Left      *float64 `json:"left"` // nil = N/A
	Right     *float64 `json:"right"`
	Delta     *float64 `json:"delta"`     // 一侧 N/A → nil
	DeltaPct  *float64 `json:"delta_pct"` // left==0 时 nil
	Verdict   string   `json:"verdict"`   // "" | better | worse | same
}

// CompareExperimentRef 是参与对比实验的摘要（不拷贝全集，只给展示所需）。
type CompareExperimentRef struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Architecture string         `json:"architecture"`
	Preset       string         `json:"preset"`
	Status       string         `json:"status"`
	Workload     WorkloadConfig `json:"workload"`
	StartedAt    *string        `json:"started_at"`
	FinishedAt   *string        `json:"finished_at"`
}

// CompareResult 是一次对比的完整输出。
type CompareResult struct {
	Left    CompareExperimentRef `json:"left"`
	Right   CompareExperimentRef `json:"right"`
	Rows    []CompareRow         `json:"rows"`
	Summary []string             `json:"summary"` // 确定性规则化结论
	Net     string               `json:"net"`     // 一句总评

	// Phase 1.5：Experiment-vs-Experiment（aggregate-level）语义。
	// 只有 same workload + same measurement semantics 才允许强结论；
	// 否则 Comparable=false（前端显示 NOT DIRECTLY COMPARABLE）。
	Comparable     bool            `json:"comparable"`
	ComparableNote string          `json:"comparable_note,omitempty"`
	LeftAgg        *AggregateBrief `json:"left_agg,omitempty"`
	RightAgg       *AggregateBrief `json:"right_agg,omitempty"`
	DiffConclusion string          `json:"diff_conclusion,omitempty"` // likely improvement/regression/no clear difference/high variance
	DiffLines      []string        `json:"diff_lines,omitempty"`      // median delta / CI overlap / CV 逐指标
}

// AggregateBrief 是 aggregate 级对比所需的摘要。
type AggregateBrief struct {
	ID               string                      `json:"id"`
	SuccessfulReps   int                         `json:"successful_reps"`
	TotalReps        int                         `json:"total_reps"`
	Stability        string                      `json:"stability"`
	SpecHash         string                      `json:"spec_hash"`
	MeasureWindow    string                      `json:"measure_window"`
	Warmup           string                      `json:"warmup"`
	MetricAggregates map[string]*MetricAggregate `json:"metric_aggregates"` // 供前端展示 median/mean/CI/CV
}

func i2f(p *int64) *float64 {
	if p == nil {
		return nil
	}
	f := float64(*p)
	return &f
}

// resultMetric 从实验结果按 key 取数值（指针，nil=该实验此处为 N/A）。
func resultMetric(e *Experiment, key string) *float64 {
	if e == nil {
		return nil
	}
	return resultMetricFrom(&e.Result, key)
}

// CompareExperiments 计算左右两个实验的逐指标对比。
func CompareExperiments(left, right *Experiment) *CompareResult {
	res := &CompareResult{Left: refOf(left), Right: refOf(right)}
	for _, spec := range compareMetrics {
		l := resultMetric(left, spec.Key)
		r := resultMetric(right, spec.Key)
		row := CompareRow{
			Metric: spec.Key, Label: spec.Label, Unit: spec.Unit,
			Group: spec.Group, Direction: spec.Direction, Left: l, Right: r,
		}
		if l != nil && r != nil {
			d := *r - *l
			row.Delta = &d
			if *l != 0 {
				pct := d / *l * 100
				row.DeltaPct = &pct
			}
			if spec.Direction != "" {
				// 语义方向：latency/errors/drops 低更好；throughput/established 高更好。
				better := (d < 0 && spec.Direction == "lower_better") || (d > 0 && spec.Direction == "higher_better")
				worse := (d > 0 && spec.Direction == "lower_better") || (d < 0 && spec.Direction == "higher_better")
				switch {
				case better:
					row.Verdict = "better"
				case worse:
					row.Verdict = "worse"
				default:
					row.Verdict = "same"
				}
			}
		}
		res.Rows = append(res.Rows, row)
	}
	summary, net := summarizeCompare(res.Rows)
	// Phase 1.5：Experiment-vs-Experiment（aggregate-level）。
	res.applyAggregateComparison(left, right, &summary)
	// legacy：非 aggregate 对比（两侧都无 run 列表）时，若 workload 不同，保留一句说明。
	if len(left.Runs) == 0 && len(right.Runs) == 0 && !workloadsEqual(left, right) {
		summary = append([]string{
			fmt.Sprintf("Note: Run A and Run B used different workloads (A: %s vs B: %s). Deltas are descriptive, not apples-to-apples.",
				workloadBrief(left), workloadBrief(right)),
		}, summary...)
	}
	res.Summary, res.Net = summary, net
	return res
}

// specHashOf 返回实验 spec_hash（legacy 迁移后也有）。
func specHashOf(e *Experiment) string {
	if e == nil {
		return ""
	}
	if e.SpecHash != "" {
		return e.SpecHash
	}
	if h, err := SpecFromExperiment(e).SpecHash(); err == nil {
		return h
	}
	return ""
}

// aggregateBriefOf 构造 aggregate 级对比摘要。
func aggregateBriefOf(e *Experiment) *AggregateBrief {
	if e == nil {
		return nil
	}
	return &AggregateBrief{
		ID:               e.ID,
		SuccessfulReps:   aggregateRepsOf(e),
		TotalReps:        DefaultExpRepetitions(e.Repetitions),
		Stability:        aggregateStabilityOf(e),
		SpecHash:         specHashOf(e),
		MeasureWindow:    windowOf(e),
		Warmup:           e.Warmup,
		MetricAggregates: aggregateMetricsOf(e),
	}
}

func aggregateRepsOf(e *Experiment) int {
	if e.Aggregate != nil {
		return e.Aggregate.SuccessfulRepetitions
	}
	return RunSuccessCount(e.Runs)
}

func aggregateStabilityOf(e *Experiment) string {
	if e.Aggregate != nil {
		return e.Aggregate.Stability
	}
	return ""
}

func aggregateMetricsOf(e *Experiment) map[string]*MetricAggregate {
	if e.Aggregate != nil {
		return e.Aggregate.Metrics
	}
	return nil
}

func windowOf(e *Experiment) string {
	if e.Duration != "" {
		return e.Duration
	}
	return e.Workload.Duration
}

// applyAggregateComparison 在两实验均为（多 repetition）实验且语义可比时，
// 生成 aggregate-level 的 median delta / CI overlap / CV 结论。
func (res *CompareResult) applyAggregateComparison(left, right *Experiment, summary *[]string) {
	res.LeftAgg = aggregateBriefOf(left)
	res.RightAgg = aggregateBriefOf(right)

	sameWorkload := res.LeftAgg != nil && res.RightAgg != nil && res.LeftAgg.SpecHash == res.RightAgg.SpecHash
	sameWindow := res.LeftAgg != nil && res.RightAgg != nil && res.LeftAgg.MeasureWindow == res.RightAgg.MeasureWindow && res.LeftAgg.Warmup == res.RightAgg.Warmup
	isAgg := res.LeftAgg != nil && res.RightAgg != nil &&
		(len(left.Runs) > 0 || len(right.Runs) > 0 || left.Aggregate != nil || right.Aggregate != nil)

	if !isAgg {
		return
	}
	if !sameWorkload {
		res.Comparable = false
		res.ComparableNote = "NOT DIRECTLY COMPARABLE: experiments used different specs (spec_hash differs)"
		*summary = append([]string{res.ComparableNote, "compare only same-workload experiments for strong conclusions."}, *summary...)
		return
	}
	if !sameWindow {
		res.Comparable = false
		res.ComparableNote = "NOT DIRECTLY COMPARABLE: measurement windows differ (" + res.LeftAgg.MeasureWindow + "/" + res.LeftAgg.Warmup + " vs " + res.RightAgg.MeasureWindow + "/" + res.RightAgg.Warmup + ")"
		*summary = append([]string{res.ComparableNote}, *summary...)
		return
	}
	res.Comparable = true
	// 确定性规则化差异结论（每指标：median delta、CI overlap、CV）。
	var lines []string
	var regressed, improved, highVariance bool
	metricKeys := []string{"p99_latency_us", "p90_latency_us", "receive_rate", "delivery_rate", "cpu_pct"}
	for _, k := range metricKeys {
		lm, rm := res.LeftAgg.MetricAggregates[k], res.RightAgg.MetricAggregates[k]
		if lm == nil || rm == nil || !lm.Measured || !rm.Measured || lm.Median == nil || rm.Median == nil {
			continue
		}
		dir := compareMetricsDirection(k)
		l, r := *lm.Median, *rm.Median
		delta := r - l
		deltaPct := 0.0
		if l != 0 {
			deltaPct = delta / l * 100
		}
		lines = append(lines, fmt.Sprintf("%s: median %s→%s%s (Δ%+.0f, %+.1f%%)", k,
			fmtVal(l, unitOf(k)), fmtVal(r, unitOf(k)), unitOf(k), delta, deltaPct))
		// CI overlap（对均值的 bootstrap CI）。
		if lm.CI95Low != nil && lm.CI95High != nil && rm.CI95Low != nil && rm.CI95High != nil {
			overlap := ciOverlap(lm, rm)
			lines[len(lines)-1] += fmt.Sprintf(" · CI overlap=%v", overlap)
		}
		// 判向
		better := (delta < 0 && dir == "lower_better") || (delta > 0 && dir == "higher_better")
		worse := (delta > 0 && dir == "lower_better") || (delta < 0 && dir == "higher_better")
		highCV := (lm.CV != nil && *lm.CV >= 0.30) || (rm.CV != nil && *rm.CV >= 0.30)
		switch {
		case highCV:
			highVariance = true
		case better:
			improved = true
		case worse:
			regressed = true
		}
	}
	res.DiffLines = lines
	switch {
	case highVariance:
		res.DiffConclusion = "high variance"
	case improved && regressed:
		res.DiffConclusion = "mixed / no clear direction"
	case improved:
		res.DiffConclusion = "likely improvement"
	case regressed:
		res.DiffConclusion = "likely regression"
	default:
		res.DiffConclusion = "no clear difference"
	}
	*summary = append([]string{
		fmt.Sprintf("Both sides are experiment aggregates (left %d/%d reps stability=%s; right %d/%d reps stability=%s). Conclusion: %s.",
			res.LeftAgg.SuccessfulReps, res.LeftAgg.TotalReps, res.LeftAgg.Stability,
			res.RightAgg.SuccessfulReps, res.RightAgg.TotalReps, res.RightAgg.Stability, res.DiffConclusion),
	}, *summary...)
}

// aggregateDiff 需要的辅助。
func compareMetricsDirection(key string) string {
	for _, spec := range compareMetrics {
		if spec.Key == key {
			return spec.Direction
		}
	}
	return ""
}

func unitOf(key string) string {
	for _, spec := range compareMetrics {
		if spec.Key == key {
			return spec.Unit
		}
	}
	return ""
}

// ciOverlap 判断两个 bootstrap CI 是否重叠（粗略：区间有交集）。
func ciOverlap(a, b *MetricAggregate) bool {
	if a.CI95Low == nil || a.CI95High == nil || b.CI95Low == nil || b.CI95High == nil {
		return true // 无 CI（insufficient samples）→ 保守视为重叠
	}
	return *a.CI95High >= *b.CI95Low && *b.CI95High >= *a.CI95Low
}

// workloadsEqual 比较两个实验请求的 workload 是否一致。
func workloadsEqual(a, b *Experiment) bool {
	if a == nil || b == nil {
		return false
	}
	wa, wb := a.Workload, b.Workload
	return wa.Connections == wb.Connections && wa.Rooms == wb.Rooms &&
		wa.MessageRate == wb.MessageRate && wa.Duration == wb.Duration
}

// workloadBrief 一句人话描述 workload。
func workloadBrief(e *Experiment) string {
	if e == nil {
		return "?"
	}
	w := e.Workload
	return fmt.Sprintf("%dc/%dr @%v/s %s", w.Connections, w.Rooms, w.MessageRate, w.Duration)
}

func refOf(e *Experiment) CompareExperimentRef {
	if e == nil {
		return CompareExperimentRef{}
	}
	var s, f *string
	if e.StartedAt != nil {
		v := e.StartedAt.UTC().Format("2006-01-02T15:04:05Z")
		s = &v
	}
	if e.FinishedAt != nil {
		v := e.FinishedAt.UTC().Format("2006-01-02T15:04:05Z")
		f = &v
	}
	return CompareExperimentRef{
		ID: e.ID, Name: e.Name, Architecture: e.Architecture,
		Preset: e.Preset, Status: e.Status, Workload: e.Workload,
		StartedAt: s, FinishedAt: f,
	}
}

// notable 判断一行差异是否值得写进摘要：两侧都有值，且绝对差超过该指标下限，
// 且百分比差（left≠0 时）超过 10%。
func notable(row CompareRow) bool {
	if row.Delta == nil || row.Left == nil {
		return false
	}
	spec := specOf(row.Metric)
	if spec == nil {
		return false
	}
	if absF(*row.Delta) < spec.Floor {
		return false
	}
	if row.Left == nil {
		return false
	}
	// left==0 时只看绝对差（已 >= floor）；否则要求 ±10% 以上。
	if *row.Left != 0 && row.DeltaPct != nil && absF(*row.DeltaPct) < 10 {
		return false
	}
	return true
}

func specOf(key string) *compareMetricSpec {
	for i := range compareMetrics {
		if compareMetrics[i].Key == key {
			return &compareMetrics[i]
		}
	}
	return nil
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// comparePick 记录每个分组里最显著的一行。
type comparePick struct {
	row CompareRow
	ok  bool
}

// summarizeCompare 生成确定性摘要：按固定优先级组抽最显著差异，最多 3 句 + 一句总评。
func summarizeCompare(rows []CompareRow) ([]string, string) {
	best := map[string]comparePick{}
	var order []string
	for _, row := range rows {
		if !notable(row) {
			continue
		}
		if _, seen := best[row.Group]; !seen {
			order = append(order, row.Group)
		}
		cur := best[row.Group]
		if !cur.ok || notability(row) > notability(cur.row) {
			best[row.Group] = comparePick{row: row, ok: true}
		}
	}

	// 固定优先级：latency > reliability > throughput > scale > distributed
	const prio = "latency,reliability,throughput,scale,distributed"
	var lines []string
	for _, g := range strings.Split(prio, ",") {
		if len(lines) >= 3 {
			break
		}
		p, ok := best[g]
		if !ok {
			continue
		}
		if s := sentenceFor(p.row); s != "" {
			lines = append(lines, s)
		}
	}
	if len(lines) == 0 {
		lines = []string{
			"No notable difference between Run A and Run B on any comparable metric (all deltas within ±10% / below floor).",
		}
	}

	net := netVerdict(best)
	return lines, net
}

// notability 衡量一行差异的显著度（|pct|，若无 pct 用 |delta|）。
func notability(row CompareRow) float64 {
	if row.DeltaPct != nil {
		return absF(*row.DeltaPct)
	}
	if row.Delta != nil {
		return absF(*row.Delta)
	}
	return 0
}

// sentenceFor 为一行最显著差异生成中文/英文混合的确定性句子。
func sentenceFor(row CompareRow) string {
	l, r, d := row.Left, row.Right, row.Delta
	if l == nil || r == nil || d == nil {
		return ""
	}
	spec := specOf(row.Metric)
	if spec == nil {
		return ""
	}
	unit := spec.Unit
	fm := func(p *float64) string { return fmtVal(*p, unit) }

	// 根据方向与 delta 符号选动词
	improved := (*d < 0) == (spec.Direction == "lower_better") || (*d > 0) == (spec.Direction == "higher_better")
	switch spec.Group {
	case "latency":
		if *d < 0 {
			return fmt.Sprintf("Run B achieved lower %s (%s → %s %s).", spec.Label, fm(l), fm(r), unit)
		}
		return fmt.Sprintf("Run B suffered higher %s (%s → %s %s).", spec.Label, fm(l), fm(r), unit)
	case "reliability":
		if *d < 0 {
			return fmt.Sprintf("Run B produced fewer %s (%s → %s%s).", strings.ToLower(spec.Label), fm(l), fm(r), unitSuffix(unit))
		}
		return fmt.Sprintf("Run B produced more %s (%s → %s%s).", strings.ToLower(spec.Label), fm(l), fm(r), unitSuffix(unit))
	case "throughput":
		if *d > 0 {
			return fmt.Sprintf("Run B sustained higher %s (%s %s → %s %s).", spec.Label, fm(l), unit, fm(r), unit)
		}
		return fmt.Sprintf("Run B sustained lower %s (%s %s → %s %s).", spec.Label, fm(l), unit, fm(r), unit)
	case "scale":
		if *d > 0 {
			return fmt.Sprintf("Run B connected more connections (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
		}
		return fmt.Sprintf("Run B connected fewer connections (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
	case "distributed":
		if row.Metric == "kafka_lag" {
			if *d < 0 {
				return fmt.Sprintf("Run B had lower Kafka lag (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
			}
			return fmt.Sprintf("Run B had higher Kafka lag (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
		}
		if *d > 0 {
			return fmt.Sprintf("Run B recorded higher trace completion (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
		}
		return fmt.Sprintf("Run B recorded lower trace completion (%s → %s%s).", fm(l), fm(r), unitSuffix(unit))
	default:
		if improved {
			return fmt.Sprintf("Run B improved on %s (%s → %s%s).", spec.Label, fm(l), fm(r), unitSuffix(unit))
		}
		return fmt.Sprintf("Run B regressed on %s (%s → %s%s).", spec.Label, fm(l), fm(r), unitSuffix(unit))
	}
}

func unitSuffix(u string) string {
	if u == "" {
		return ""
	}
	return " " + u
}

// netVerdict 给每组打一句干净总评（better/worse/mixed/no data）。
func netVerdict(best map[string]comparePick) string {
	verdictGroup := func(group string) string {
		p, ok := best[group]
		if !ok {
			return "no notable"
		}
		row := p.row
		if row.Delta == nil {
			return "no data"
		}
		if row.Verdict == "same" {
			return "same"
		}
		return row.Verdict
	}
	lat, rel, thr := verdictGroup("latency"), verdictGroup("reliability"), verdictGroup("throughput")
	parts := []string{}
	parts = append(parts, "latency: "+lat, "reliability: "+rel, "throughput: "+thr)
	if lat == "no notable" && rel == "no notable" && thr == "no notable" {
		return "Net: no notable difference between Run A and Run B."
	}
	return "Net — " + strings.Join(parts, ", ") + "."
}

// fmtVal 确定性数值文本：整型趋势用 %.0f，否则 %.3f（rate 类）。
func fmtVal(v float64, unit string) string {
	if unit == "rate" {
		return fmt.Sprintf("%.3f", v)
	}
	if strings.HasSuffix(unit, "/s") || v != float64(int64(v)) {
		return fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%.0f", v)
}
