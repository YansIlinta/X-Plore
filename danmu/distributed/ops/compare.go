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
	r := e.Result
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
	// 若 workload 不同，先放一句说明，避免把"不同负荷下的数字差"读成单纯优劣。
	if !workloadsEqual(left, right) {
		summary = append([]string{
			fmt.Sprintf("Note: Run A and Run B used different workloads (A: %s vs B: %s). Deltas are descriptive, not apples-to-apples.",
				workloadBrief(left), workloadBrief(right)),
		}, summary...)
	}
	res.Summary, res.Net = summary, net
	return res
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
