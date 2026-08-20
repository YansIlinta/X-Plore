package ops

import (
	"math"
	"sort"
	"strconv"
)

// --- Workload Regime（Phase 1.5）---
//
// Regime 本质是一份 workload spec。本阶段只定义与离线分析，不实现任何
// controller / adaptive reaction。regime 的唯一作用是让跨 workload 分析
// 有稳定的命名与可复现的默认 workload。

// 房间热度分布。
const (
	DistUniform = "uniform"
	DistHotRoom = "hot_room"
	DistZipf    = "zipf"
)

// KnownRegime 判断 regime 标签是否已注册。
func KnownRegime(r string) bool {
	switch r {
	case RegimeLowFanout, RegimeHotRoom, RegimeSkewedHotRoom, RegimeHighRate:
		return true
	}
	return false
}

const (
	RegimeLowFanout     = "low-fanout"
	RegimeHotRoom       = "hot-room"
	RegimeSkewedHotRoom = "skewed-hot-room"
	RegimeHighRate      = "high-rate"
)

// RegimeInfo 描述一个 workload regime 的默认负载。
type RegimeInfo struct {
	Name        string         `json:"name"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	Workload    WorkloadConfig `json:"workload"`
	Target      string         `json:"target"` // 环境相关，默认 ws://localhost:8081
}

// Regimes 返回全部已注册 regime 的默认 workload。
// Target 需在运行时按环境覆盖（不同机器端口不同）。
func Regimes(target string) []RegimeInfo {
	def := target
	if def == "" {
		def = "ws://localhost:8081"
	}
	return []RegimeInfo{
		{
			Name: RegimeLowFanout, Label: "Low Fanout",
			Description: "大量房间、少量连接/房 —— 低扇出基线，接近真实的普通弹幕房分布。",
			Workload: WorkloadConfig{
				Connections: 600, Rooms: 300, MessageRate: 1, Duration: "10s",
				Distribution: DistUniform, Seed: 1, Target: def,
			},
		},
		{
			Name: RegimeHotRoom, Label: "Hot Room",
			Description: "少数房间承载绝大多数连接 —— 广播放大导致的高扇出压力。",
			Workload: WorkloadConfig{
				Connections: 600, Rooms: 10, MessageRate: 2, Duration: "10s",
				Distribution: DistHotRoom, Seed: 2, Target: def,
			},
		},
		{
			Name: RegimeSkewedHotRoom, Label: "Skewed Hot Room",
			Description: "Zipf 房间热度：少数超热房 + 长尾 —— 更贴近真实直播热度分布。",
			Workload: WorkloadConfig{
				Connections: 600, Rooms: 200, MessageRate: 1.5, Duration: "10s",
				Distribution: DistZipf, ZipfS: 1.1, Seed: 3, Target: def,
			},
		},
		{
			Name: RegimeHighRate, Label: "High Rate",
			Description: "提升每连接消息率 —— 冲击发送/接收吞吐与水线。",
			Workload: WorkloadConfig{
				Connections: 400, Rooms: 100, MessageRate: 4, Duration: "10s",
				Distribution: DistUniform, Seed: 4, Target: def,
			},
		},
	}
}

// WorkloadDiagnostics 是一次 run 的房间热度诊断（由 loadtest 依真实分配上报）。
// 目标：让 "Hot Room" 不只是名字，而是可以被数据证明。
type WorkloadDiagnostics struct {
	Distribution          string  `json:"distribution"`
	Rooms                 int     `json:"rooms"`
	Connections           int     `json:"connections"`
	LargestRoomShare      float64 `json:"largest_room_share"`        // 最热房间占的连接比例（0~1）
	Top10PercentRoomShare float64 `json:"top_10_percent_room_share"` // 前 10% 房间占的连接比例
	MeanRoomSize          float64 `json:"mean_room_size"`
	MaxRoomSize           int     `json:"max_room_size"`
	MinRoomSize           int     `json:"min_room_size"`
	// RoomSizes 有界保留：最多 200 个房间的成员数（大房在前），防止无限时序。
	RoomSizes []int `json:"room_sizes,omitempty"`
}

// DiagnosticLabel 输出一句话可读诊断摘要。
func (d *WorkloadDiagnostics) DiagnosticLabel() string {
	if d == nil {
		return "no workload diagnostics"
	}
	return "distribution=" + d.Distribution + " largest_room_share=" + pctStr(d.LargestRoomShare)
}

// roomAssign 是"把 conn 分配到房间"的确定性算法（与 loadtest 端一致）。
// 返回每个 conn 的 room index（0..rooms-1）。相同输入产生相同分配。
//
//   - uniform:  conn % rooms（legacy 行为，绝对均匀）。
//   - hot_room: 80% 连接进最热房（index 0），其余均分其余房间。
//   - zipf:     按 s 参数用 zipf 分布取样房间 index。
func roomAssign(conns, rooms int, dist string, zipfS float64, seed int64) []int {
	assign := make([]int, conns)
	switch dist {
	case DistHotRoom:
		hotBoundary := int(float64(conns) * 0.8)
		for i := 0; i < conns; i++ {
			if i < hotBoundary {
				assign[i] = 0
			} else {
				assign[i] = 1 + (i % (rooms - 1))
			}
		}
	case DistZipf:
		// 近似 Zipf：指数 s，rank k 的概率 ∝ 1/k^s。用归一化累积分布 + 确定性哈希。
		s := zipfS
		if s <= 0 {
			s = 1.1
		}
		g := newZipfGenerator(rooms, s)
		rng := newDeterministic(seed)
		for i := 0; i < conns; i++ {
			assign[i] = g.sample(rng)
		}
	default: // uniform
		for i := 0; i < conns; i++ {
			assign[i] = i % rooms
		}
	}
	return assign
}

// diagnosticsFromAssign 从真实房间分配计算诊断。
func diagnosticsFromAssign(assign []int, rooms int, dist string) *WorkloadDiagnostics {
	if len(assign) == 0 {
		return nil
	}
	sizes := make([]int, rooms)
	for _, r := range assign {
		if r >= 0 && r < rooms {
			sizes[r]++
		}
	}
	sorted := append([]int(nil), sizes...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))

	d := &WorkloadDiagnostics{
		Distribution: dist,
		Rooms:        rooms,
		Connections:  len(assign),
		MaxRoomSize:  sorted[0],
		MinRoomSize:  sorted[len(sorted)-1],
	}
	if rooms > 0 {
		mean := float64(len(assign)) / float64(rooms)
		d.MeanRoomSize = mean
	}
	d.LargestRoomShare = float64(sorted[0]) / float64(len(assign))
	topN := rooms / 10
	if topN < 1 {
		topN = 1
	}
	topSum := 0
	for i := 0; i < topN && i < len(sorted); i++ {
		topSum += sorted[i]
	}
	d.Top10PercentRoomShare = float64(topSum) / float64(len(assign))

	// 有界保留：最多 200 个（大房在前的成员数）。
	if len(sorted) > 200 {
		sorted = sorted[:200]
	}
	d.RoomSizes = sorted
	return d
}

// zipfGenerator 是确定性 Zipf 取样器（归一化累积分布逆采样）。
type zipfGenerator struct {
	cdf []float64 // cdf[k] = sum_{r=1..k} 1/r^s
}

func newZipfGenerator(rooms int, s float64) *zipfGenerator {
	w := make([]float64, rooms)
	sum := 0.0
	for k := 1; k <= rooms; k++ {
		w[k-1] = 1.0 / math.Pow(float64(k), s)
		sum += w[k-1]
	}
	cdf := make([]float64, rooms)
	acc := 0.0
	for k := 0; k < rooms; k++ {
		acc += w[k] / sum
		cdf[k] = acc
	}
	return &zipfGenerator{cdf: cdf}
}

func (g *zipfGenerator) sample(rng *deterministic) int {
	u := rng.Float64()
	lo, hi := 0, len(g.cdf)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if g.cdf[mid] < u {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// newDeterministic 构造确定性随机源（独立于 math/rand 全局状态）。
type deterministic struct {
	state uint64
}

func newDeterministic(seed int64) *deterministic {
	return &deterministic{state: uint64(seed)*0x9E3779B97F4A7C15 + 1442695040888963407}
}

// splitmix64：确定性、可复现、足够快。
func (d *deterministic) next() uint64 {
	d.state += 0x9E3779B97F4A7C15
	z := d.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (d *deterministic) Float64() float64 {
	// 53 位精度
	return float64(d.next()>>11) / float64(1<<53)
}

func pctStr(v float64) string {
	return fmtPct(v)
}

func fmtPct(v float64) string {
	return fmt2dp(v*100) + "%"
}

func fmt2dp(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}
