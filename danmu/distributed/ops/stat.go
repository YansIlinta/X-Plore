package ops

import (
	"math"
	"math/rand"
	"sort"
)

// --- 统计聚合（Phase 1.5）---
//
// 全部为纯函数，无 I/O，可单测。语义约定：
//   - 只聚合"真实测量到的值"；没测到的（N/A）不进任何统计。
//   - 空值永远不变成 0（null ≠ 0）。
//   - 混合 N/A 时报告 samples/total（如 3/5），绝不静默当作全样本。

// Moments 是一组数值的摘要统计。
type Moments struct {
	Count  int // 样本数
	Mean   *float64
	Median *float64
	Min    *float64
	Max    *float64
	StdDev *float64 // 样本标准差（n>=2 时才计算）
	CV     *float64 // 变异系数 stddev/mean（mean==0 时 nil）
}

// Mean 计算均值；空切片返回 nil。
func Mean(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	s := 0.0
	for _, v := range values {
		s += v
	}
	m := s / float64(len(values))
	return &m
}

// Median 计算中位数（未排序输入可直接用）。
func Median(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		v := sorted[n/2]
		return &v
	}
	v := (sorted[n/2-1] + sorted[n/2]) / 2
	return &v
}

// MinMax 返回 (min, max)；空切片返回两个 nil。
func MinMax(values []float64) (*float64, *float64) {
	if len(values) == 0 {
		return nil, nil
	}
	mn, mx := values[0], values[0]
	for _, v := range values[1:] {
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	return &mn, &mx
}

// StdDev 计算样本标准差（ddof=1）；n<2 返回 nil。
func StdDev(values []float64) *float64 {
	if len(values) < 2 {
		return nil
	}
	mean := Mean(values)
	if mean == nil {
		return nil
	}
	s := 0.0
	for _, v := range values {
		d := v - *mean
		s += d * d
	}
	sd := math.Sqrt(s / float64(len(values)-1))
	return &sd
}

// CV 计算变异系数（stddev/mean）；len<2 或 mean==0 返回 nil（0 不除）。
func CV(values []float64) *float64 {
	if len(values) < 2 {
		return nil
	}
	mean := Mean(values)
	sd := StdDev(values)
	if mean == nil || sd == nil {
		return nil
	}
	if *mean == 0 {
		return nil
	}
	cv := *sd / *mean
	return &cv
}

// MomentsOf 一次性计算一组数值的摘要统计。
func MomentsOf(values []float64) Moments {
	m := Moments{Count: len(values)}
	if len(values) == 0 {
		return m
	}
	m.Mean = Mean(values)
	m.Median = Median(values)
	m.Min, m.Max = MinMax(values)
	m.StdDev = StdDev(values)
	m.CV = CV(values)
	return m
}

// BootstrapMeanCI 计算均值的 (1-alpha) 分位数 bootstrap 置信区间。
//
//   - rng: 可注入的确定性随机源（测试/复现用固定 seed）；nil 时内部 new 一个
//     rand.New(rand.NewSource(42))。
//   - nResample: 重采样次数（建议 >= 500；<=0 时用 1000）。
//   - 返回 (low, high, ok)；ok=false 表示样本太少（<3）或全为零协方差，此时
//     调用方必须标记 insufficient_samples，绝不假装存在可靠区间。
func BootstrapMeanCI(values []float64, rng *rand.Rand, nResample int, alpha float64) (float64, float64, bool) {
	if len(values) < 3 {
		return 0, 0, false
	}
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.05
	}
	if nResample <= 0 {
		nResample = 1000
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(42))
	}
	loQ := alpha / 2
	hiQ := 1 - alpha/2
	n := len(values)
	means := make([]float64, nResample)
	for i := 0; i < nResample; i++ {
		s := 0.0
		for j := 0; j < n; j++ {
			s += values[rng.Intn(n)]
		}
		means[i] = s / float64(n)
	}
	sort.Float64s(means)
	low := PercentileSorted(means, loQ)
	high := PercentileSorted(means, hiQ)
	return low, high, true
}

// PercentileSorted 返回已排序切片的分位数（线性插值，p ∈ [0,1]）。
func PercentileSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	f := idx - float64(lo)
	return sorted[lo]*(1-f) + sorted[hi]*f
}

// aggregateValues 是构建 MetricAggregate 的主体：把已收集的实测值换算为摘要。
func aggregateValues(values []float64, rng *rand.Rand) (m MetricAggregate) {
	mm := MomentsOf(values)
	m.Mean = mm.Mean
	m.Median = mm.Median
	m.Min = mm.Min
	m.Max = mm.Max
	m.StdDev = mm.StdDev
	m.CV = mm.CV
	if len(values) >= 3 {
		if low, high, ok := BootstrapMeanCI(values, rng, 1000, 0.05); ok {
			m.CI95Low = &low
			m.CI95High = &high
			m.CIStatus = "ok"
		} else {
			m.CIStatus = "insufficient_samples"
		}
	} else if len(values) > 0 {
		m.CIStatus = "insufficient_samples"
	}
	return m
}
