package main

import (
	"sort"
)

// PeakPoint 弹幕密度时间点（ts 为秒级 unix 或毫秒，count 为该桶内条数）。
type PeakPoint struct {
	TS    int64 `json:"ts"`
	Count int64 `json:"count"`
}

// FindPeaks 从密度序列中挑出「高能点」：
//  1. 计算 count 的 percentile 分位阈值（默认 0.9）
//  2. 取严格大于左右邻居且 count >= 阈值的局部极大值
//  3. 按 count 降序取 topK（默认 10）
//
// 空序列 / 全 0 / 不足 3 点 → 返回空。阴性结果也算完成（无峰值可展示）。
func FindPeaks(series []PeakPoint, topK int, percentile float64) []PeakPoint {
	if len(series) < 3 {
		return nil
	}
	if topK <= 0 {
		topK = 10
	}
	if percentile <= 0 || percentile >= 1 {
		percentile = 0.9
	}

	// 分位阈值
	counts := make([]int64, len(series))
	for i, p := range series {
		counts[i] = p.Count
	}
	sorted := append([]int64(nil), counts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * percentile)
	threshold := sorted[idx]

	// 局部极大
	var candidates []PeakPoint
	for i := 1; i < len(series)-1; i++ {
		c := series[i]
		if c.Count < threshold {
			continue
		}
		if c.Count > series[i-1].Count && c.Count > series[i+1].Count {
			candidates = append(candidates, c)
		}
	}
	// 按 count 降序取 topK
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Count > candidates[j].Count })
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	// 输出按时间升序（UI 进度条友好）
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].TS < candidates[j].TS })
	return candidates
}
