package main

import "testing"

func TestFindPeaksBasic(t *testing.T) {
	// 构造：两端低、中间两个尖峰
	series := []PeakPoint{
		{TS: 1, Count: 1},
		{TS: 2, Count: 2},
		{TS: 3, Count: 50}, // peak
		{TS: 4, Count: 5},
		{TS: 5, Count: 3},
		{TS: 6, Count: 80}, // peak
		{TS: 7, Count: 4},
		{TS: 8, Count: 2},
		{TS: 9, Count: 1},
		{TS: 10, Count: 1},
	}
	peaks := FindPeaks(series, 5, 0.7)
	if len(peaks) != 2 {
		t.Fatalf("peaks = %v, want 2", peaks)
	}
	if peaks[0].TS != 3 || peaks[1].TS != 6 {
		t.Fatalf("peak ts = %v, want 3 then 6", peaks)
	}
}

func TestFindPeaksEmpty(t *testing.T) {
	if got := FindPeaks(nil, 10, 0.9); got != nil {
		t.Fatalf("empty = %v", got)
	}
	if got := FindPeaks([]PeakPoint{{TS: 1, Count: 1}, {TS: 2, Count: 2}}, 10, 0.9); got != nil {
		t.Fatalf("short = %v", got)
	}
}

func TestFindPeaksTopK(t *testing.T) {
	series := make([]PeakPoint, 20)
	for i := range series {
		series[i] = PeakPoint{TS: int64(i), Count: int64(i % 5)} // 周期尖峰
	}
	// 强制几个高点
	series[5].Count = 100
	series[10].Count = 90
	series[15].Count = 80
	peaks := FindPeaks(series, 2, 0.5)
	if len(peaks) != 2 {
		t.Fatalf("topK=2 got %d: %v", len(peaks), peaks)
	}
	// 按时间升序输出，但 top2 应是 count 最大的两个
	counts := map[int64]bool{}
	for _, p := range peaks {
		counts[p.Count] = true
	}
	if !counts[100] || !counts[90] {
		t.Fatalf("top2 should be 100 and 90, got %v", peaks)
	}
}

func TestFindPeaksNoLocalMax(t *testing.T) {
	// 单调递增：无局部极大
	series := []PeakPoint{
		{TS: 1, Count: 1}, {TS: 2, Count: 2}, {TS: 3, Count: 3},
		{TS: 4, Count: 4}, {TS: 5, Count: 5},
	}
	if got := FindPeaks(series, 10, 0.5); len(got) != 0 {
		t.Fatalf("monotonic should yield no peaks: %v", got)
	}
}
