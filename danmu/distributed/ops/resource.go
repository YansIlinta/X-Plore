package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

// NumCPU 返回当前进程可见的 CPU 核数。
func NumCPU() int { return runtime.NumCPU() }

// --- Server Resource Measurement（Phase 1.5）---
//
// 采样目标 server 的 /api/v1/stats 端点（monolith server 与 distributed comet 均有，
// 本阶段为 endpoint 增加 rss_bytes / cpu_ns / gc_pause_ns 字段）。
//
// 约束：
//   - 采样间隔有界（默认 1s）
//   - 实验停止后 sampler 必须退出（绑生命周期 ctx + done）
//   - 无 goroutine leak
//   - metric 拿不到 → null
//   - 不读全系统数据冒充目标 process 数据（只读目标进程自身的 /proc/self 上报值）
//
// 注意：CPU/RSS 来自目标进程自己上报的 /proc/self 数据，是"目标 process 数据"，
// 不是我们读 /proc 目录里任意 pid 的越权行为。

// ResourceSample 是单次采样点（有界保留，供前端画趋势）。
type ResourceSample struct {
	T          int64   `json:"t"` // unix 秒
	Goroutines int64   `json:"goroutines,omitempty"`
	HeapMB     float64 `json:"heap_mb,omitempty"`
	RSSMB      float64 `json:"rss_mb,omitempty"`
	CPUPercent float64 `json:"cpu_pct,omitempty"` // 采样间隔内的 CPU 利用率（0~100×ncpu 归一）
	GCPerMin   float64 `json:"gc_per_min,omitempty"`
}

// ResourceSummary 是一次 run 期间采到的 server-side 资源汇总。
// 全部指针：nil = 该资源不可测（如非 Linux / 端点无字段），绝不填 0。
type ResourceSummary struct {
	// 采样信息
	Sampled           bool       `json:"sampled"` // 是否有任何成功采样
	FirstSample       *time.Time `json:"first_sample,omitempty"`
	LastSample        *time.Time `json:"last_sample,omitempty"`
	SampleCount       int        `json:"sample_count"`
	UnavailableReason string     `json:"unavailable_reason,omitempty"`

	// 关键指标（mean / peak）
	CPUPercentMean *float64 `json:"cpu_pct_mean,omitempty"`
	CPUPercentPeak *float64 `json:"cpu_pct_peak,omitempty"`
	RSSMean        *float64 `json:"rss_mb_mean,omitempty"`
	RSSPeak        *float64 `json:"rss_mb_peak,omitempty"`
	HeapMean       *float64 `json:"heap_mb_mean,omitempty"`
	HeapPeak       *float64 `json:"heap_mb_peak,omitempty"`
	GoroutinesMean *float64 `json:"goroutines_mean,omitempty"`
	GoroutinesPeak *float64 `json:"goroutines_peak,omitempty"`
	GCTotal        *int64   `json:"gc_total,omitempty"`    // 测量窗内 GC 周期数
	GCPauseMS      *float64 `json:"gc_pause_ms,omitempty"` // 测量窗内 GC 暂停总时长(ms)

	// 有界时序 samples（最多 MaxResourceSamples 个），供前端画趋势。
	Samples []ResourceSample `json:"samples,omitempty"`
}

// MaxResourceSamples 保留的资源时序样本上限。
const MaxResourceSamples = 240

const (
	// ResourceSampleInterval 默认采样间隔。
	ResourceSampleInterval = time.Second
	// ResourceStatsPath 目标 server 的统计端点。
	ResourceStatsPath = "/api/v1/stats"
)

// ResourceSampler 周期采样一个 HTTP 目标。
type ResourceSampler struct {
	mu       sync.Mutex
	baseURL  string // http://host:port
	token    string
	interval time.Duration

	pending map[string]float64 // 上一周期值（cpu_ns / gc_count 增量计算）

	summary  *ResourceSummary
	seq      []ResourceSample
	stop     chan struct{} // Stop() 关闭，通知 loop 退出
	exited   chan struct{} // loop 退出时关闭，Wait() 等待
	stopOnce sync.Once
}

// StartResourceSampler 开始对 target（ws://host:port 或 http://host:port）采样，
// 直到 ctx 取消或 Stop 被调用。立即返回 *ResourceSampler（挂起 goroutine 采样）。
func StartResourceSampler(ctx context.Context, target, token string, interval time.Duration) *ResourceSampler {
	base := httpBaseOf(target)
	s := &ResourceSampler{
		baseURL:  base,
		token:    token,
		interval: interval,
		pending:  map[string]float64{},
		summary:  &ResourceSummary{},
		stop:     make(chan struct{}),
		exited:   make(chan struct{}),
	}
	s.summary.Sampled = false
	go s.loop(ctx)
	return s
}

// Summary 返回采到的汇总（并发安全）。
func (s *ResourceSampler) Summary() *ResourceSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.summary == nil || !s.summary.Sampled {
		return s.summary
	}
	out := *s.summary
	out.Samples = append([]ResourceSample(nil), s.summary.Samples...)
	return &out
}

// Stop 终止采样并等待 goroutine 退出（保证无泄漏）。幂等。
func (s *ResourceSampler) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.exited
}

// Wait 阻塞至 sampler 结束（供测试断言无泄漏）。
func (s *ResourceSampler) Wait() { <-s.exited }

func (s *ResourceSampler) loop(ctx context.Context) {
	defer close(s.exited)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	// 立即采第一点
	s.sampleOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-ticker.C:
			s.sampleOnce(ctx)
		}
	}
}

func (s *ResourceSampler) sampleOnce(ctx context.Context) {
	ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx2, http.MethodGet, s.baseURL+ResourceStatsPath, nil)
	if err != nil {
		return
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.recordFailure("stats endpoint unreachable: " + err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		s.recordFailure(fmt.Sprintf("stats endpoint HTTP %d", resp.StatusCode))
		return
	}
	var st struct {
		Goroutines int64   `json:"goroutines"`
		HeapMB     float64 `json:"heap_mb"`
		RSSBytes   *int64  `json:"rss_bytes"`
		CPUNs      *int64  `json:"cpu_ns"`
		GCCount    *int64  `json:"gc_count"`
		GCPauseNS  *int64  `json:"gc_pause_ns"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		s.recordFailure("stats endpoint bad json: " + err.Error())
		return
	}
	now := time.Now().UTC()

	// CPU：cpu_ns 增量 / (墙钟增量 × ncpu)。
	cpuPct := s.deltaCPU(st.CPUNs, now)
	gcDelta := s.deltaGC(st.GCCount)
	gcPauseMS := s.deltaGCPause(st.GCPauseNS)

	sp := ResourceSample{
		T:          now.Unix(),
		Goroutines: st.Goroutines,
		HeapMB:     st.HeapMB,
		CPUPercent: cpuPct,
		GCPerMin:   gcDelta,
	}
	var rssMB *float64
	if st.RSSBytes != nil {
		v := float64(*st.RSSBytes) / 1024 / 1024
		rssMB = &v
		sp.RSSMB = v
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.summary.SampleCount == 0 {
		t := now
		s.summary.FirstSample = &t
		s.summary.Sampled = true
	}
	last := now
	s.summary.LastSample = &last
	s.summary.SampleCount++
	n := float64(s.summary.SampleCount)

	// 均值（滚动）与峰值分别维护
	updateMeanPeak := func(meanP, peakP **float64, v float64) {
		if meanP != nil {
			if *meanP == nil {
				nv := v
				*meanP = &nv
			} else {
				nv := (**meanP*(n-1) + v) / n
				*meanP = &nv
			}
		}
		if peakP != nil && (*peakP == nil || v > **peakP) {
			nv := v
			*peakP = &nv
		}
	}
	updateMeanPeak(&s.summary.CPUPercentMean, &s.summary.CPUPercentPeak, cpuPct)
	updateMeanPeak(&s.summary.GoroutinesMean, &s.summary.GoroutinesPeak, float64(st.Goroutines))
	updateMeanPeak(&s.summary.HeapMean, &s.summary.HeapPeak, st.HeapMB)
	if rssMB != nil {
		updateMeanPeak(&s.summary.RSSMean, &s.summary.RSSPeak, *rssMB)
	}
	if gcPauseMS > 0 {
		if s.summary.GCPauseMS == nil {
			v := gcPauseMS
			s.summary.GCPauseMS = &v
		} else {
			*s.summary.GCPauseMS += gcPauseMS
		}
	}
	if gcDelta > 0 {
		total := float64(0)
		if s.summary.GCTotal != nil {
			total = float64(*s.summary.GCTotal)
		}
		s.summary.GCTotal = intPtr(int64(total + gcDelta))
	}
	s.seq = append(s.seq, sp)
	if len(s.seq) > MaxResourceSamples {
		s.seq = s.seq[len(s.seq)-MaxResourceSamples:]
	}
	// 暂存最终 samples 于 summary（副本）
	s.summary.Samples = append([]ResourceSample(nil), s.seq...)
}

func intPtr(v int64) *int64 { return &v }

func (s *ResourceSampler) deltaCPU(cpuNS *int64, now time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cpuNS == nil {
		return 0
	}
	prev, seen := s.pending["cpu_ns"]
	lastT, hasT := s.pending["cpu_t"]
	s.pending["cpu_ns"] = float64(*cpuNS)
	s.pending["cpu_t"] = float64(now.UnixNano())
	if !seen || !hasT {
		return 0
	}
	dt := now.UnixNano() - int64(lastT)
	if dt <= 0 {
		return 0
	}
	dCPU := float64(*cpuNS) - prev
	if dCPU < 0 {
		return 0
	}
	ncpu := float64(NumCPU())
	pct := dCPU / float64(dt) * 100 * ncpu // 进程的 CPU 秒 / 墙钟秒 × ncpu = 利用率%
	if pct > 100*ncpu {
		pct = 100 * ncpu
	}
	return pct
}

func (s *ResourceSampler) deltaGC(gc *int64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gc == nil {
		return 0
	}
	prev, seen := s.pending["gc"]
	s.pending["gc"] = float64(*gc)
	if !seen {
		return 0
	}
	d := float64(*gc) - prev
	if d < 0 {
		return 0
	}
	return d
}

func (s *ResourceSampler) deltaGCPause(ns *int64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns == nil {
		return 0
	}
	prev, seen := s.pending["gc_pause"]
	s.pending["gc_pause"] = float64(*ns)
	if !seen {
		return 0
	}
	d := float64(*ns) - prev
	if d < 0 {
		return 0
	}
	return d / 1e6 // ns → ms
}

func (s *ResourceSampler) recordFailure(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.summary.Sampled && s.summary.UnavailableReason == "" {
		s.summary.UnavailableReason = reason
	}
}

// httpBaseOf 把 ws:///wss:///http(s):// 归一成 http(s)://host:port 基址。
func httpBaseOf(target string) string {
	t := strings.TrimSpace(strings.Split(target, ",")[0])
	if strings.HasPrefix(t, "ws://") {
		return "http://" + strings.TrimPrefix(t, "ws://")
	}
	if strings.HasPrefix(t, "wss://") {
		return "https://" + strings.TrimPrefix(t, "wss://")
	}
	if strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") {
		return t
	}
	// 形如 "localhost:8081"
	if _, _, err := net.SplitHostPort(t); err == nil {
		return "http://" + t
	}
	return "http://" + t
}
