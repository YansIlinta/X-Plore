package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gorilla/websocket"
)

// --- 消息结构 ---

type UpMessage struct {
	Type     string `json:"type"`
	Content  string `json:"content"`
	ClientTS int64  `json:"client_ts"`
}

type DownMessage struct {
	Type     string `json:"type"`
	RoomID   string `json:"room_id"`
	UID      string `json:"uid"`
	Content  string `json:"content"`
	ClientTS int64  `json:"client_ts"`
	ServerTS int64  `json:"server_ts"`
}

// --- 指标收集 ---

type Metrics struct {
	// 连接层
	targetConns   int64
	successConns  atomic.Int64
	failedConns   atomic.Int64
	activeConns   atomic.Int64
	connLatencyHR *hdrhistogram.Histogram // 建连耗时（微秒）

	// 吞吐层
	sendCount    atomic.Int64
	recvCount    atomic.Int64
	dropCount    atomic.Int64

	// 延迟层（端到端，微秒）
	e2eLatencyHR *hdrhistogram.Histogram

	// 错误层
	writeErrors atomic.Int64
	readErrors  atomic.Int64
	timeouts    atomic.Int64

	mu sync.Mutex // 保护 histogram 的并发写入
}

func NewMetrics(targetConns int64) *Metrics {
	return &Metrics{
		targetConns:   targetConns,
		connLatencyHR: hdrhistogram.New(1, 60_000_000, 3),   // 1μs ~ 60s
		e2eLatencyHR:  hdrhistogram.New(1, 60_000_000_000, 3), // 1μs ~ 60000s
	}
}

func (m *Metrics) RecordConnLatency(d time.Duration) {
	m.mu.Lock()
	m.connLatencyHR.RecordValue(d.Microseconds())
	m.mu.Unlock()
}

func (m *Metrics) RecordE2ELatency(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	m.mu.Lock()
	m.e2eLatencyHR.RecordValue(us)
	m.mu.Unlock()
}

// --- 每秒统计快照 ---

type Snapshot struct {
	Time         string
	ActiveConns  int64
	SuccessConns int64
	FailedConns  int64
	SendQPS      int64
	RecvQPS      int64
	TotalSend    int64
	TotalRecv    int64
	E2EP50       int64 // μs
	E2EP90       int64
	E2EP99       int64
	WriteErrors  int64
	ReadErrors   int64
	Goroutines   int
	HeapMB       uint64
}

// --- 主程序 ---

func main() {
	servers := flag.String("server", "ws://localhost:8080", "Server URLs (comma separated)")
	conns := flag.Int("conns", 1000, "Number of connections")
	rooms := flag.Int("rooms", 10, "Number of rooms")
	rate := flag.Float64("rate", 1.0, "Messages per second per connection")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	ramp := flag.Duration("ramp", 5*time.Second, "Ramp-up duration for connections")
	token := flag.String("token", "danmu-secret-token", "Auth token")
	pprofAddr := flag.String("pprof", ":6061", "pprof listen address")
	outputJSON := flag.String("output-json", "", "Output JSON report to file")
	outputCSV := flag.String("output-csv", "", "Output CSV report to file")
	flag.Parse()

	serverList := strings.Split(*servers, ",")
	metrics := NewMetrics(int64(*conns))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pprof
	go func() {
		log.Printf("[pprof] listening on %s", *pprofAddr)
		http.ListenAndServe(*pprofAddr, nil)
	}()

	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[loadtest] shutdown signal")
		cancel()
	}()

	// 每秒打印指标
	var snapshots []Snapshot
	var lastSend, lastRecv int64
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curSend := metrics.sendCount.Load()
				curRecv := metrics.recvCount.Load()
				sendQPS := curSend - lastSend
				recvQPS := curRecv - lastRecv
				lastSend = curSend
				lastRecv = curRecv

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)

				metrics.mu.Lock()
				p50 := metrics.e2eLatencyHR.ValueAtPercentile(50)
				p90 := metrics.e2eLatencyHR.ValueAtPercentile(90)
				p99 := metrics.e2eLatencyHR.ValueAtPercentile(99)
				metrics.mu.Unlock()

				snap := Snapshot{
					Time:         time.Now().Format("15:04:05"),
					ActiveConns:  metrics.activeConns.Load(),
					SuccessConns: metrics.successConns.Load(),
					FailedConns:  metrics.failedConns.Load(),
					SendQPS:      sendQPS,
					RecvQPS:      recvQPS,
					TotalSend:    curSend,
					TotalRecv:    curRecv,
					E2EP50:       p50,
					E2EP90:       p90,
					E2EP99:       p99,
					WriteErrors:  metrics.writeErrors.Load(),
					ReadErrors:   metrics.readErrors.Load(),
					Goroutines:   runtime.NumGoroutine(),
					HeapMB:       mem.HeapAlloc / 1024 / 1024,
				}
				snapshots = append(snapshots, snap)

				fmt.Printf("[%s] conns=%d/%d sendQPS=%d recvQPS=%d "+
					"e2e_p50=%dμs p90=%dμs p99=%dμs "+
					"errs(w=%d r=%d) goroutines=%d heap=%dMB\n",
					snap.Time, snap.ActiveConns, metrics.targetConns,
					sendQPS, recvQPS,
					p50, p90, p99,
					snap.WriteErrors, snap.ReadErrors,
					snap.Goroutines, snap.HeapMB)
			}
		}
	}()

	// 分批建连（爬坡）
	var wg sync.WaitGroup
	rampDelay := *ramp / time.Duration(*conns)
	if rampDelay < time.Microsecond {
		rampDelay = time.Microsecond
	}

	log.Printf("[loadtest] starting: conns=%d rooms=%d rate=%.1f/s duration=%s ramp=%s",
		*conns, *rooms, *rate, *duration, *ramp)

	startTime := time.Now()

	for i := 0; i < *conns; i++ {
		select {
		case <-ctx.Done():
			goto waitDone
		default:
		}

		wg.Add(1)
		uid := fmt.Sprintf("bench-%d", i)
		roomID := fmt.Sprintf("room-%d", i%*rooms)
		serverURL := serverList[i%len(serverList)]

		go func(uid, roomID, serverURL string) {
			defer wg.Done()
			runClient(ctx, metrics, serverURL, uid, roomID, *token, *rate, *duration, startTime)
		}(uid, roomID, serverURL)

		time.Sleep(rampDelay)
	}

	// 等待 duration
	select {
	case <-ctx.Done():
	case <-time.After(*duration + *ramp):
	}
	cancel()

waitDone:
	wg.Wait()

	// 打印最终报告
	printReport(metrics, time.Since(startTime))

	// 导出 JSON/CSV
	if *outputJSON != "" {
		exportJSON(*outputJSON, snapshots, metrics)
	}
	if *outputCSV != "" {
		exportCSV(*outputCSV, snapshots)
	}
}

// runClient 单个压测连接
func runClient(ctx context.Context, m *Metrics, serverURL, uid, roomID, token string, rate float64, duration time.Duration, startTime time.Time) {
	// 建连
	wsURL := fmt.Sprintf("%s/ws?uid=%s&room=%s&token=%s", serverURL, uid, roomID, token)

	connStart := time.Now()
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		m.failedConns.Add(1)
		return
	}
	connDur := time.Since(connStart)
	m.successConns.Add(1)
	m.activeConns.Add(1)
	m.RecordConnLatency(connDur)
	defer func() {
		conn.Close()
		m.activeConns.Add(-1)
	}()

	// Pong handler
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	// 读取 goroutine：收消息、统计延迟
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() == nil {
					m.readErrors.Add(1)
				}
				return
			}

			// 解析消息（可能是数组）
			var msgs []DownMessage
			if err := json.Unmarshal(data, &msgs); err != nil {
				// 尝试单条
				var single DownMessage
				if err2 := json.Unmarshal(data, &single); err2 == nil {
					msgs = []DownMessage{single}
				} else {
					continue
				}
			}

			now := time.Now().UnixMilli()
			for _, msg := range msgs {
				m.recvCount.Add(1)
				if msg.ClientTS > 0 {
					latency := now - msg.ClientTS
					if latency > 0 {
						m.RecordE2ELatency(time.Duration(latency) * time.Millisecond)
					}
				}
			}
		}
	}()

	// 发送循环
	if rate > 0 {
		interval := time.Duration(float64(time.Second) / rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		content := fmt.Sprintf("bench msg from %s", uid)

		for {
			select {
			case <-ctx.Done():
				return
			case <-readDone:
				return
			case <-ticker.C:
				if time.Since(startTime) > duration {
					return
				}
				msg := UpMessage{
					Type:     "danmu",
					Content:  content,
					ClientTS: time.Now().UnixMilli(),
				}
				data, _ := json.Marshal(msg)
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					m.writeErrors.Add(1)
					return
				}
				m.sendCount.Add(1)
			}
		}
	} else {
		// rate=0 只接收
		select {
		case <-ctx.Done():
		case <-readDone:
		case <-time.After(duration):
		}
	}
}

// printReport 打印最终汇总报告
func printReport(m *Metrics, elapsed time.Duration) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("                    LOAD TEST REPORT")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\n%-30s %s\n", "Duration:", elapsed.Round(time.Second))
	fmt.Println()

	// 连接层
	fmt.Println("--- Connection ---")
	fmt.Printf("  %-28s %d\n", "Target:", m.targetConns)
	fmt.Printf("  %-28s %d\n", "Success:", m.successConns.Load())
	fmt.Printf("  %-28s %d\n", "Failed:", m.failedConns.Load())
	fmt.Printf("  %-28s %d\n", "Active (final):", m.activeConns.Load())
	fmt.Println()

	fmt.Println("  Connect Latency:")
	fmt.Printf("    P50:  %d μs\n", m.connLatencyHR.ValueAtPercentile(50))
	fmt.Printf("    P90:  %d μs\n", m.connLatencyHR.ValueAtPercentile(90))
	fmt.Printf("    P99:  %d μs\n", m.connLatencyHR.ValueAtPercentile(99))
	fmt.Printf("    Max:  %d μs\n", m.connLatencyHR.Max())
	fmt.Printf("    Mean: %.0f μs\n", m.connLatencyHR.Mean())
	fmt.Println()

	// 吞吐层
	fmt.Println("--- Throughput ---")
	totalSend := m.sendCount.Load()
	totalRecv := m.recvCount.Load()
	elapsedSec := elapsed.Seconds()
	fmt.Printf("  %-28s %d (%.0f/s)\n", "Total Sent:", totalSend, float64(totalSend)/elapsedSec)
	fmt.Printf("  %-28s %d (%.0f/s)\n", "Total Received:", totalRecv, float64(totalRecv)/elapsedSec)
	fmt.Printf("  %-28s %d\n", "Dropped:", m.dropCount.Load())
	fmt.Println()

	// 延迟层
	fmt.Println("--- End-to-End Latency ---")
	if m.e2eLatencyHR.TotalCount() > 0 {
		fmt.Printf("  P50:   %d μs (%.1f ms)\n", m.e2eLatencyHR.ValueAtPercentile(50), float64(m.e2eLatencyHR.ValueAtPercentile(50))/1000)
		fmt.Printf("  P90:   %d μs (%.1f ms)\n", m.e2eLatencyHR.ValueAtPercentile(90), float64(m.e2eLatencyHR.ValueAtPercentile(90))/1000)
		fmt.Printf("  P99:   %d μs (%.1f ms)\n", m.e2eLatencyHR.ValueAtPercentile(99), float64(m.e2eLatencyHR.ValueAtPercentile(99))/1000)
		fmt.Printf("  P999:  %d μs (%.1f ms)\n", m.e2eLatencyHR.ValueAtPercentile(99.9), float64(m.e2eLatencyHR.ValueAtPercentile(99.9))/1000)
		fmt.Printf("  Max:   %d μs (%.1f ms)\n", m.e2eLatencyHR.Max(), float64(m.e2eLatencyHR.Max())/1000)
		fmt.Printf("  Mean:  %.0f μs (%.1f ms)\n", m.e2eLatencyHR.Mean(), m.e2eLatencyHR.Mean()/1000)
	} else {
		fmt.Println("  (no latency data)")
	}
	fmt.Println()

	// 错误层
	fmt.Println("--- Errors ---")
	fmt.Printf("  %-28s %d\n", "Connect Failed:", m.failedConns.Load())
	fmt.Printf("  %-28s %d\n", "Write Errors:", m.writeErrors.Load())
	fmt.Printf("  %-28s %d\n", "Read Errors:", m.readErrors.Load())
	fmt.Printf("  %-28s %d\n", "Timeouts:", m.timeouts.Load())
	fmt.Println()

	// 资源层
	fmt.Println("--- Resources (loadtest machine) ---")
	fmt.Printf("  %-28s %d\n", "Goroutines:", runtime.NumGoroutine())
	fmt.Printf("  %-28s %d MB\n", "Heap Alloc:", mem.HeapAlloc/1024/1024)
	fmt.Printf("  %-28s %d\n", "GC Cycles:", mem.NumGC)
	fmt.Printf("  %-28s %s\n", "GC Pause Total:", time.Duration(mem.PauseTotalNs))
	fmt.Println(strings.Repeat("=", 70))
}

// exportJSON 导出 JSON 报告
func exportJSON(path string, snapshots []Snapshot, m *Metrics) {
	m.mu.Lock()
	defer m.mu.Unlock()

	report := map[string]interface{}{
		"summary": map[string]interface{}{
			"target_conns":  m.targetConns,
			"success_conns": m.successConns.Load(),
			"failed_conns":  m.failedConns.Load(),
			"total_sent":    m.sendCount.Load(),
			"total_recv":    m.recvCount.Load(),
			"e2e_p50_us":    m.e2eLatencyHR.ValueAtPercentile(50),
			"e2e_p90_us":    m.e2eLatencyHR.ValueAtPercentile(90),
			"e2e_p99_us":    m.e2eLatencyHR.ValueAtPercentile(99),
			"e2e_p999_us":   m.e2eLatencyHR.ValueAtPercentile(99.9),
			"e2e_max_us":    m.e2eLatencyHR.Max(),
		},
		"snapshots": snapshots,
	}

	data, _ := json.MarshalIndent(report, "", "  ")
	os.WriteFile(path, data, 0644)
	log.Printf("[loadtest] JSON report written to %s", path)
}

// exportCSV 导出 CSV 报告
func exportCSV(path string, snapshots []Snapshot) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("[loadtest] CSV export error: %v", err)
		return
	}
	defer f.Close()

	fmt.Fprintln(f, "time,active_conns,success_conns,failed_conns,send_qps,recv_qps,total_send,total_recv,e2e_p50_us,e2e_p90_us,e2e_p99_us,write_errors,read_errors,goroutines,heap_mb")
	for _, s := range snapshots {
		fmt.Fprintf(f, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			s.Time, s.ActiveConns, s.SuccessConns, s.FailedConns,
			s.SendQPS, s.RecvQPS, s.TotalSend, s.TotalRecv,
			s.E2EP50, s.E2EP90, s.E2EP99,
			s.WriteErrors, s.ReadErrors, s.Goroutines, s.HeapMB)
	}
	log.Printf("[loadtest] CSV report written to %s", path)
}

// init 初始化随机数种子
func init() {
	rand.Seed(time.Now().UnixNano())
}
