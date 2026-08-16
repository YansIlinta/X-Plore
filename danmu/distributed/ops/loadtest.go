package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Loadtest 集成：复用 ../monolith/loadtest 二进制（package main，只能子进程调用）。
// ops 只做"启动/观察/停止"，不复制任何压测逻辑。
//
// 安全边界：这是 ACTION 类接口（区别于其他只读 API）。只允许一个压测同时进行；
// 二进制不存在时返回 503 并说明，绝不假装在压测。

// loadtest 每秒快照行：
// [15:04:05] conns=1900/2000 sendQPS=3800 recvQPS=3750 e2e_p50=600μs p90=1100μs p99=1700μs errs(w=0 r=2) goroutines=3891 heap=42MB
var ltLineRe = regexp.MustCompile(
	`^\[[\d:]+\] conns=(\d+)/(\d+) sendQPS=(\d+) recvQPS=(\d+) e2e_p50=(\d+)μs p90=(\d+)μs p99=(\d+)μs errs\(w=(\d+) r=(\d+)\) goroutines=(\d+) heap=(\d+)MB`)

type loadtestManager struct {
	bin   string
	token string

	mu        sync.Mutex
	running   bool
	available bool // 二进制是否存在
	params    map[string]any
	startedAt time.Time
	latest    map[string]any // 最近一行快照解析结果
	report    map[string]any // 结束后 --output-json 的完整报告
	err       string
	cancel    context.CancelFunc
}

// NewLoadtestManager 构造压测管理器。bin 是相对/绝对路径或 PATH 里的可执行名；
// 不存在时 available=false，Start 会返回明确错误。
// Windows 特例：go build -o bin/loadtest 产出的无扩展名 PE 文件 os.Stat 可见，
// 但 exec 因没有可执行扩展名拒绝运行（Go 的 LookPath 要求 .exe 等 PATHEXT 扩展名），
// 所以这里先归一成绝对路径，并对 bin+".exe" 做一次兜底探测。
func NewLoadtestManager(bin, token string) *loadtestManager {
	m := &loadtestManager{bin: bin, token: token}
	if abs, err := filepath.Abs(bin); err == nil {
		m.bin = abs
	}
	for _, cand := range []string{m.bin, m.bin + ".exe"} {
		if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
			m.bin, m.available = cand, true
			break
		}
	}
	if !m.available {
		if p, err := exec.LookPath(bin); err == nil {
			m.bin, m.available = p, true
		}
	}
	return m
}

// Start 启动压测子进程。已在运行则报 409。
func (m *loadtestManager) Start(params map[string]any) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("loadtest already running")
	}
	if !m.available {
		m.mu.Unlock()
		return fmt.Errorf("loadtest binary not found: %s", m.bin)
	}
	m.mu.Unlock()

	server, _ := params["server"].(string)
	if server == "" {
		server = "ws://localhost:8080"
	}
	conns := numOr(params["conns"], 1000)
	rooms := numOr(params["rooms"], 10)
	rate := floatOr(params["rate"], 1)
	duration, _ := params["duration"].(string)
	if duration == "" {
		duration = "30s"
	}
	token, _ := params["token"].(string)
	if token == "" {
		token = m.token
	}

	reportPath := filepath.Join(os.TempDir(), fmt.Sprintf("danmu-loadtest-%d.json", time.Now().Unix()))
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, m.bin,
		"-server", server,
		"-conns", strconv.Itoa(conns),
		"-rooms", strconv.Itoa(rooms),
		"-rate", strconv.FormatFloat(rate, 'f', -1, 64),
		"-duration", duration,
		"-token", token,
		"-output-json", reportPath,
	)

	var out bytes.Buffer
	cmd.Stdout = &lineScanWriter{onLine: func(line string) {
		if snap := parseLoadtestLine(line); snap != nil {
			m.mu.Lock()
			m.latest = snap
			m.mu.Unlock()
		}
	}}
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		cancel()
		return err
	}

	m.mu.Lock()
	m.running = true
	m.params = map[string]any{"server": server, "conns": conns, "rooms": rooms, "rate": rate, "duration": duration}
	m.startedAt = time.Now()
	m.latest = nil
	m.report = nil
	m.err = ""
	m.cancel = cancel
	m.mu.Unlock()

	go func() {
		err := cmd.Wait()
		// 进程结束：读 JSON 报告归档
		var report map[string]any
		if data, rerr := os.ReadFile(reportPath); rerr == nil {
			_ = json.Unmarshal(data, &report)
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.running = false
		m.cancel = nil
		if err != nil && ctx.Err() == nil {
			m.err = fmt.Sprintf("exit: %v; stderr tail: %s", err, tail(out.String(), 300))
		}
		if report != nil {
			m.report = report
		}
	}()
	return nil
}

// Stop 终止正在运行的压测。
func (m *loadtestManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
}

// Status 返回当前/最近一次压测的状态。
func (m *loadtestManager) Status() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var elapsed *float64
	if !m.startedAt.IsZero() {
		e := time.Since(m.startedAt).Seconds()
		elapsed = &e
	}
	return map[string]any{
		"available":  m.available,
		"running":    m.running,
		"params":     m.params,
		"started_at": m.startedAt,
		"elapsed_s":  elapsed,
		"latest":     m.latest,
		"report":     m.report,
		"err":        m.err,
	}
}

// parseLoadtestLine 解析一行秒级快照；不匹配返回 nil（启动横幅、报告段等行忽略）。
func parseLoadtestLine(line string) map[string]any {
	m := ltLineRe.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	atoi := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
	return map[string]any{
		"active_conns": atoi(m[1]), "target_conns": atoi(m[2]),
		"send_qps": atoi(m[3]), "recv_qps": atoi(m[4]),
		"e2e_p50_us": atoi(m[5]), "e2e_p90_us": atoi(m[6]), "e2e_p99_us": atoi(m[7]),
		"write_errors": atoi(m[8]), "read_errors": atoi(m[9]),
		"goroutines": atoi(m[10]), "heap_mb": atoi(m[11]),
	}
}

// lineScanWriter 按行切分压测 stdout，整行交给回调。
type lineScanWriter struct {
	buf    []byte
	onLine func(string)
}

func (w *lineScanWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.onLine(string(w.buf[:i]))
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func numOr(v any, def int) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return def
}

func floatOr(v any, def float64) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	}
	return def
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
