package ops

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// --- 受控 server 进程（Phase 1.5，system-config sweep 用）---
//
// 安全边界：这是对 loadtestManager（既有"安全固定 argv 子进程"模式）的同类扩展。
//   - 只有 monolith server 一个受控二进制；argv 完全固定、不经过 shell。
//   - 只允许 SystemConfig 白名单参数（batch-size / batch-timeout / workers），
//     取值由 SystemConfig.Validate 约束。
//   - server 以 -mq=none 启动（无中间件，本机广播），并显式禁用/分离 pprof。
//   - 一个进程管理器同时最多管理一个被控 server；Ensure 换配置先 Release 旧的。
//   - 进程随 ops 生命周期 ctx 或 Release 退出；避免孤儿进程。
//
// 用途：让 batch_size / batch_timeout 这类 *需要重启* 的系统参数也能被真正 sweep。

// managedServerArgs 是被控 server 允许的全部 flag（白名单，防任意 argv）。
// 每个 flag 的取值只能来自 SystemConfig。这不是任意命令通道。
const (
	managedBinDefault = "bin/server"
)

// ServerProcessManager 管理一个受控的 monolith server 进程。
type ServerProcessManager struct {
	mu    sync.Mutex
	bin   string
	token string
	ctx   context.Context

	proc   *os.Process
	port   int          // 当前受控进程监听端口；0 = 无
	sys    SystemConfig // 当前受控进程应用的系统配置
	stdout *strings.Builder
}

// NewServerProcessManager 构造受控 server 管理器。bin 是 monolith server 二进制路径；
// token 用于 stats 端点鉴权；ctx 为 nil 时使用 background（仅靠 Release 停止）。
func NewServerProcessManager(bin, token string, ctx context.Context) *ServerProcessManager {
	if ctx == nil {
		ctx = context.Background()
	}
	m := &ServerProcessManager{bin: bin, token: token, ctx: ctx, stdout: &strings.Builder{}}
	if m.bin == "" {
		m.bin = managedBinDefault
	}
	go func() {
		<-ctx.Done()
		m.Release()
	}()
	return m
}

// Available 报告被控 server 二进制是否存在。
func (m *ServerProcessManager) Available() bool {
	if m == nil {
		return false
	}
	if _, err := os.Stat(m.bin); err == nil || m.bin == managedBinDefault {
		return true
	}
	if _, err := exec.LookPath(m.bin); err == nil {
		return true
	}
	return false
}

// Ensure 确保存在一个监听 target（ws://host:port）且应用了 sys 配置的受控 server。
// 若端口/配置与当前进程不同，先 Release 旧进程再启动新进程。
func (m *ServerProcessManager) Ensure(target string, sys SystemConfig) error {
	port, err := portOfTarget(target)
	if err != nil {
		return fmt.Errorf("controlled server target %q: %w", target, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil && m.port == port && m.sys == sys {
		return nil // 已就绪，复用
	}
	if err := m.releaseLocked(); err != nil {
		return err
	}
	return m.startLocked(port, sys)
}

// startLocked 启动一个新 server 进程并等待其 stats 端点可达。
func (m *ServerProcessManager) startLocked(port int, sys SystemConfig) error {
	if _, err := os.Stat(m.bin); err != nil {
		return fmt.Errorf("controlled server binary not found: %s", m.bin)
	}
	argv := []string{
		"-addr", "127.0.0.1:" + strconv.Itoa(port),
		"-id", "ctl" + strconv.Itoa(port),
		"-mq", "none",
		"-pprof", "127.0.0.1:0",
	}
	// 只透传显式设置的白名单系统参数；0/"" = server 默认。
	if sys.BatchSize > 0 {
		argv = append(argv, "-batch-size", strconv.Itoa(sys.BatchSize))
	}
	if sys.BatchTimeout != "" {
		argv = append(argv, "-batch-timeout", sys.BatchTimeout)
	}
	if sys.Workers > 0 {
		argv = append(argv, "-workers", strconv.Itoa(sys.Workers))
	}

	cmd := exec.CommandContext(m.ctx, m.bin, argv...)
	cmd.Env = append(os.Environ(), "DANMU_AUTH_TOKEN="+m.token)
	cmd.Stdout = m.stdout
	cmd.Stderr = m.stdout
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start controlled server: %w", err)
	}
	m.proc = cmd.Process
	m.port = port
	m.sys = sys

	if err := m.waitHealthy(port); err != nil {
		m.releaseLocked()
		return err
	}
	log.Printf("[ops] controlled server started on :%d (sys=%s)", port, sys.Label())
	return nil
}

// waitHealthy 轮询 stats 端点直到可服务。
func (m *ServerProcessManager) waitHealthy(port int) error {
	deadline := time.Now().Add(10 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/api/v1/stats", port), nil)
		if m.token != "" {
			req.Header.Set("Authorization", "Bearer "+m.token)
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("controlled server on :%d did not become healthy (stdout tail: %s)", port, tail(m.stdout.String(), 300))
}

// Release 停止当前受控 server 进程（幂等）。
func (m *ServerProcessManager) Release() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.releaseLocked()
}

func (m *ServerProcessManager) releaseLocked() error {
	if m.proc == nil {
		m.port = 0
		m.sys = SystemConfig{}
		return nil
	}
	proc := m.proc
	m.proc = nil
	m.port = 0
	m.sys = SystemConfig{}
	_ = proc.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_, _ = proc.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = proc.Kill()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	return nil
}

// portOfTarget 从 ws://host:port 提取端口。
func portOfTarget(target string) (int, error) {
	t := strings.TrimSpace(strings.Split(target, ",")[0])
	t = strings.TrimPrefix(strings.TrimPrefix(t, "wss://"), "ws://")
	_, portStr, err := net.SplitHostPort(t)
	if err != nil {
		return 0, fmt.Errorf("cannot parse port from %q", target)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("invalid port %q in target %q", portStr, target)
	}
	return port, nil
}
