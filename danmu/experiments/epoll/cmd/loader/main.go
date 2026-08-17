// loader 是 P8 实验的压测端：拉起被测 WS 服务器（epoll 版或 gorilla 版），
// 并发建立 N 条空闲长连接，测量建连 P99 与服务器进程 RSS。
//
// 用法：
//
//	loader -server-bin ./wsd -server-args "-addr :19000 -stats :19001" \
//	       -ws-url ws://localhost:19000 -conns 10000 -probe 5000
//
// 预注册判据（见 RESULT.md）：RSS 降 ≥40% 且建连 P99 不劣化 → CONTINUE；否则 NEGATIVE。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	serverBin := flag.String("server-bin", "", "被测服务器二进制路径")
	serverArgs := flag.String("server-args", "", "被测服务器参数（空格分隔）")
	wsURL := flag.String("ws-url", "", "WebSocket URL（loader 用 net.Dial 模拟握手，仅需 host:port 语义）")
	conns := flag.Int("conns", 5000, "连接数")
	workers := flag.Int("workers", 100, "并发建连协程数")
	settle := flag.Duration("settle", 10*time.Second, "建连后静置时长（让 RSS 稳定）")
	reqPath := flag.String("req-path", "/ws?uid=bench&room=room-1&token=danmu-secret-token", "WS 握手请求路径（monolith 需要 uid/room/token）")
	flag.Parse()

	if *serverBin == "" || *wsURL == "" {
		log.Fatal("server-bin 与 ws-url 必填")
	}
	host := *wsURL
	host = strings.TrimPrefix(host, "ws://")
	host = strings.TrimPrefix(host, "wss://")
	if i := strings.Index(host, "/"); i >= 0 {
		host = host[:i]
	}

	// 1) 拉起被测服务器
	args := []string{}
	if *serverArgs != "" {
		args = strings.Fields(*serverArgs)
	}
	cmd := exec.Command(*serverBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("start server: %v", err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()
	log.Printf("[loader] server pid=%d", cmd.Process.Pid)

	// 2) 等端口可连
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", host, time.Second)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 3) 并发建连（模拟 WS 握手请求，服务器应答后即空闲）
	var mu sync.Mutex
	var latencies []time.Duration
	var failCount int32
	var wg sync.WaitGroup
	connPool := make([]net.Conn, 0, *conns)

	dialOne := func(i int) {
		defer wg.Done()
		start := time.Now()
		conn, err := net.DialTimeout("tcp", host, 10*time.Second)
		if err != nil {
			mu.Lock()
			failCount++
			mu.Unlock()
			return
		}
		// 发 WS 握手请求
		req := "GET " + *reqPath + " HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(req)); err != nil {
			conn.Close()
			mu.Lock()
			failCount++
			mu.Unlock()
			return
		}
		// 读 101 应答
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		status, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(status, " 101 ") {
			conn.Close()
			mu.Lock()
			failCount++
			mu.Unlock()
			return
		}
		// 排空剩余响应头
		for {
			line, err := reader.ReadString('\n')
			if err != nil || line == "\r\n" || line == "\n" {
				break
			}
		}
		lat := time.Since(start)
		mu.Lock()
		latencies = append(latencies, lat)
		connPool = append(connPool, conn)
		mu.Unlock()
	}

	log.Printf("[loader] dialing %d conns...", *conns)
	startAll := time.Now()
	ch := make(chan struct{})
	go func() {
		for i := 0; i < *conns; i++ {
			ch <- struct{}{}
		}
		close(ch)
	}()
	for w := 0; w < *workers; w++ {
		go func() {
			for range ch {
				wg.Add(1)
				dialOne(0)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(startAll)

	// 4) 静置，读 RSS
	time.Sleep(*settle)
	rssKB := readRSS(cmd.Process.Pid)

	// 5) 报告
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)-1) * q)
		return latencies[idx]
	}
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("SERVER      : %s\n", *serverBin)
	fmt.Printf("CONNS       : %d (ok=%d fail=%d)\n", *conns, len(connPool), failCount)
	fmt.Printf("DIAL RATE   : %.0f conns/s (%.2fs)\n", float64(len(connPool))/elapsed.Seconds(), elapsed.Seconds())
	fmt.Printf("CONNECT P50 : %s\n", p(0.5))
	fmt.Printf("CONNECT P90 : %s\n", p(0.9))
	fmt.Printf("CONNECT P99 : %s\n", p(0.99))
	fmt.Printf("RSS (pid %d): %.1f MB\n", cmd.Process.Pid, float64(rssKB)/1024)
	fmt.Printf("LIVE CONNS  : %d\n", len(connPool))
	fmt.Println(strings.Repeat("=", 60))

	// 6) 报告完毕退出（defer 会 kill server；连接随进程退出关闭）
}

func readRSS(pid int) int64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return -1
}
