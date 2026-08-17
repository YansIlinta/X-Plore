// loader 是 P8 实验的压测端：拉起被测 WS 服务器（epoll 版或 gorilla 版），
// 并发建立 N 条空闲长连接，测量建连 P99 与服务器进程 RSS。
//
// 多端口：-ports N 时连接轮询分布到 base-port..base-port+N-1（绕开客户端单 IP
// 临时端口上限 28k：同一源端口对不同目标端口可并存，四元组唯一）。
//
// 用法：
//
//	# epoll 版（loader 负责拉起单进程多端口 server）
//	loader -server-bin ./wsd -server-args "-addr :19000 -ports 64" \
//	       -conns 100000 -ports 64
//
//	# gorilla 基线（多实例由 wrapper 脚本拉起，loader 只负责建连与测 RSS）
//	loader -no-spawn -pids-file /tmp/srv.pids -conns 100000 -ports 64
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
	serverBin := flag.String("server-bin", "", "被测服务器二进制路径（-no-spawn 时忽略）")
	serverArgs := flag.String("server-args", "", "被测服务器参数（空格分隔）")
	host := flag.String("host", "localhost", "被测服务器主机")
	basePort := flag.Int("base-port", 19000, "被测服务器起始端口")
	ports := flag.Int("ports", 1, "被测服务器端口数（连接轮询分布，绕开单 IP 临时端口上限）")
	conns := flag.Int("conns", 5000, "连接数")
	workers := flag.Int("workers", 200, "并发建连协程数")
	settle := flag.Duration("settle", 10*time.Second, "建连后静置时长（让 RSS 稳定）")
	reqPath := flag.String("req-path", "/ws?uid=bench&room=room-1&token=danmu-secret-token", "WS 握手请求路径（monolith 需要 uid/room/token）")
	noSpawn := flag.Bool("no-spawn", false, "不拉起 server（已由外部启动），仅建连与测 RSS")
	pidsFile := flag.String("pids-file", "", "服务器进程 pid 列表文件（逗号/换行分隔，-no-spawn 时用于 RSS 汇总）")
	flag.Parse()

	if !*noSpawn && *serverBin == "" {
		log.Fatal("server-bin 必填（或 -no-spawn）")
	}

	// 1) 拉起被测服务器（可选）
	var spawnedPid int
	if !*noSpawn {
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
		spawnedPid = cmd.Process.Pid
		log.Printf("[loader] server pid=%d", spawnedPid)
	}

	// 2) 等端口可连
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", *host, *basePort), time.Second)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 3) 并发建连（模拟 WS 握手请求，服务器应答后即空闲），连接轮询分布到各端口
	var mu sync.Mutex
	var latencies []time.Duration
	var failCount int32
	var wg sync.WaitGroup
	connPool := make([]net.Conn, 0, *conns)

	dialOne := func(i int) {
		defer wg.Done()
		dstPort := *basePort + i%*ports
		start := time.Now()
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", *host, dstPort), 10*time.Second)
		if err != nil {
			mu.Lock()
			failCount++
			mu.Unlock()
			return
		}
		// 发 WS 握手请求
		req := "GET " + *reqPath + " HTTP/1.1\r\nHost: " + fmt.Sprintf("%s:%d", *host, dstPort) + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write([]byte(req)); err != nil {
			conn.Close()
			mu.Lock()
			failCount++
			mu.Unlock()
			return
		}
		// 读 101 应答
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
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

	log.Printf("[loader] dialing %d conns across %d ports...", *conns, *ports)
	startAll := time.Now()
	ch := make(chan int, *workers*4)
	go func() {
		for i := 0; i < *conns; i++ {
			ch <- i
		}
		close(ch)
	}()
	for w := 0; w < *workers; w++ {
		go func() {
			for i := range ch {
				wg.Add(1)
				dialOne(i)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(startAll)

	// 4) 静置，读 RSS
	time.Sleep(*settle)
	var rssKB int64
	if *noSpawn {
		for _, pid := range readPIDs(*pidsFile) {
			if v := readRSS(pid); v > 0 {
				rssKB += v
			}
		}
	} else {
		rssKB = readRSS(spawnedPid)
	}

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
	fmt.Printf("SERVER      : %s\n", serverBinOrPids(*noSpawn, *pidsFile))
	fmt.Printf("CONNS       : %d (ok=%d fail=%d)\n", *conns, len(connPool), failCount)
	fmt.Printf("DIAL RATE   : %.0f conns/s (%.2fs)\n", float64(len(connPool))/elapsed.Seconds(), elapsed.Seconds())
	fmt.Printf("CONNECT P50 : %s\n", p(0.5))
	fmt.Printf("CONNECT P90 : %s\n", p(0.9))
	fmt.Printf("CONNECT P99 : %s\n", p(0.99))
	if rssKB >= 0 {
		fmt.Printf("RSS         : %.1f MB\n", float64(rssKB)/1024)
	} else {
		fmt.Printf("RSS         : N/A\n")
	}
	fmt.Printf("LIVE CONNS  : %d\n", len(connPool))
	fmt.Println(strings.Repeat("=", 60))
}

func serverBinOrPids(noSpawn bool, pidsFile string) string {
	if !noSpawn {
		return "spawned"
	}
	return "external (pids-file " + pidsFile + ")"
}

func readPIDs(path string) []int {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var pids []int
	for _, tok := range strings.FieldsFunc(string(data), func(r rune) bool {
		return r == ',' || r == '\n' || r == ' '
	}) {
		if v, err := strconv.Atoi(tok); err == nil {
			pids = append(pids, v)
		}
	}
	return pids
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
