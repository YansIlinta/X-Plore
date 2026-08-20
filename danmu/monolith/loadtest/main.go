package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sort"
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
	Type         string `json:"type"`
	Content      string `json:"content"`
	ClientTS     int64  `json:"client_ts"`
	ClientTSNano int64  `json:"client_ts_ns"`
	MsgID        string `json:"msg_id,omitempty"`   // 幂等 ID（服务端回 ack）
	Priority     int    `json:"priority,omitempty"` // 0=普通，1=高优先级
}

// scanFrame 快速扫描一帧（JSON 数组或单对象），提取压测统计量：
//   - danmu：弹幕消息数（"type":"danmu" 出现次数）
//   - ack  ：服务端 ack 数（"type":"ack" 出现次数）
//   - high ：高优消息数（"priority":1 出现次数）
//   - nanos：每条消息的 client_ts_ns 值（追加到传入切片以复用缓冲）
//   - seqs ：每条弹幕消息的 room seq 值（仅 wantSeq=true 时做第二遍扫描）
//
// 正确性前提：服务端 json 序列化字段顺序稳定（Type 在首）、内容字段被 JSON
// 转义（用户文本里的 "type":"danmu" 不会误匹配）。seq 只在广播弹幕消息上出现
// （ack 只有 msg_id；reconnect 的 after_seq 带下划线，不会误匹配 "seq":）。
// 字节扫描比整帧 json.Unmarshal 快一个量级，是万级连接满扇出下客户端能跟上投递的关键。
func scanFrame(data []byte, nanos, seqs []int64, wantSeq bool) (danmu, ack, high int, out []int64, outSeqs []int64) {
	danmu = bytes.Count(data, []byte(`"type":"danmu"`))
	ack = bytes.Count(data, []byte(`"type":"ack"`))
	high = bytes.Count(data, []byte(`"priority":1`))
	out = nanos[:0]
	outSeqs = seqs[:0]
	rest := data
	for {
		idx := bytes.Index(rest, []byte(`"client_ts_ns":`))
		if idx < 0 {
			break
		}
		rest = rest[idx+len(`"client_ts_ns":`):]
		var v int64
		j := 0
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			v = v*10 + int64(rest[j]-'0')
			j++
		}
		out = append(out, v)
		rest = rest[j:]
	}
	if wantSeq {
		rest = data
		for {
			idx := bytes.Index(rest, []byte(`"seq":`))
			if idx < 0 {
				break
			}
			rest = rest[idx+len(`"seq":`):]
			var v int64
			j := 0
			for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
				v = v*10 + int64(rest[j]-'0')
				j++
			}
			outSeqs = append(outSeqs, v)
			rest = rest[j:]
		}
	}
	return
}

// deliveryTracker 是单连接的 seq 连续性跟踪器（-delivery-check 用）。
// 语义：期望 nextSeq = lastSeq+1。缺口 = 服务端广播了但该连接没收到、
// 或出现顺序跳跃（丢消息）。replay（seq <= lastSeq）不计缺口不计数。
type deliveryTracker struct {
	epoch      uint64
	lastSeq    int64
	contiguous int64
	gaps       int64
}

// onSeqs 处理一帧内的若干弹幕消息 seq，返回 (累计连续投递数, 累计缺口数)。
func (t *deliveryTracker) onSeqs(ep uint64, seqs []int64) (int64, int64) {
	if ep != t.epoch {
		// epoch 变化 = 测量窗口重置（warm-up → measurement）。
		t.epoch = ep
		t.lastSeq = 0
		t.contiguous = 0
		t.gaps = 0
	}
	for _, s := range seqs {
		if t.lastSeq == 0 {
			t.contiguous++
			t.lastSeq = s
			continue
		}
		switch {
		case s == t.lastSeq+1:
			t.contiguous++
			t.lastSeq = s
		case s > t.lastSeq+1:
			t.gaps += s - t.lastSeq - 1 // 缺口：被跳过的 seq 数
			t.contiguous++
			t.lastSeq = s
		default:
			// replay / stale：忽略（不判缺口，不计数）。
		}
	}
	return t.contiguous, t.gaps
}

// Metrics 收集压测期间的指标。

// --- 指标收集 ---

// e2eShard 端到端延迟直方图的一个分片。hdrhistogram 非线程安全，需加锁；
// 但万级连接下若共用一把锁，数百万次记录会严重串行化——既拖垮读取吞吐、
// 又把测得的延迟灌水（消息在缓冲里等锁的时间被算进延迟）。故按连接分片，
// 每片独立锁，report 时合并。
type e2eShard struct {
	mu sync.Mutex
	h  *hdrhistogram.Histogram
}

type Metrics struct {
	// 连接层
	targetConns   int64
	successConns  atomic.Int64
	failedConns   atomic.Int64
	activeConns   atomic.Int64
	connLatencyHR *hdrhistogram.Histogram // 建连耗时（微秒），每连接仅一次，低频，共用 mu 即可

	// 吞吐层
	sendCount atomic.Int64
	recvCount atomic.Int64
	dropCount atomic.Int64

	// 可靠性层（P3：服务端 ack / 高优先级通道）
	ackCount atomic.Int64 // 收到的服务端 ack 数（按发送 msg_id 计数）
	sentHigh atomic.Int64 // 发送的高优先级消息数
	recvHigh atomic.Int64 // 收到的高优先级消息数（含自己回声）

	// 投递核算（-delivery-check）：按连接 seq 连续性。
	deliveryObserved atomic.Int64
	deliveryMissing  atomic.Int64
	deliveryEnabled  bool

	// 延迟层（端到端，微秒），分片降低锁竞争
	e2eShardsAtomic atomic.Value // 持有一个 []*e2eShard（measureStart 重置时整体替换）

	// 错误层
	writeErrors atomic.Int64
	readErrors  atomic.Int64
	timeouts    atomic.Int64

	// 测量窗基线（measureStart 时快照，报告用 Window 值）
	baseSent atomic.Int64
	baseRecv atomic.Int64
	baseAck  atomic.Int64
	baseDrop atomic.Int64
	baseWE   atomic.Int64
	baseRE   atomic.Int64
	epoch    atomic.Uint64 // warm-up 结束 / 测量开始 = epoch+1

	mu sync.Mutex // 仅保护 connLatencyHR
}

func NewMetrics(targetConns int64, deliveryEnabled bool) *Metrics {
	nShards := runtime.NumCPU()
	if nShards < 8 {
		nShards = 8
	}
	if nShards > 256 {
		nShards = 256
	}
	shards := make([]*e2eShard, nShards)
	for i := range shards {
		shards[i] = &e2eShard{h: hdrhistogram.New(1, 60_000_000_000, 3)} // 1μs ~ 60000s
	}
	m := &Metrics{
		targetConns:     targetConns,
		connLatencyHR:   hdrhistogram.New(1, 60_000_000, 3), // 1μs ~ 60s
		deliveryEnabled: deliveryEnabled,
	}
	m.e2eShardsAtomic.Store(shards)
	return m
}

func (m *Metrics) RecordConnLatency(d time.Duration) {
	m.mu.Lock()
	m.connLatencyHR.RecordValue(d.Microseconds())
	m.mu.Unlock()
}

// RecordE2ELatency 按 shardIdx 落到某个分片记录，避免全局锁竞争
func (m *Metrics) RecordE2ELatency(shardIdx int, d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	shards := m.e2eShardsAtomic.Load().([]*e2eShard)
	s := shards[shardIdx%len(shards)]
	s.mu.Lock()
	s.h.RecordValue(us)
	s.mu.Unlock()
}

// e2eMerged 合并所有分片为一个直方图，供快照/报告读取分位数
func (m *Metrics) e2eMerged() *hdrhistogram.Histogram {
	merged := hdrhistogram.New(1, 60_000_000_000, 3)
	for _, s := range m.e2eShardsAtomic.Load().([]*e2eShard) {
		s.mu.Lock()
		merged.Merge(s.h)
		s.mu.Unlock()
	}
	return merged
}

// ResetMeasurement 在 measureStart（warm-up 结束）把测量基线清零：
// 记录计数器基线、替换 e2e 直方图、推进 epoch。warm-up 段数据不再计入最终统计。
func (m *Metrics) ResetMeasurement() {
	m.baseSent.Store(m.sendCount.Load())
	m.baseRecv.Store(m.recvCount.Load())
	m.baseAck.Store(m.ackCount.Load())
	m.baseDrop.Store(m.dropCount.Load())
	m.baseWE.Store(m.writeErrors.Load())
	m.baseRE.Store(m.readErrors.Load())
	m.epoch.Add(1)
	nShards := runtime.NumCPU()
	if nShards < 8 {
		nShards = 8
	}
	if nShards > 256 {
		nShards = 256
	}
	shards := make([]*e2eShard, nShards)
	for i := range shards {
		shards[i] = &e2eShard{h: hdrhistogram.New(1, 60_000_000_000, 3)}
	}
	m.e2eShardsAtomic.Store(shards)
}

// WindowSent / WindowRecv 返回测量窗内的真实计数（非累计）。
func (m *Metrics) WindowSent() int64 { return m.sendCount.Load() - m.baseSent.Load() }
func (m *Metrics) WindowRecv() int64 { return m.recvCount.Load() - m.baseRecv.Load() }
func (m *Metrics) WindowDrop() int64 { return m.dropCount.Load() - m.baseDrop.Load() }
func (m *Metrics) WindowWE() int64   { return m.writeErrors.Load() - m.baseWE.Load() }
func (m *Metrics) WindowRE() int64   { return m.readErrors.Load() - m.baseRE.Load() }

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

// runSalt 是本进程一次运行的唯一后缀。没有它，连续两次压测会复用相同的
// msg_id（bench-N-seq），落在服务端 30s 幂等窗口内被当作重复、只回 ack 不广播，
// 导致第二次压测 recv=0 —— 这是重复基准的核心绊脚石。
var runSalt = newRunSalt()

func newRunSalt() string {
	b := make([]byte, 4)
	if _, err := cryptorand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func main() {
	servers := flag.String("server", "ws://localhost:8080", "Server URLs (comma separated)")
	conns := flag.Int("conns", 1000, "Number of connections")
	rooms := flag.Int("rooms", 10, "Number of rooms")
	rate := flag.Float64("rate", 1.0, "Messages per second per connection")
	duration := flag.Duration("duration", 30*time.Second, "Measurement window duration")
	ramp := flag.Duration("ramp", 5*time.Second, "Ramp-up duration for connections")
	token := flag.String("token", "danmu-secret-token", "Auth token")
	pprofAddr := flag.String("pprof", ":6061", "pprof listen address")
	outputJSON := flag.String("output-json", "", "Output JSON report to file")
	outputCSV := flag.String("output-csv", "", "Output CSV report to file")
	reconnectCheck := flag.Bool("reconnect-check", false, "运行重连补发校验（单连接短场景）：校验通过/失败后退出")
	priorityRatio := flag.Float64("priority-ratio", 0, "发送消息中高优先级(priority=1)的比例 0~1（0=全部普通）")
	reauthEvery := flag.Duration("reauth-every", 8*time.Minute, "会话令牌续期间隔（需小于服务端 -session-ttl 默认 10min，否则长跑会被 4008 断开）")
	noReauth := flag.Bool("no-reauth", false, "禁用会话续期（短时压测用）")
	warmup := flag.Duration("warmup", 0, "Warm-up duration：期间发送流量但不计入测量（measureStart 重置计数器/直方图）")
	dist := flag.String("dist", "uniform", "Room popularity distribution: uniform | hot_room | zipf")
	zipfS := flag.Float64("zipf-s", 1.1, "zipf 分布的 s 参数（>0；越大越集中）")
	seed := flag.Int64("seed", 1, "Deterministic random seed（相同 seed 产出相同房间分配）")
	deliveryCheck := flag.Bool("delivery-check", false, "开启 per-connection seq 缺口投递核算（drops/delivery_rate 才有真实值）")
	flag.Parse()

	serverList := strings.Split(*servers, ",")
	metrics := NewMetrics(int64(*conns), *deliveryCheck)

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

	// 重连补发校验模式：验证服务端「热历史 + after_seq 缺口补发」的验收场景
	if *reconnectCheck {
		if err := runReconnectCheck(serverList[0], *token); err != nil {
			log.Fatalf("[reconnect-check] FAIL: %v", err)
		}
		log.Println("[reconnect-check] PASS")
		return
	}

	// 测量窗口：warm-up 时段发送流量（让系统满负荷预热）但统计数据全被归零；
	// measureStart 后开始计量；sendEnd 停止发送。总挂起时间 = warmup+duration+ramp。
	measureStart := time.Now().Add(*warmup)
	sendEnd := measureStart.Add(*duration)
	log.Printf("[loadtest] measurement window: warmup=%s measureStart=%s duration=%s sendEnd=%s",
		*warmup, measureStart.Format(time.RFC3339), *duration, sendEnd.Format(time.RFC3339))
	// 测量窗重置：恰在 warm-up 结束后清零基线 + 换直方图（边界后流量才计入测量）。
	if *warmup > 0 {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(measureStart)):
			}
			metrics.ResetMeasurement()
			log.Println("[loadtest] warm-up ended; measurement window started")
		}()
	}

	// 每秒打印指标（从测量窗开始才打印，快照标测量窗内值）
	var snapshots []Snapshot
	var lastSend, lastRecv int64
	go func() {
		if *warmup > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(measureStart)):
			}
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curSend := metrics.WindowSent()
				curRecv := metrics.WindowRecv()
				sendQPS := curSend - lastSend
				recvQPS := curRecv - lastRecv
				lastSend = curSend
				lastRecv = curRecv

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)

				e2e := metrics.e2eMerged()
				p50 := e2e.ValueAtPercentile(50)
				p90 := e2e.ValueAtPercentile(90)
				p99 := e2e.ValueAtPercentile(99)

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

	// 分批建连（爬坡）；房间分配由分布 + seed 决定（确定性）。
	var wg sync.WaitGroup
	rampDelay := *ramp / time.Duration(*conns)
	if rampDelay < time.Microsecond {
		rampDelay = time.Microsecond
	}
	roomAssign2 := roomAssignFor(*conns, *rooms, *dist, *zipfS, *seed)

	log.Printf("[loadtest] starting: conns=%d rooms=%d rate=%.1f/s warmup=%s measure=%s ramp=%s dist=%s seed=%d",
		*conns, *rooms, *rate, *warmup, *duration, *ramp, *dist, *seed)

	startTime := time.Now()

	for i := 0; i < *conns; i++ {
		select {
		case <-ctx.Done():
			goto waitDone
		default:
		}

		wg.Add(1)
		uid := fmt.Sprintf("bench-%d", i)
		roomID := fmt.Sprintf("room-%d", roomAssign2[i])
		serverURL := serverList[i%len(serverList)]

		go func(connIdx int, uid, roomID, serverURL string) {
			defer wg.Done()
			runClient(ctx, metrics, connIdx, serverURL, uid, roomID, *token, *rate, *duration, *warmup, *deliveryCheck, measureStart, sendEnd, startTime, *priorityRatio, *reauthEvery, *noReauth)
		}(i, uid, roomID, serverURL)

		time.Sleep(rampDelay)
	}

	// 等待测量窗结束（warmup + duration + ramp 的保守上界）
	select {
	case <-ctx.Done():
	case <-time.After(*warmup + *duration + *ramp):
	}
	cancel()

waitDone:
	wg.Wait()

	// 打印最终报告（测量窗内统计）
	printReport(metrics, time.Since(startTime), *deliveryCheck)

	// 导出 JSON/CSV
	if *outputJSON != "" {
		roomStats := computeRoomStats(roomAssign2, *rooms, *dist)
		var delivery map[string]any
		if *deliveryCheck {
			obs := metrics.deliveryObserved.Load()
			miss := metrics.deliveryMissing.Load()
			exp := obs + miss
			rate := float64(0)
			if exp > 0 {
				rate = float64(obs) / float64(exp)
			}
			delivery = map[string]any{
				"enabled": true, "observed_deliveries": obs, "missing_deliveries": miss,
				"expected_deliveries": exp, "delivery_rate": rate,
			}
		} else {
			delivery = map[string]any{"enabled": false}
		}
		exportJSON(*outputJSON, snapshots, metrics, measureStart, (*warmup).String(), (*duration).String(), roomStats, delivery)
	}
	if *outputCSV != "" {
		exportCSV(*outputCSV, snapshots)
	}
}

// runReconnectCheck 校验服务端「热历史 + 断线补发」：
// 发送端 A 以固定节奏发 K 条，接收端 B 以「读空闲 600ms」为排空判据收完并记录
// lastSeq 后断开；A 再发 M 条（B 缺席）；B 带 after_seq=lastSeq 重连，
// 核对 replay_done.recovered == M。
// 注：发送与读取完全解耦（不因读到控制帧而补发），避免多发消息污染期望值。
func runReconnectCheck(serverURL, token string) error {
	const room = "room-reconnect-check"
	wsURL := func(uid string) string {
		return fmt.Sprintf("%s/ws?uid=%s&room=%s&token=%s", serverURL, uid, room, token)
	}
	send := func(c *websocket.Conn, content string) error {
		payload, _ := json.Marshal(map[string]any{
			"type": "danmu", "content": content, "client_ts": time.Now().UnixMilli(),
		})
		return c.WriteMessage(websocket.TextMessage, payload)
	}

	// A：发送端（独立 uid，避免与 B 同 uid 顶号）
	a, _, err := websocket.DefaultDialer.Dial(wsURL("rc-sender"), nil)
	if err != nil {
		return fmt.Errorf("sender dial: %w", err)
	}
	defer a.Close()
	// A 自己也会收到广播，后台读走避免缓冲堆积
	go func() {
		for {
			if _, _, err := a.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// B：接收端
	b, _, err := websocket.DefaultDialer.Dial(wsURL("rc-recv"), nil)
	if err != nil {
		return fmt.Errorf("recv dial: %w", err)
	}
	time.Sleep(200 * time.Millisecond) // 等注册完成

	// phase1：A 固定发 K 条，B 读到空闲为止
	const k, m = 6, 4
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for i := 0; i < k; i++ {
			if err := send(a, fmt.Sprintf("k-%d", i)); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var lastSeq uint64
	seen := 0
	b.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	for {
		_, data, err := b.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break // 600ms 无消息，视为已排空
			}
			b.Close()
			return fmt.Errorf("recv read: %w", err)
		}
		var msgs []map[string]any
		if err := json.Unmarshal(data, &msgs); err != nil {
			b.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
			continue
		}
		for _, msg := range msgs {
			if msg["type"] != "danmu" {
				continue
			}
			seen++
			if seq, ok := msg["seq"].(float64); ok && uint64(seq) > lastSeq {
				lastSeq = uint64(seq)
			}
		}
		b.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	}
	<-sendDone
	if seen < k {
		b.Close()
		return fmt.Errorf("phase1: received=%d < sent=%d", seen, k)
	}
	log.Printf("[reconnect-check] phase1 done: received=%d lastSeq=%d", seen, lastSeq)

	// B 断开，A 继续发 M 条（B 缺席，消息只进服务端热历史）
	b.Close()
	for i := 0; i < m; i++ {
		if err := send(a, fmt.Sprintf("m-%d", i)); err != nil {
			return fmt.Errorf("sender write: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // 等 worker flush 与热历史落定

	// B 重连（同 uid），带 after_seq=lastSeq 请求补发
	b2, _, err := websocket.DefaultDialer.Dial(wsURL("rc-recv"), nil)
	if err != nil {
		return fmt.Errorf("recv redial: %w", err)
	}
	defer b2.Close()
	b2.SetReadDeadline(time.Now().Add(30 * time.Second))
	time.Sleep(200 * time.Millisecond) // 等注册完成
	if err := b2.WriteMessage(websocket.TextMessage,
		[]byte(fmt.Sprintf(`{"type":"reconnect","after_seq":%d}`, lastSeq))); err != nil {
		return fmt.Errorf("reconnect send: %w", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, data, err := b2.ReadMessage()
		if err != nil {
			return fmt.Errorf("recv read: %w", err)
		}
		var msgs []map[string]any
		if err := json.Unmarshal(data, &msgs); err != nil {
			continue
		}
		for _, msg := range msgs {
			if msg["type"] != "replay_done" {
				continue
			}
			recovered := int(msg["recovered"].(float64))
			if recovered != m {
				return fmt.Errorf("recovered=%d, want %d", recovered, m)
			}
			log.Printf("[reconnect-check] recovered=%d == expected %d", recovered, m)
			return nil
		}
	}
	return fmt.Errorf("timeout waiting replay_done")
}

// reauthLoop 周期性刷新会话令牌：REST /api/v1/session-token 换新令牌 → WS 发 reauth。
// 固定间隔（默认 8min < 服务端 -session-ttl 默认 10min），失败静默等待下一轮
// （若连续失败，服务端到期检查每秒一次，仍可能 4008 断开——由报告中的断连暴露）。
func reauthLoop(ctx context.Context, conn *websocket.Conn, serverURL, uid, roomID, token string, every time.Duration) {
	// ws://host → http://host
	httpBase := serverURL
	if strings.HasPrefix(httpBase, "ws://") {
		httpBase = "http://" + strings.TrimPrefix(httpBase, "ws://")
	} else if strings.HasPrefix(httpBase, "wss://") {
		httpBase = "https://" + strings.TrimPrefix(httpBase, "wss://")
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			body, _ := json.Marshal(map[string]string{"uid": uid, "room_id": roomID})
			req, err := http.NewRequest(http.MethodPost, httpBase+"/api/v1/session-token", bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := httpClient.Do(req)
			if err != nil {
				continue
			}
			var out struct {
				Token string `json:"token"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if decodeErr != nil || out.Token == "" {
				continue
			}
			payload, _ := json.Marshal(map[string]string{"type": "reauth", "token": out.Token})
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			conn.WriteMessage(websocket.TextMessage, payload)
		}
	}
}

// runClient 单个压测连接
func runClient(ctx context.Context, m *Metrics, connIdx int, serverURL, uid, roomID, token string, rate float64, duration, warmup time.Duration, deliveryCheck bool, measureStart, sendEnd, startTime time.Time, priorityRatio float64, reauthEvery time.Duration, noReauth bool) {
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

	// closing 标志：收尾时是"我们主动关连接"，reader 观察到的错误属正常，不计 readError。
	var closing atomic.Bool
	defer func() {
		closing.Store(true)
		conn.Close()
		m.activeConns.Add(-1)
	}()

	// Pong handler
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	// 会话续期：>10min 的压测必须续期，否则服务端 sessionTTL(10min) 到期 4008 断开。
	if !noReauth && reauthEvery > 0 {
		go reauthLoop(ctx, conn, serverURL, uid, roomID, token, reauthEvery)
	}

	// 投递核算（-delivery-check）：本连接维护 seq 连续性。
	var tracker deliveryTracker
	var prevObs, prevMiss int64 // 上一帧上报的累计值（用于 delta）

	// 读取 goroutine：收消息、统计延迟
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		var nanos []int64 // 复用的纳秒时间戳缓冲
		var seqs []int64  // 复用的 seq 缓冲（delivery-check）
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() == nil && !closing.Load() {
					m.readErrors.Add(1)
				}
				return
			}

			// 快速扫描器：只提取压测需要的统计量（danmu/ack/高优计数 + 纳秒时间戳）。
			nowNano := time.Now().UnixNano()
			danmu, acks, high, nanos, seqs := scanFrame(data, nanos, seqs, deliveryCheck)
			if acks > 0 {
				m.ackCount.Add(int64(acks))
			}
			if danmu > 0 {
				m.recvCount.Add(int64(danmu))
				if high > 0 {
					m.recvHigh.Add(int64(high))
				}
				for _, ts := range nanos {
					if latency := nowNano - ts; latency > 0 {
						m.RecordE2ELatency(connIdx, time.Duration(latency))
					}
				}
				if deliveryCheck && len(seqs) > 0 {
					obs, miss := tracker.onSeqs(m.epoch.Load(), seqs)
					if obs != prevObs {
						m.deliveryObserved.Add(obs - prevObs)
						prevObs = obs
					}
					if miss != prevMiss {
						m.deliveryMissing.Add(miss - prevMiss)
						prevMiss = miss
					}
				}
			}
		}
	}()

	// 发送循环：warm-up 结束后才开始发；sendEnd 停止。
	if rate > 0 {
		if d := measureStart.Sub(time.Now()); d > 0 {
			select {
			case <-ctx.Done():
				<-readDone
				return
			case <-readDone:
				return
			case <-time.After(d):
			}
		}
		interval := time.Duration(float64(time.Second) / rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		content := fmt.Sprintf("bench msg from %s", uid)
		var seq int64

		for {
			select {
			case <-ctx.Done():
				return
			case <-readDone:
				return
			case <-ticker.C:
				if time.Now().After(sendEnd) {
					return
				}
				now := time.Now()
				seq++
				msg := UpMessage{
					Type:         "danmu",
					Content:      content,
					ClientTS:     now.UnixMilli(),
					ClientTSNano: now.UnixNano(),
					MsgID:        fmt.Sprintf("%s-%d-%s", uid, seq, runSalt),
				}
				if priorityRatio > 0 && rand.Float64() < priorityRatio {
					msg.Priority = 1
					m.sentHigh.Add(1)
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
		case <-time.After(time.Until(sendEnd)):
		}
	}
}

// roomAssignFor 依分布 + seed 确定性地把每个连接分配到房间 index（0..rooms-1）。
// 与 ops 端 workload.go 的算法保持一致（两处皆确定性，供诊断可复现）。
func roomAssignFor(conns, rooms int, dist string, zipfS float64, seed int64) []int {
	assign := make([]int, conns)
	switch dist {
	case "hot_room":
		hotBoundary := int(float64(conns) * 0.8)
		for i := 0; i < conns; i++ {
			if i < hotBoundary {
				assign[i] = 0
			} else {
				assign[i] = 1 + (i % (rooms - 1))
			}
		}
	case "zipf":
		s := zipfS
		if s <= 0 {
			s = 1.1
		}
		g := newZipfFor(rooms, s)
		r := newSeeded(seed)
		for i := 0; i < conns; i++ {
			assign[i] = g.sample(r)
		}
	default: // uniform
		for i := 0; i < conns; i++ {
			assign[i] = i % rooms
		}
	}
	return assign
}

// computeRoomStats 依据真实分配计算房间热度诊断（大房在前）。
func computeRoomStats(assign []int, rooms int, dist string) map[string]any {
	sizes := make([]int, rooms)
	for _, r := range assign {
		if r >= 0 && r < rooms {
			sizes[r]++
		}
	}
	sorted := append([]int(nil), sizes...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
	out := map[string]any{
		"distribution":  dist,
		"rooms":         rooms,
		"connections":   len(assign),
		"max_room_size": sorted[0],
		"min_room_size": sorted[len(sorted)-1],
	}
	mean := float64(len(assign)) / float64(rooms)
	out["mean_room_size"] = mean
	out["largest_room_share"] = float64(sorted[0]) / float64(len(assign))
	topN := rooms / 10
	if topN < 1 {
		topN = 1
	}
	topSum := 0
	for i := 0; i < topN; i++ {
		topSum += sorted[i]
	}
	out["top_10_percent_room_share"] = float64(topSum) / float64(len(assign))
	var sizesOut []int
	for i := 0; i < len(sorted) && i < 200; i++ {
		sizesOut = append(sizesOut, sorted[i])
	}
	out["room_sizes"] = sizesOut
	return out
}

// zipfGen 确定性 Zipf 取样（inverse-CDF on splitmix64）。
type zipfGen struct {
	cdf []float64
}

func newZipfFor(rooms int, s float64) *zipfGen {
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
	return &zipfGen{cdf: cdf}
}

func (g *zipfGen) sample(r *seeded) int {
	u := r.Float64()
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

// seeded splitmix64 确定性随机源（独立于 math/rand 全局状态）。
type seeded struct{ state uint64 }

func newSeeded(seed int64) *seeded {
	return &seeded{state: uint64(seed)*0x9E3779B97F4A7C15 + 1442695040888963407}
}

func (s *seeded) next() uint64 {
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (s *seeded) Float64() float64 {
	return float64(s.next()>>11) / float64(1<<53)
}

// printReport 打印最终汇总报告
func printReport(m *Metrics, elapsed time.Duration, deliveryCheck bool) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	e2e := m.e2eMerged()
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("                    LOAD TEST REPORT")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\n%-30s %s\n", "Wall Duration:", elapsed.Round(time.Second))
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

	// 吞吐层（测量窗内）
	fmt.Println("--- Throughput (measurement window) ---")
	totalSend := m.WindowSent()
	totalRecv := m.WindowRecv()
	ackCount := m.ackCount.Load() - m.baseAck.Load()
	sentHigh := m.sentHigh.Load()
	recvHigh := m.recvHigh.Load()
	elapsedSec := elapsed.Seconds()
	fmt.Printf("  %-28s %d (%.0f/s)\n", "Total Sent:", totalSend, float64(totalSend)/elapsedSec)
	fmt.Printf("  %-28s %d (%.0f/s)\n", "Total Received:", totalRecv, float64(totalRecv)/elapsedSec)
	fmt.Printf("  %-28s %d\n", "Dropped:", m.WindowDrop())
	ackRate := 0.0
	if totalSend > 0 {
		ackRate = float64(ackCount) / float64(totalSend) * 100
	}
	fmt.Printf("  %-28s %.2f%% (%d/%d)\n", "Ack Rate:", ackRate, ackCount, totalSend)
	fmt.Printf("  %-28s %d\n", "High-prio Sent:", sentHigh)
	fmt.Printf("  %-28s %d\n", "High-prio Recv:", recvHigh)
	highLoss := sentHigh*m.targetConns - recvHigh
	if highLoss < 0 {
		highLoss = 0
	}
	fmt.Printf("  %-28s %d (期望 %d，单房间假设)\n", "High-prio Loss:", highLoss, sentHigh*m.targetConns)
	fmt.Println()

	// 投递核算
	fmt.Println("--- Delivery Accounting (-delivery-check) ---")
	if deliveryCheck {
		obs := m.deliveryObserved.Load()
		miss := m.deliveryMissing.Load()
		exp := obs + miss
		rate := 1.0
		if exp > 0 {
			rate = float64(obs) / float64(exp)
		}
		fmt.Printf("  %-28s %d\n", "Observed deliveries (conn-level):", obs)
		fmt.Printf("  %-28s %d\n", "Missing deliveries (seq gaps):", miss)
		fmt.Printf("  %-28s %d\n", "Expected deliveries:", exp)
		fmt.Printf("  %-28s %.6f\n", "Delivery rate:", rate)
	} else {
		fmt.Println("  (disabled; pass -delivery-check for real delivery accounting)")
	}
	fmt.Println()

	// 延迟层
	fmt.Println("--- End-to-End Latency ---")
	if e2e.TotalCount() > 0 {
		fmt.Printf("  P50:   %d μs (%.1f ms)\n", e2e.ValueAtPercentile(50), float64(e2e.ValueAtPercentile(50))/1000)
		fmt.Printf("  P90:   %d μs (%.1f ms)\n", e2e.ValueAtPercentile(90), float64(e2e.ValueAtPercentile(90))/1000)
		fmt.Printf("  P99:   %d μs (%.1f ms)\n", e2e.ValueAtPercentile(99), float64(e2e.ValueAtPercentile(99))/1000)
		fmt.Printf("  P999:  %d μs (%.1f ms)\n", e2e.ValueAtPercentile(99.9), float64(e2e.ValueAtPercentile(99.9))/1000)
		fmt.Printf("  Max:   %d μs (%.1f ms)\n", e2e.Max(), float64(e2e.Max())/1000)
		fmt.Printf("  Mean:  %.0f μs (%.1f ms)\n", e2e.Mean(), e2e.Mean()/1000)
	} else {
		fmt.Println("  (no latency data)")
	}
	fmt.Println()

	// 错误层
	fmt.Println("--- Errors ---")
	fmt.Printf("  %-28s %d\n", "Connect Failed:", m.failedConns.Load())
	fmt.Printf("  %-28s %d\n", "Write Errors:", m.WindowWE())
	fmt.Printf("  %-28s %d\n", "Read Errors:", m.WindowRE())
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

// exportJSON 导出 JSON 报告（summary 均为测量窗内值；含测量窗口、房间诊断、投递核算）
func exportJSON(path string, snapshots []Snapshot, m *Metrics, measureStart time.Time, warmup, measurement string, roomStats, delivery map[string]any) {
	e2e := m.e2eMerged()

	report := map[string]interface{}{
		"summary": map[string]interface{}{
			"target_conns":  m.targetConns,
			"success_conns": m.successConns.Load(),
			"failed_conns":  m.failedConns.Load(),
			"total_sent":    m.WindowSent(),
			"total_recv":    m.WindowRecv(),
			"e2e_p50_us":    e2e.ValueAtPercentile(50),
			"e2e_p90_us":    e2e.ValueAtPercentile(90),
			"e2e_p99_us":    e2e.ValueAtPercentile(99),
			"e2e_p999_us":   e2e.ValueAtPercentile(99.9),
			"e2e_max_us":    e2e.Max(),
		},
		"measurement": map[string]any{
			"start":       measureStart.UTC().Format(time.RFC3339Nano),
			"end":         time.Now().UTC().Format(time.RFC3339Nano),
			"warmup":      warmup,
			"measurement": measurement,
		},
		"room_stats": roomStats,
		"delivery":   delivery,
		"snapshots":  snapshots,
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
