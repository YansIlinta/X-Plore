// Package ops 实现 Danmu Ops Console 的后端数据面：轮询 registry 发现实例，
// 并发抓取各实例的观测端点，聚合出 overview / services / topology / events。
// 它是纯旁路观测者：只读 + 聚合，不参与消息链路，自身挂掉不影响弹幕系统。
//
// 数据真实性约定：
//   - 默认模式严禁伪造数据；拿不到的值一律 null（前端显示 N/A）。
//   - 仅 -mock 显式启用时喂假数据，且每个响应都带 "mock": true。
package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	healthHealthy   = "healthy"
	healthDegraded  = "degraded"
	healthCritical  = "critical"
	eventInfo       = "INFO"
	eventWarning    = "WARNING"
	eventError      = "ERROR"
	eventBufferSize = 500
)

// Config 是 Collector 的构造参数（由 cmd/ops 的 flags 映射而来）。
type Config struct {
	RegistryURL  string        // registry base URL（如 http://localhost:7350）
	Token        string        // comet /api/v1/stats 的 Bearer token（DANMU_AUTH_TOKEN）
	KafkaBrokers string        // 逗号分隔；空字符串 = 不观测 Kafka
	KafkaTopic   string        // 广播 topic，默认 danmu-broadcast
	KafkaGroups  []string      // 计算 lag 的消费组
	Poll         time.Duration // 采集周期
	Mock         bool          // mock 模式：喂假数据（显式）
}

// Instance 是单个服务实例的观测快照。
type Instance struct {
	HTTPAddr   string             `json:"http_addr"`
	RPCAddr    string             `json:"rpc_addr,omitempty"` // 对应 RPC 地址（若能从 registry 对上）
	Healthy    bool               `json:"healthy"`
	Err        string             `json:"err,omitempty"`   // 探测失败原因
	MsgInRate  *float64           `json:"msg_in_rate"`     // 上行 msg/s（仅 comet，null=暂无）
	MsgOutRate *float64           `json:"msg_out_rate"`    // 下行 msg/s（仅 comet）
	Rates      map[string]float64 `json:"rates,omitempty"` // stats 里 *_total 计数器的通用差分速率（key 去掉 _total 后缀）
	Stats      map[string]any     `json:"stats"`           // 实例 /api/v1/stats 原始 JSON；不可达为 null
}

// Service 是按逻辑组件聚合的实例组。
type Service struct {
	Name      string     `json:"name"`
	Instances []Instance `json:"instances"`
}

// KafkaInfo 是 Kafka 观测结果；任何一步失败 Available=false，lag 为 null。
type KafkaInfo struct {
	Available    bool              `json:"available"`
	Topic        string            `json:"topic"`
	Partitions   int               `json:"partitions"`
	Lag          map[string]*int64 `json:"lag"`           // group → lag 总数；不可得为 null
	ProducedRate *float64          `json:"produced_rate"` // logic onmessage_total 差分（msg/s 入队）
	ConsumedRate *float64          `json:"consumed_rate"` // job consumed_total 差分（msg/s 出队）
	Err          string            `json:"err,omitempty"`
}

// Snapshot 是一轮采集的不可变结果，API 只读它。
type Snapshot struct {
	Mock         bool      `json:"mock"`
	TS           time.Time `json:"ts"`
	RegistryUp   bool      `json:"registry_up"`
	Health       string    `json:"health"`
	HealthDetail []string  `json:"health_detail"`
	Services     []Service `json:"services"`
	Kafka        KafkaInfo `json:"kafka"`
}

// Event 是系统事件流条目。
type Event struct {
	TS      time.Time `json:"ts"`
	Level   string    `json:"level"` // INFO | WARNING | ERROR
	Kind    string    `json:"kind"`  // instance | registry | health | kafka | loadtest
	Message string    `json:"message"`
}

// eventBuffer 有界环形缓冲：实时消息/事件绝不能无限保存。
type eventBuffer struct {
	mu  sync.Mutex
	buf []Event
}

func (b *eventBuffer) Add(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, e)
	if len(b.buf) > eventBufferSize {
		b.buf = b.buf[len(b.buf)-eventBufferSize:]
	}
}

func (b *eventBuffer) Recent(n int) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n <= 0 || n > len(b.buf) {
		n = len(b.buf)
	}
	out := make([]Event, n)
	copy(out, b.buf[len(b.buf)-n:])
	// 最新在前，前端好渲染
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// counterSample 是 /metrics 里 danmu_messages_total 的一次采样。
type counterSample struct {
	in, out float64
	ts      time.Time
}

// Collector 维护采集循环与最新快照。
type Collector struct {
	cfg        Config
	httpClient *http.Client
	events     eventBuffer

	traces *traceStore // 跨服务 trace 汇聚（自带锁）

	mu          sync.RWMutex
	snap        Snapshot
	prevCounts  map[string]counterSample // http addr → 上一轮计数器采样
	prevStats   map[string]map[string]float64
	prevStatsTS map[string]time.Time
	kafka       KafkaInfo
	lastService map[string][]string // 上一轮 registry 全量结果（registry 掉线时留用）
}

// NewCollector 构造采集器。httpClient 超时 2s：任何实例慢都不能拖垮采集循环。
func NewCollector(cfg Config) *Collector {
	return &Collector{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout:   2 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 4},
		},
		prevCounts:  make(map[string]counterSample),
		prevStats:   make(map[string]map[string]float64),
		prevStatsTS: make(map[string]time.Time),
		kafka:       KafkaInfo{Available: false, Lag: map[string]*int64{}},
		traces:      newTraceStore(),
	}
}

// Run 启动采集循环与 Kafka lag 循环，直到 ctx 取消。mock 模式走独立分支。
func (c *Collector) Run(ctx context.Context) {
	if c.cfg.Mock {
		go c.runMock(ctx)
		return
	}
	go c.kafkaLoop(ctx)
	go c.pollLoop(ctx)
	go c.traceLoop(ctx)
}

// Traces 返回最近 n 条汇聚好的消息链路（最新在前）。
func (c *Collector) Traces(n int) []Trace { return c.traces.list(n) }

// TraceSources 返回各节点自述的 trace 状态（是否开启、采样率、缓冲溢出数）。
func (c *Collector) TraceSources() map[string]any { return c.traces.sourceStats() }

// Snapshot 返回最新采集结果（只读语义，调用方不得修改）。
func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snap
}

// Events 返回最近 n 条事件。
func (c *Collector) Events(n int) []Event { return c.events.Recent(n) }

// pollLoop 主采集循环：registry → 实例探测 → 聚合 → 事件 diff。
func (c *Collector) pollLoop(ctx context.Context) {
	c.pollOnce(true) // 启动先采一轮，避免 API 冷启动全是 null
	t := time.NewTicker(c.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.pollOnce(false)
		}
	}
}

func (c *Collector) pollOnce(first bool) {
	c.mu.RLock()
	prevSnap := c.snap
	c.mu.RUnlock()

	all, regErr := c.fetchRegistry()
	snap := Snapshot{
		Mock:       false,
		TS:         time.Now(),
		RegistryUp: regErr == nil,
	}

	if regErr == nil {
		c.mu.Lock()
		c.lastService = all
		c.mu.Unlock()
	} else {
		c.mu.RLock()
		all = c.lastService // registry 掉线：用上次已知实例清单继续探测
		c.mu.RUnlock()
		log.Printf("[ops] registry fetch: %v", regErr)
	}

	snap.Services = c.probeServices(all)
	// 清理已消失实例的差分样本：实例 churn 下 prevStats/prevCounts 会无界增长
	c.prunePrev(snap.Services)

	c.mu.RLock()
	kafkaInfo := c.kafka
	c.mu.RUnlock()
	snap.Kafka = kafkaInfo

	// Kafka 出入队速率：logic 的 onmessage 速率 ≈ produce，job 的 consumed 速率 ≈ consume。
	// 来自真实计数器差分；首轮无样本时为 null。
	var produced, consumed *float64
	for _, svc := range snap.Services {
		for _, it := range svc.Instances {
			if !it.Healthy {
				continue
			}
			switch svc.Name {
			case "logic":
				if r, ok := it.Rates["onmessage"]; ok {
					produced = addF(produced, r)
				}
			case "job":
				if r, ok := it.Rates["consumed"]; ok {
					consumed = addF(consumed, r)
				}
			}
		}
	}
	snap.Kafka.ProducedRate = produced
	snap.Kafka.ConsumedRate = consumed

	c.evalHealth(&snap)
	if first {
		// 首轮不发事件（全部是"出现"，无信息量）
	} else {
		c.diffEvents(prevSnap, snap, regErr)
	}

	c.mu.Lock()
	c.snap = snap
	c.mu.Unlock()
}

// fetchRegistry GET registry /services（无参 → 全部服务 map）。
func (c *Collector) fetchRegistry() (map[string][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.RegistryURL+"/services", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry status %d", resp.StatusCode)
	}
	var all map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return nil, err
	}
	return all, nil
}

// probeTarget 是一次实例探测的目标。
type probeTarget struct {
	component string // comet / logic / job
	httpAddr  string
	rpcAddr   string
}

// probeServices 并发探测各 *-http 实例，聚合为按逻辑组件的 Service 列表。
// rpcAddrOf 用主机名把 comet-http ↔ comet、logic-http ↔ logic 对上。
func (c *Collector) probeServices(all map[string][]string) []Service {
	var targets []probeTarget
	for _, comp := range []string{"comet", "logic", "job"} {
		httpAddrs := all[comp+"-http"]
		rpcAddrs := all[comp]
		sort.Strings(httpAddrs)
		sort.Strings(rpcAddrs)
		for _, h := range httpAddrs {
			targets = append(targets, probeTarget{comp, h, rpcAddrOf(h, rpcAddrs)})
		}
	}

	// 并发探测，结果按 component 分组保序
	type result struct {
		i  int
		it Instance
	}
	resCh := make(chan result, len(targets))
	var wg sync.WaitGroup
	for i, tg := range targets {
		wg.Add(1)
		go func(i int, tg probeTarget) {
			defer wg.Done()
			resCh <- result{i, c.probeInstance(tg)}
		}(i, tg)
	}
	wg.Wait()
	close(resCh)

	insts := make([]Instance, len(targets))
	for r := range resCh {
		insts[r.i] = r.it
	}

	byComp := make(map[string][]Instance)
	var order []string
	for _, tg := range targets {
		if _, ok := byComp[tg.component]; !ok {
			order = append(order, tg.component)
		}
	}
	for i, tg := range targets {
		byComp[tg.component] = append(byComp[tg.component], insts[i])
	}
	out := make([]Service, 0, len(order))
	for _, comp := range order {
		out = append(out, Service{Name: comp, Instances: byComp[comp]})
	}
	return out
}

// rpcAddrOf 按主机名把 HTTP 观测地址与 RPC 地址对上（如 comet1:8080 ↔ comet1:7500）。
func rpcAddrOf(httpAddr string, rpcAddrs []string) string {
	host, _, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return ""
	}
	for _, r := range rpcAddrs {
		if h, _, err := net.SplitHostPort(r); err == nil && h == host {
			return r
		}
	}
	return ""
}

// probeInstance 探测单个实例：/health 判活，/api/v1/stats 取原始 JSON，
// comet 额外抓 /metrics 解析 danmu_messages_total 算速率。
func (c *Collector) probeInstance(tg probeTarget) Instance {
	it := Instance{
		HTTPAddr: tg.httpAddr,
		RPCAddr:  tg.rpcAddr,
		Stats:    nil,
	}

	if _, err := c.getJSON("http://"+tg.httpAddr+"/health", nil); err != nil {
		it.Healthy = false
		it.Err = err.Error()
		return it
	}
	it.Healthy = true

	stats, err := c.getJSON("http://"+tg.httpAddr+"/api/v1/stats", map[string]string{"Authorization": "Bearer " + c.cfg.Token})
	if err != nil {
		// 健康但 stats 拉不到（如鉴权配置不一致）：标记错误但不判死
		it.Err = "stats: " + err.Error()
		return it
	}
	it.Stats = stats

	if tg.component == "comet" {
		it.MsgInRate, it.MsgOutRate = c.cometRates(tg.httpAddr)
	}
	it.Rates = c.statRates(tg.httpAddr, stats)
	return it
}

// statRates 对 stats 里所有 *_total 计数器做 Δ/Δt 差分，key 去掉 _total 后缀。
// 首轮或计数器回退（进程重启）时该 key 不出现在结果里（不伪造 0）。
func (c *Collector) statRates(httpAddr string, stats map[string]any) map[string]float64 {
	now := time.Now()
	cur := make(map[string]float64)
	for k, v := range stats {
		if !strings.HasSuffix(k, "_total") {
			continue
		}
		if f := asFloat(v); f != nil {
			cur[k] = *f
		}
	}

	c.mu.Lock()
	prev := c.prevStats[httpAddr]
	prevTS := c.prevStatsTS[httpAddr]
	c.prevStats[httpAddr] = cur
	c.prevStatsTS[httpAddr] = now
	c.mu.Unlock()

	if prev == nil || now.Sub(prevTS) <= 0 {
		return nil
	}
	dt := now.Sub(prevTS).Seconds()
	var out map[string]float64
	for k, cv := range cur {
		pv, ok := prev[k]
		if !ok || cv < pv {
			continue // 新出现的计数器或回退（重启）：本轮不算
		}
		if out == nil {
			out = make(map[string]float64)
		}
		out[strings.TrimSuffix(k, "_total")] = (cv - pv) / dt
	}
	return out
}

// prunePrev 删除已不存在实例的差分样本（prevStats/prevStatsTS/prevCounts），
// 防止实例反复上线/下线时这些 map 无界增长。
func (c *Collector) prunePrev(services []Service) {
	live := make(map[string]bool)
	for _, svc := range services {
		for _, it := range svc.Instances {
			live[it.HTTPAddr] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.prevStats {
		if !live[k] {
			delete(c.prevStats, k)
			delete(c.prevStatsTS, k)
		}
	}
	for k := range c.prevCounts {
		if !live[k] {
			delete(c.prevCounts, k)
		}
	}
}

// getJSON GET url 并解析 JSON object；2s 超时由 httpClient 保证。
func (c *Collector) getJSON(url string, headers map[string]string) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

var counterRe = regexp.MustCompile(`danmu_messages_total\{direction="(in|out)"\} ([0-9.eE+]+)`)

// cometRates 抓 comet /metrics，用 Δcounter/Δt 算上行/下行速率；首轮返回 nil。
func (c *Collector) cometRates(httpAddr string) (*float64, *float64) {
	req, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/metrics", nil)
	if err != nil {
		return nil, nil
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 上限 1MB，防异常大响应
	if err != nil {
		return nil, nil
	}
	var cur counterSample
	for _, m := range counterRe.FindAllSubmatch(body, -1) {
		if v, err := strconv.ParseFloat(string(m[2]), 64); err == nil {
			if string(m[1]) == "in" {
				cur.in = v
			} else {
				cur.out = v
			}
		}
	}
	now := time.Now()

	c.mu.Lock()
	prev, ok := c.prevCounts[httpAddr]
	c.prevCounts[httpAddr] = cur
	c.mu.Unlock()
	if !ok || now.Sub(prev.ts) <= 0 {
		return nil, nil // 首轮采样，无速率
	}
	dt := now.Sub(prev.ts).Seconds()
	inRate, outRate := (cur.in-prev.in)/dt, (cur.out-prev.out)/dt
	if inRate < 0 || outRate < 0 {
		return nil, nil // 计数器重置（进程重启），本轮不作数
	}
	return &inRate, &outRate
}

// evalHealth 根据 registry / 各组件实例 / Kafka 的真实状态判定系统健康。
// 规则：registry 不可达或所有 comet 不可达或 Kafka（启用观测时）不可用 → critical；
// 任一实例不可达 → degraded；否则 healthy。
func (c *Collector) evalHealth(snap *Snapshot) {
	var detail []string
	health := healthHealthy

	if !snap.RegistryUp {
		health = healthCritical
		detail = append(detail, "registry 不可达：服务发现失效")
	}

	var cometTotal, cometHealthy int
	var degradedComp []string
	for _, svc := range snap.Services {
		total, healthy := 0, 0
		for _, it := range svc.Instances {
			total++
			if it.Healthy {
				healthy++
			}
		}
		if svc.Name == "comet" {
			cometTotal, cometHealthy = total, healthy
		}
		if healthy < total {
			degradedComp = append(degradedComp, fmt.Sprintf("%s %d/%d 实例不可达", svc.Name, total-healthy, total))
		}
	}
	if cometTotal > 0 && cometHealthy == 0 {
		health = healthCritical
		detail = append(detail, "所有 comet 实例不可达：接入层瘫痪")
	}
	if len(degradedComp) > 0 && health != healthCritical {
		health = healthDegraded
		detail = append(detail, degradedComp...)
	}
	if cometTotal == 0 && snap.RegistryUp {
		health = healthDegraded
		detail = append(detail, "registry 中没有 comet 实例注册")
	}
	if c.cfg.KafkaBrokers != "" && !snap.Kafka.Available {
		health = healthCritical
		detail = append(detail, "Kafka 不可用：消息总线中断")
	}
	if health == healthHealthy {
		detail = append(detail, "所有组件正常")
	}
	snap.Health = health
	snap.HealthDetail = detail
}

// diffEvents 对比前后两轮快照，产出实例出现/消失、registry 恢复/掉线、健康状态翻转事件。
func (c *Collector) diffEvents(prev, cur Snapshot, regErr error) {
	prevInsts := map[string]bool{}
	curInsts := map[string]bool{}
	for _, svc := range prev.Services {
		for _, it := range svc.Instances {
			prevInsts[it.HTTPAddr] = it.Healthy
		}
	}
	for _, svc := range cur.Services {
		for _, it := range svc.Instances {
			curInsts[it.HTTPAddr] = it.Healthy
		}
	}

	for addr, healthy := range curInsts {
		if was, ok := prevInsts[addr]; !ok {
			c.events.Add(Event{TS: cur.TS, Level: eventInfo, Kind: "instance", Message: fmt.Sprintf("%s registered", addr)})
		} else if was && !healthy {
			c.events.Add(Event{TS: cur.TS, Level: eventError, Kind: "instance", Message: fmt.Sprintf("%s unavailable", addr)})
		} else if !was && healthy {
			c.events.Add(Event{TS: cur.TS, Level: eventInfo, Kind: "instance", Message: fmt.Sprintf("%s recovered", addr)})
		}
	}
	for addr := range prevInsts {
		if _, ok := curInsts[addr]; !ok {
			c.events.Add(Event{TS: cur.TS, Level: eventWarning, Kind: "instance", Message: fmt.Sprintf("%s disappeared", addr)})
		}
	}

	if !prev.RegistryUp && cur.RegistryUp {
		c.events.Add(Event{TS: cur.TS, Level: eventInfo, Kind: "registry", Message: "registry recovered"})
	} else if prev.RegistryUp && !cur.RegistryUp {
		c.events.Add(Event{TS: cur.TS, Level: eventError, Kind: "registry", Message: "registry unreachable"})
	}

	if prev.Health != cur.Health && prev.TS != cur.TS {
		level := eventInfo
		if cur.Health == healthDegraded {
			level = eventWarning
		} else if cur.Health == healthCritical {
			level = eventError
		}
		c.events.Add(Event{TS: cur.TS, Level: level, Kind: "health", Message: fmt.Sprintf("system health: %s → %s", prev.Health, cur.Health)})
	}
}

// kafkaLoop 独立周期（poll*3）算 consumer lag，不阻塞主采集循环。
// Kafka 未启用（brokers 为空）时不运行。
func (c *Collector) kafkaLoop(ctx context.Context) {
	if c.cfg.KafkaBrokers == "" {
		return
	}
	t := time.NewTicker(c.cfg.Poll * 3)
	defer t.Stop()
	for {
		c.kafkaOnce()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// kafkaOnce 计算 topic 各消费组的 lag（最新 offset − 已提交 offset 求和）。
// 任一步失败 → Available=false、lag 全 null，绝不伪造。
func (c *Collector) kafkaOnce() {
	info := KafkaInfo{Available: false, Lag: map[string]*int64{}}
	brokers := splitComma(c.cfg.KafkaBrokers)
	if len(brokers) == 0 {
		c.setKafka(info) // Kafka 未配置：不观测，前端显示 N/A
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: kafka.TCP(brokers...), Topics: []string{c.cfg.KafkaTopic}})
	if err != nil {
		info.Err = "metadata: " + err.Error()
		c.setKafka(info)
		return
	}
	if len(meta.Topics) == 0 {
		info.Err = "topic " + c.cfg.KafkaTopic + " not found"
		c.setKafka(info)
		return
	}
	partitions := make([]int, 0, len(meta.Topics[0].Partitions))
	for _, p := range meta.Topics[0].Partitions {
		partitions = append(partitions, p.ID)
	}
	info.Topic = c.cfg.KafkaTopic
	info.Partitions = len(partitions)

	// 最新 offset
	listReq := &kafka.ListOffsetsRequest{
		Addr:   kafka.TCP(brokers...),
		Topics: map[string][]kafka.OffsetRequest{c.cfg.KafkaTopic: {}},
	}
	for _, p := range partitions {
		listReq.Topics[c.cfg.KafkaTopic] = append(listReq.Topics[c.cfg.KafkaTopic], kafka.LastOffsetOf(p))
	}
	listResp, err := client.ListOffsets(ctx, listReq)
	if err != nil {
		info.Err = "list offsets: " + err.Error()
		c.setKafka(info)
		return
	}
	latest := map[int]int64{}
	for _, po := range listResp.Topics[c.cfg.KafkaTopic] {
		latest[po.Partition] = po.LastOffset
	}

	// 各消费组已提交 offset → lag
	for _, group := range c.cfg.KafkaGroups {
		ofResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			Addr:    kafka.TCP(brokers...),
			GroupID: group,
			Topics:  map[string][]int{c.cfg.KafkaTopic: partitions},
		})
		if err != nil {
			// 单个组失败不拖垮整体：该组 lag=null，其余照算
			log.Printf("[ops] kafka group %s offset fetch: %v", group, err)
			info.Lag[group] = nil
			continue
		}
		var lag int64
		ok := false
		for _, fp := range ofResp.Topics[c.cfg.KafkaTopic] {
			if fp.Error != nil || fp.CommittedOffset < 0 {
				continue
			}
			lag += latest[fp.Partition] - fp.CommittedOffset
			ok = true
		}
		if !ok {
			info.Lag[group] = nil // 组无提交记录：lag 未知
			continue
		}
		lagVal := lag
		info.Lag[group] = &lagVal
	}
	info.Available = true
	c.setKafka(info)
}

func (c *Collector) setKafka(info KafkaInfo) {
	c.mu.Lock()
	c.kafka = info
	c.mu.Unlock()
}

// splitComma 拆分逗号分隔列表。
func splitComma(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
