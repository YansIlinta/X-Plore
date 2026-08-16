package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/YansIlinta/danmu-distributed/core"
)

// 消息 trace 汇聚：各服务只在本机存自己的 span（有界环形缓冲），ops 定期把它们拉回来，
// 按 msg_id 拼成跨服务链路。
//
// 为什么是"拉"不是"推"：推需要业务服务感知 ops 的地址、失败要重试要缓冲，等于在消息链路
// 旁边再建一条链路。拉的话 ops 挂了业务侧毫无感知——这和 ops 整体"旁路观测者"的定位一致。
//
// 代价是可能漏采：span 缓冲在服务侧是有界的，拉取间隔内溢出的就永远看不到了。各服务
// /api/v1/traces 会返回 dropped 计数，ops 原样透传，让"我没采全"这件事是可见的。

// traceMaxKept 是 ops 侧保留的 msg_id 条数上限（超出丢最早出现的）。
const traceMaxKept = 200

// 完整链路应有的环节。少任何一个都说明消息在那一段之前就停了（或采样窗口没覆盖到）。
var expectedHops = []string{
	core.HopCometUplink,
	core.HopLogicProduce,
	core.HopJobConsume,
	core.HopJobPush,
	core.HopCometDeliver,
}

// Trace 是一条消息的跨服务链路。
type Trace struct {
	MsgID      string      `json:"msg_id"`
	RoomID     string      `json:"room_id,omitempty"`
	Spans      []core.Span `json:"spans"`       // 按时间升序
	DurationMS float64     `json:"duration_ms"` // 末段 − 首段；跨节点，受时钟偏差影响
	Complete   bool        `json:"complete"`    // 五个环节是否齐全
	MissingHop []string    `json:"missing_hops,omitempty"`
}

// traceStore 是 ops 侧的汇聚缓冲：msg_id → 链路，按首次出现顺序淘汰。
type traceStore struct {
	mu      sync.Mutex
	byID    map[string]*Trace
	seen    map[string]map[string]bool // msg_id → "hop@node" 去重集
	order   []string                   // 首次出现顺序
	sources map[string]any             // 各节点自述的 trace 状态（enabled/rate/dropped）
}

func newTraceStore() *traceStore {
	return &traceStore{
		byID:    make(map[string]*Trace),
		seen:    make(map[string]map[string]bool),
		sources: make(map[string]any),
	}
}

// add 并入一条 span，重复的（同 msg_id + 同环节 + 同节点）忽略。
func (s *traceStore) add(sp core.Span) {
	if sp.MsgID == "" || sp.Hop == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := sp.Hop + "@" + sp.Node
	if s.seen[sp.MsgID] == nil {
		s.seen[sp.MsgID] = make(map[string]bool)
		s.byID[sp.MsgID] = &Trace{MsgID: sp.MsgID}
		s.order = append(s.order, sp.MsgID)
		// 淘汰最早出现的，保持有界
		for len(s.order) > traceMaxKept {
			old := s.order[0]
			s.order = s.order[1:]
			delete(s.byID, old)
			delete(s.seen, old)
		}
	}
	if s.seen[sp.MsgID][key] {
		return
	}
	s.seen[sp.MsgID][key] = true

	t := s.byID[sp.MsgID]
	if t == nil { // 刚被淘汰（缓冲极小时可能发生）
		return
	}
	if t.RoomID == "" {
		t.RoomID = sp.RoomID
	}
	t.Spans = append(t.Spans, sp)
}

// setSource 记录某节点的 trace 自述状态。
func (s *traceStore) setSource(node string, stats any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources[node] = stats
}

// list 返回最近 n 条链路（最新在前），并现算 duration/completeness。
func (s *traceStore) list(n int) []Trace {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.order) {
		n = len(s.order)
	}
	out := make([]Trace, 0, n)
	// order 是首次出现顺序，取末尾 n 条再倒序 → 最新在前
	for i := len(s.order) - 1; i >= 0 && len(out) < n; i-- {
		t := s.byID[s.order[i]]
		if t == nil {
			continue
		}
		out = append(out, finalize(*t))
	}
	return out
}

func (s *traceStore) sourceStats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.sources))
	for k, v := range s.sources {
		out[k] = v
	}
	return out
}

// finalize 排序 span 并算出耗时与完整性。传值拷贝，不动 store 里的原对象。
func finalize(t Trace) Trace {
	spans := make([]core.Span, len(t.Spans))
	copy(spans, t.Spans)
	sort.Slice(spans, func(i, j int) bool { return spans[i].TSNano < spans[j].TSNano })
	t.Spans = spans

	if len(spans) > 1 {
		t.DurationMS = float64(spans[len(spans)-1].TSNano-spans[0].TSNano) / 1e6
	}

	have := make(map[string]bool, len(spans))
	for _, sp := range spans {
		have[sp.Hop] = true
	}
	for _, h := range expectedHops {
		if !have[h] {
			t.MissingHop = append(t.MissingHop, h)
		}
	}
	t.Complete = len(t.MissingHop) == 0
	return t
}

// traceLoop 周期性从各实例拉 span。与主采集同频，复用 Snapshot 里已发现的实例地址。
func (c *Collector) traceLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.traceOnce()
		}
	}
}

// traceOnce 并发拉取所有健康实例的 /api/v1/traces 并并入 store。
// 拉不到就跳过：trace 是尽力而为的观测，不该影响任何其他判定。
func (c *Collector) traceOnce() {
	var addrs []string
	for _, svc := range c.Snapshot().Services {
		for _, it := range svc.Instances {
			if it.Healthy {
				addrs = append(addrs, it.HTTPAddr)
			}
		}
	}

	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			node, stats, spans, err := c.fetchTraces(addr)
			if err != nil {
				return
			}
			if node != "" {
				c.traces.setSource(node, stats)
			}
			for _, sp := range spans {
				c.traces.add(sp)
			}
		}(addr)
	}
	wg.Wait()
}

// fetchTraces 拉单个实例的 span。comet 的端点要 Bearer，logic/job 不要，统一带上。
func (c *Collector) fetchTraces(httpAddr string) (node string, stats any, spans []core.Span, err error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/api/v1/traces?limit=500", nil)
	if err != nil {
		return "", nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil, nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body struct {
		Node  string      `json:"node"`
		Stats any         `json:"stats"`
		Spans []core.Span `json:"spans"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", nil, nil, err
	}
	return body.Node, body.Stats, body.Spans, nil
}
