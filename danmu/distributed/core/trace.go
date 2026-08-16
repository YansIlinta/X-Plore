package core

import (
	"hash/fnv"
	"net/http"
	"strconv"
	"sync"
)

// 消息级 trace：各环节对**同一批采样消息**记一条 span，ops 侧按 msg_id 汇聚成链路。
//
// 两个设计前提：
//
//  1. 采样决策必须全链路一致。各环节独立随机采样的话，同一条消息只会在个别环节留痕，
//     永远拼不出完整链路。这里用 msg_id 的确定性哈希，任何环节算出的结果都相同。
//  2. 判断"要不要记"本身不能有成本。msg_id 藏在 JSON 消息体里，job/comet 若为了取它
//     而反序列化每条消息，热路径代价就超过 trace 本身的价值了。所以 logic 把采样中的
//     msg_id 写进 Kafka header，job 再经 gRPC metadata 传给 comet——下游只读 header，
//     不碰 payload。
//
// 存储是有界环形缓冲：观测数据绝不能无限增长。缓冲满了丢最旧的，并计数丢弃量，
// 让 ops 能看出"这段链路我没采全"而不是默默给出残缺结论。
const (
	// TraceHeaderKey 是 logic→Kafka 的 header 键，值为 msg_id（仅采样中的消息带）。
	TraceHeaderKey = "x-danmu-trace"
	// TraceMetadataKey 是 job→comet 的 gRPC metadata 键，值为逗号分隔的 msg_id。
	// 用 metadata 而非 proto 字段：PushRoom 的契约不用动，旧版 comet 直接忽略。
	TraceMetadataKey = "danmu-trace-msgids"
)

// 各环节名。ops 按这个顺序渲染链路。
const (
	HopCometUplink  = "comet.uplink"  // comet 收到上行、调用 logic 返回后
	HopLogicProduce = "logic.produce" // logic 写入 Kafka 后
	HopJobConsume   = "job.consume"   // job 从 Kafka 消费到
	HopJobPush      = "job.push"      // job 扇出 PushRoom 完成
	HopCometDeliver = "comet.deliver" // comet 投递给本机房间连接
)

// Span 是单个环节的一次记录。TSNano 用 UnixNano：跨进程比较要求各节点时钟同步，
// 不同步时 ops 算出的段间耗时会失真——这是本方案已知的精度上限，没引入 OTel 也就没有
// 更好的时钟处理。
type Span struct {
	MsgID  string `json:"msg_id"`
	Hop    string `json:"hop"`
	Node   string `json:"node"`
	TSNano int64  `json:"ts_nano"`
	RoomID string `json:"room_id,omitempty"`
	Detail string `json:"detail,omitempty"` // 如 delivered=3、err=...
}

// TraceRecorder 是每个进程一份的采样器 + 有界 span 缓冲。零值不可用，走 NewTraceRecorder。
type TraceRecorder struct {
	node string
	rate uint32 // 1/rate 采样；0 = 关闭
	size int

	mu      sync.Mutex
	buf     []Span
	dropped int64 // 缓冲溢出丢弃的 span 数（不是采样丢弃）
}

// NewTraceRecorder 构造记录器。rate=0 关闭 trace（Sampled 恒 false，Record 直接返回）。
func NewTraceRecorder(node string, rate uint32, size int) *TraceRecorder {
	if size <= 0 {
		size = 512
	}
	return &TraceRecorder{node: node, rate: rate, size: size}
}

// Enabled 报告是否开启采样。
func (t *TraceRecorder) Enabled() bool { return t != nil && t.rate > 0 }

// Sampled 判定该 msg_id 是否命中采样。确定性：同一 msg_id 在任何进程结果一致。
func (t *TraceRecorder) Sampled(msgID string) bool {
	if !t.Enabled() || msgID == "" {
		return false
	}
	if t.rate == 1 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(msgID))
	return h.Sum32()%t.rate == 0
}

// Record 追加一条 span。调用方应先用 Sampled 过滤；这里不重复判定，
// 因为下游环节是靠 header 得知采样结果的，不该再算一次哈希。
func (t *TraceRecorder) Record(msgID, hop, roomID, detail string, tsNano int64) {
	if !t.Enabled() || msgID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.buf) >= t.size {
		// 丢最旧：trace 是"最近发生了什么"的窗口，旧数据没有保留价值
		copy(t.buf, t.buf[1:])
		t.buf = t.buf[:t.size-1]
		t.dropped++
	}
	t.buf = append(t.buf, Span{
		MsgID: msgID, Hop: hop, Node: t.node,
		TSNano: tsNano, RoomID: roomID, Detail: detail,
	})
}

// Recent 返回最近 n 条 span（时间升序）。n<=0 或超出则返回全部。
func (t *TraceRecorder) Recent(n int) []Span {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if n <= 0 || n > len(t.buf) {
		n = len(t.buf)
	}
	out := make([]Span, n)
	copy(out, t.buf[len(t.buf)-n:])
	return out
}

// Dropped 返回因缓冲溢出丢弃的 span 数。
func (t *TraceRecorder) Dropped() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dropped
}

// TraceLimit 解析 /api/v1/traces 的 ?limit=N。默认 200、上限 1000：
// 三个服务的端点共用同一套上限，省得各写一份。
func TraceLimit(r *http.Request) int {
	const def, max = 200, 1000
	v := r.URL.Query().Get("limit")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// Stats 供各服务的 /api/v1/traces 一并返回，让 ops 知道数据完整性。
func (t *TraceRecorder) Stats() map[string]any {
	if t == nil {
		return map[string]any{"enabled": false}
	}
	return map[string]any{
		"enabled":  t.Enabled(),
		"rate":     t.rate,
		"buffered": len(t.Recent(0)),
		"dropped":  t.Dropped(),
	}
}
