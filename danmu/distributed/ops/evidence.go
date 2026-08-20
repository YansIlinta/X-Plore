package ops

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// Evidence 页的产品语义：区分
//
//	VERIFIED           —— 存储中存在支撑该 claim 的实验（跑出来的数字）
//	PARTIALLY VERIFIED —— 有相关实验但只做到低于目标量级
//	CODE VERIFIED      —— 代码/单测/集成测试层面已实现并验证（但未被 benchmark）
//	TARGET             —— 架构目标；除非有实验支撑，绝不自动升级成 benchmark 结果
//	UNKNOWN            —— 状态未知（本仓库未使用；保留语义与前端图例）
//
// 本引擎绝不把"目标是 X"自动变成"验证了 X"。

type ClaimStatus string

const (
	ClaimVerified     ClaimStatus = "VERIFIED"
	ClaimPartially    ClaimStatus = "PARTIALLY VERIFIED"
	ClaimCodeVerified ClaimStatus = "CODE VERIFIED"
	ClaimTarget       ClaimStatus = "TARGET"
	ClaimUnknown      ClaimStatus = "UNKNOWN"
)

// ClaimStatuses 图例顺序（前端展示用）。
var ClaimStatuses = []ClaimStatus{ClaimVerified, ClaimPartially, ClaimCodeVerified, ClaimTarget, ClaimUnknown}

// Claim 是一个"可追溯的 claim"：状态由实验存储驱动，绝不硬编码为 VERIFIED。
type Claim struct {
	ID           string               `json:"id"`
	Claim        string               `json:"claim"`
	Status       ClaimStatus          `json:"status"`
	Evidence     []string             `json:"evidence"`      // 实验 id 或代码/测试路径
	ExperimentID *string              `json:"experiment_id"` // 支撑它的最有力实验；无则 null
	Environment  *EnvironmentSnapshot `json:"environment,omitempty"`
	Commit       *string              `json:"commit"`
	Date         *string              `json:"date"`
	Notes        string               `json:"notes"`
}

// claimSeed 是 claim 的静态定义 + 验证规则。规则全部确定性、由实验存储驱动。
type claimSeed struct {
	id    string
	claim string
	notes string

	// 验证规则
	mode          string                 // "metric" | "code" | "capability"
	metric        string                 // metric 模式：取实验该指标的最大值
	minValue      float64                // metric 模式：达到多少才算 VERIFIED
	arch          string                 // 限制架构；"" = 任意
	preset        string                 // 限制 preset；"" = 任意
	customMatch   func(*Experiment) bool // 覆写 metric 判定（如 Kafka 可达）
	codeEvidence  []string
	defaultStatus ClaimStatus            // 无实验支撑时的降级状态（目标 → TARGET，代码 → CODE VERIFIED）
	upgradeOn     func(*Experiment) bool // 有实验满足时从 default 升级到 VERIFIED
}

// claimSeeds 是仓库现状下的真实 claim 清单。数字阈值是本项目文档中的目标/实测点。
var claimSeeds = []claimSeed{
	{
		id: "claim-10k-connections", claim: "10,000 simultaneous WebSocket connections",
		mode: "metric", metric: "connections_established", minValue: 10000,
		defaultStatus: ClaimTarget,
		notes:         "存在 conns=10000 且成功建立连接的实验时自动升级为 VERIFIED；此前只是仓库文档中的历史记录，不能冒充实验证据。",
	},
	{
		id: "claim-million-connections", claim: "1,000,000 concurrent WebSocket connections",
		mode: "goal", metric: "connections_established", minValue: 1_000_000,
		defaultStatus: ClaimTarget,
		notes:         "架构目标。除非真有百万连接实验（connections_established ≥ 1,000,000），绝不显示 VERIFIED；小于目标的实验也不事后宣称，保持 TARGET。",
	},
	{
		id: "claim-hot-room-observed", claim: "热门房间高扇出（Hot Room）一次完整运行被记录：延迟 / 错误 状态可见",
		mode: "capability", preset: "hot-room", metric: "p90_latency_us",
		defaultStatus: ClaimTarget,
		notes:         "跑过一次 hot-room preset 且测得 P90 后升级为 VERIFIED；目的是验证「热门房间为何更难」能被产品直接观察，而非自行声称。",
	},
	{
		id: "claim-etcd-service-discovery", claim: "etcd 服务发现（comet/logic 注册 + 前缀 Watch）",
		mode: "code", codeEvidence: []string{"etcdreg/ 单测（进程内 embed etcd：注册/发现/排序/Watch）", "cmd/chaintest（etcd 发现实测）"},
		defaultStatus: ClaimCodeVerified,
		notes:         "CODE VERIFIED：注册/发现/Watch 有 embed etcd 单测与 chaintest 佐证；尚未做大规模负载下的注册抖动验证。",
	},
	{
		id: "claim-hub-256-shard", claim: "Hub 使用 256 分片锁管理房间-连接映射",
		mode: "code", codeEvidence: []string{"core/hub.go（256 分片 RWMutex）"},
		defaultStatus: ClaimCodeVerified,
		notes:         "CODE VERIFIED：代码层面已实现；分片压力下的锁竞争需热房实验进一步验证。",
	},
	{
		id: "claim-kafka-fanout-chain", claim: "Kafka → Job → Comet.PushRoom → WS 全链路扇出",
		mode:          "code",
		codeEvidence:  []string{"logic/（kafka-go produce）", "job/（消费 + 定向 PushRoom）", "comet/（PushRoom→BroadcastToRoom）", "cmd/chaintest（delivered=1 实测）"},
		defaultStatus: ClaimCodeVerified,
		upgradeOn: func(e *Experiment) bool {
			return e.Architecture == ArchDistributed && e.Result.KafkaAvailable != nil && *e.Result.KafkaAvailable
		},
		notes: "chaintest 已实测 PushRoom→WS；CODE VERIFIED 表示代码与链路测试通过。当有分布式实验记录 Kafka available=true（真 broker 下跑出）时自动升级为 VERIFIED。",
	},
	{
		id: "claim-monolith-local-broadcast", claim: "单体无中间件降级本机广播的 WS 收发闭环",
		mode: "code", codeEvidence: []string{"monolith/server/（Redis/Kafka 缺失时本机广播）", "README 验证状态表"},
		defaultStatus: ClaimCodeVerified,
		notes:         "CODE VERIFIED：代码路径存在且仓库记录过实测（本机闭环）。",
	},
	{
		id: "claim-reproducible-meta", claim: "每个性能数字可追溯到实验记录（workload / commit / 环境 / 报告）",
		mode: "code", codeEvidence: []string{"distributed/ops/experiment_*.go（Realtime Systems Lab 记录层）"},
		defaultStatus: ClaimCodeVerified,
		notes:         "CODE VERIFIED：本 lab 层把 workload、git commit、环境快照、结果报告一并落盘；可复现性由 Evidence/报告页反过来可查证。",
	},
}

// EvidenceService 把 claimSeeds 与实验存储结合，算出每个 claim 的当前状态。
type EvidenceService struct {
	store *experimentStore
	repo  string

	mu    sync.Mutex
	env   *EnvironmentSnapshot
	envAt *time.Time
}

// NewEvidenceService 构造证据服务。
func NewEvidenceService(store *experimentStore, repo string) *EvidenceService {
	return &EvidenceService{store: store, repo: repo}
}

// List 计算全部 claim 的当前状态（确定性；每次调用按存储最新实验重算）。
func (s *EvidenceService) List() []Claim {
	completed, err := s.store.ListCompleted()
	if err != nil {
		completed = nil
	}
	env, when := s.envSnapshot()
	out := make([]Claim, 0, len(claimSeeds))
	for _, seed := range claimSeeds {
		out = append(out, s.eval(seed, completed, env, when))
	}
	return out
}

// envSnapshot 缓存环境采集（git 子进程只跑一次）。
func (s *EvidenceService) envSnapshot() (*EnvironmentSnapshot, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.env != nil && s.envAt != nil {
		return s.env, *s.envAt
	}
	e := captureEnvironment(s.repo)
	now := time.Now().UTC()
	s.env = e
	s.envAt = &now
	return e, now
}

// eval 求单个 claim 当前状态。
func (s *EvidenceService) eval(seed claimSeed, completed []*Experiment, env *EnvironmentSnapshot, when time.Time) Claim {
	c := Claim{ID: seed.id, Claim: seed.claim, Notes: seed.notes, Evidence: nil}

	sameArch := func(e *Experiment) bool {
		return seed.arch == "" || e.Architecture == seed.arch
	}
	samePreset := func(e *Experiment) bool {
		return seed.preset == "" || e.Preset == seed.preset
	}

	var considered []*Experiment
	for _, e := range completed {
		if sameArch(e) && samePreset(e) {
			considered = append(considered, e)
		}
	}

	match := func(e *Experiment) bool {
		if seed.customMatch != nil {
			return seed.customMatch(e)
		}
		if seed.mode == "capability" {
			// 能力型：能测出该指标即证明可观察此能力
			return resultMetric(e, seed.metric) != nil
		}
		v := resultMetric(e, seed.metric)
		return v != nil && *v >= seed.minValue
	}

	var supporters []*Experiment
	for _, e := range considered {
		if match(e) {
			supporters = append(supporters, e)
		}
	}

	switch seed.mode {
	case "code":
		// 先看是否有实验把它升级成 VERIFIED（如 Kafka 真 broker）
		if len(supporters) > 0 {
			support := s.best(supporters, seed.metric)
			s.applySupport(&c, support)
			c.Status = ClaimVerified
			c.Evidence = append(c.Evidence, "lab experiment "+support.ID)
			return c
		}
		if seed.upgradeOn != nil {
			for _, e := range considered {
				if seed.upgradeOn(e) {
					s.applySupport(&c, e)
					c.Status = ClaimVerified
					c.Evidence = append(c.Evidence, "lab experiment "+e.ID)
					return c
				}
			}
		}
		c.Status = ClaimCodeVerified
		c.Evidence = append([]string{}, seed.codeEvidence...)
		s.applyEnv(&c, env, when)
		return c

	case "capability":
		if len(supporters) > 0 {
			support := s.best(supporters, seed.metric)
			s.applySupport(&c, support)
			c.Status = ClaimVerified
			return c
		}
		if len(considered) > 0 && seed.customMatch == nil {
			// 有相关实验但没测出该指标 → PARTIAL
			c.Status = ClaimPartially
			c.Evidence = []string{"related experiments exist but metric not captured"}
			for _, e := range considered {
				c.Evidence = append(c.Evidence, e.ID)
			}
			s.applySupport(&c, considered[len(considered)-1])
			return c
		}
		c.Status = seed.defaultStatus
		return c

	default: // metric / goal
		if len(supporters) > 0 {
			support := s.best(supporters, seed.metric)
			s.applySupport(&c, support)
			c.Status = ClaimVerified
			c.Evidence = append(c.Evidence, support.ID)
			return c
		}
		if seed.mode == "goal" {
			// 目标型 claim 不做事后宣称：打不到目标就一直是 TARGET。
			c.Status = seed.defaultStatus
			return c
		}
		if len(considered) > 0 {
			// 有相关实验但未达标 → PARTIALLY VERIFIED，并说明最高测到多少。
			// 同时把它链接到最有力的实验，让"部分支撑"可追溯。
			hi := s.highest(considered, seed.metric)
			c.Status = ClaimPartially
			c.Evidence = []string{"highest measured below target"}
			if hi != nil {
				c.Evidence = append(c.Evidence, hi.ID)
				c.Notes = seed.notes + " 最高已验证：" + fmtValueN(hi, seed.metric)
				s.applySupport(&c, hi)
			}
			return c
		}
		c.Status = seed.defaultStatus
		return c
	}
}

// best 选支撑力最强的实验（按指标值降序，稳定取最先）。
func (s *EvidenceService) best(es []*Experiment, metric string) *Experiment {
	sorted := make([]*Experiment, len(es))
	copy(sorted, es)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := resultMetric(sorted[i], metric), resultMetric(sorted[j], metric)
		if a == nil {
			return false
		}
		if b == nil {
			return true
		}
		return *a > *b
	})
	if len(sorted) > 0 {
		return sorted[0]
	}
	return nil
}

func (s *EvidenceService) highest(es []*Experiment, metric string) *Experiment {
	return s.best(es, metric)
}

func (s *EvidenceService) applySupport(c *Claim, e *Experiment) {
	if e == nil {
		return
	}
	id := e.ID
	c.ExperimentID = &id
	if e.Environment != nil {
		c.Environment = e.Environment
	}
	if e.Environment != nil {
		c.Commit = e.Environment.GitCommit
	}
	if e.FinishedAt != nil {
		d := e.FinishedAt.UTC().Format("2006-01-02")
		c.Date = &d
	}
}

func (s *EvidenceService) applyEnv(c *Claim, env *EnvironmentSnapshot, when time.Time) {
	c.Environment = env
	if env != nil {
		c.Commit = env.GitCommit
	}
	d := when.Format("2006-01-02")
	c.Date = &d
}

func fmtValueN(e *Experiment, metric string) string {
	v := resultMetric(e, metric)
	if v == nil {
		return "N/A"
	}
	return numStr(*v)
}

func numStr(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
