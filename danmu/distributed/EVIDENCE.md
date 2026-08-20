# EVIDENCE.md — Evidence / Claims 语义

> Evidence 页（`#/evidence`）是产品最重要的诚信页：它回答
> "**这些数字里，哪些真的被实验验证过，哪些只是设计目标**"。

```text
VERIFIED             —— 存储中存在支撑该 claim 的实验（跑出来的数字）
PARTIALLY VERIFIED   —— 有相关实验但只做到低于目标量级
CODE VERIFIED        —— 代码 / 单测 / 集成测试层面已实现并验证（但未经 benchmark）
TARGET               —— 架构目标；除非有实验支撑，绝不自动升级成 benchmark 结果
UNKNOWN              —— 状态未知（本仓库未使用；保留语义）
```

## 1. 铁律

- **任何 100k/1M 这类目标数字，在没有对应实验之前必须显示 TARGET。**
  Evidence 引擎**绝不**把"架构目标"自动升级为"benchmark 结果"。
- VERIFIED 只能来自实验存储（`<data-dir>/experiments/*.json` 中的 completed 实验）。
  每次请求 `/api/evidence` 都实时重算——跑个新实验刷新页面即可看到升级。
- 代码型 claim 用 **CODE VERIFIED** 单独标注（源码 + 单测/chaintest 可证，
  但它不是压测数字），不混进 VERIFIED 造成误解。

## 2. 验证算法（`distributed/ops/evidence.go`）

每个 claim 是一个静态定义 + 验证规则：

- **metric 型**（如 "10,000 simultaneous WebSocket connections"）：
  在 completed 实验里取该指标最大值；
  `max ≥ 阈值` → VERIFIED（关联到该实验，带上它的 commit/日期/环境）；
  有相关实验但未达标 → PARTIALLY VERIFIED（并写"最高已验证：N"）；
  完全没有实验 → TARGET（默认态）。
- **goal 型**（如 "1,000,000 concurrent connections"）：只认 `≥ 阈值` 的的 VERIFIED，
  否则永远是 TARGET——不会因为没有达到就降格成 PARTIAL（那是目标型 claim 的事后宣称）。
- **capability 型**（如 "Hot Room 一次完整运行被记录"）：跑过一次能测得该指标的
  hot-room 实验 → VERIFIED；有相关实验但没测到 → PARTIALLY；否则 TARGET。
- **code 型**（如 etcd 服务发现、Hub 256 分片锁、Kafka 全链路扇出）：默认
  **CODE VERIFIED**；若某个分布式实验记录了 `kafka available=true`（真 broker 跑出），
  "Kafka→Job→Comet.PushRoom 全链路"这条会自动升级为 VERIFIED。

所有判定确定性、无 LLM。

## 3. 当前 claim 清单与初始状态

| Claim | 类型/规则 | 无实验时 | 何时升级 VERIFIED |
|---|---|---|---|
| 10,000 simultaneous connections | metric `established≥10000` | TARGET | 有 ≥10000 建立的实验 |
| 1,000,000 concurrent connections | goal `established≥1e6` | TARGET | 有百万连接实验（否则永远 TARGET） |
| Hot Room 一次完整运行被记录 | capability（hot-room + p90） | TARGET | 跑过 hot-room 且测得 p90 |
| etcd 服务发现 | code（etcdreg 单测 + chaintest） | CODE VERIFIED | — |
| Hub 256 分片锁 | code（core/hub.go） | CODE VERIFIED | — |
| Kafka→Job→Comet.PushRoom 全链路 | code（chaintest） | CODE VERIFIED | 分布式实验 Kafka available=true |
| 单体降级本机广播闭环 | code（monolith/server） | CODE VERIFIED | — |
| 每个数字可追溯到实验记录 | code（本 lab 记录层） | CODE VERIFIED | — |

> 注意：仓库 README/PROJECT.md 里记录过"10k 连接 1.6ms"等历史压测，但**本 Evidence
> 页不读文档**——它只认实验存储。也就是说：文档数字要"转正"成 VERIFIED，
> 唯一途径是在当前版本上跑一次对应实验，再让引擎判定。这是有意的严格。

## 4. 在哪个页面看到 claim

- `#/evidence`：全部 claim 与图例。
- `#/experiments` → 某条历史 → Report：底部列出"本实验支撑的 claim 及其状态"
  （例如一个 200 连接实验会把 10k claim 标成 PARTIALLY VERIFIED，因为它把该 claim
  链接到具体实验，证据可追溯）。

## 5. 复现元数据（每个实验都带）

```text
workload：connections / rooms / message_rate / duration / target
环境：git commit + dirty、go version、OS/arch、CPU 数、内存、hostname
时间：created / started / finished（RFC3339）
架构：monolith | distributed
服务面（仅 distributed 且可观测）：comet/logic/job 计数与健康、Kafka available/lag、
      trace 采样数/完整率、代表性 trace
```

任何字段拿不到就 `null`，绝不编。Git 元数据由 `-repo-dir` 指向仓库目录采集；
`git` 不可用或目录不对时该项为 null（不影响实验运行）。
