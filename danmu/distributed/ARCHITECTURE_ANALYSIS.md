# ARCHITECTURE_ANALYSIS.md — Ops Console 仓库勘察报告

> Phase 1 产物，**已于 2026-08-16 更新对齐当前工作区代码**。勘察对象：本仓库 `distributed/`（goim 式分布式弹幕），以及兄弟目录 `../monolith/`（loadtest 与 ClickHouse consumer 源码）。
> 所有结论均来自真实代码，标注 `文件:行号`。文末区分 **[existing]**（现有能力）与 **[new]**（需新增的观测能力），并标注每项的落地状态。
>
> **本次更新的性质**：原报告写于观测面改造之前，第 3/4/5/6/12 节已按实现结果重写。凡新增能力均标 ✅ **[已实现]**，未落地的保留原文并标 ⬜ **[未做]**。

## 1. 组件与端口

| 组件 | 源码 | 默认端口 | 注册到 registry | 备注 |
|------|------|----------|----------------|------|
| registry | `cmd/registry/main.go` | `:7350`（HTTP） | — | `-ttl 10s` 租约 |
| logic | `logic/main.go` | `:7400`（gRPC）、`:7410`（HTTP 观测 ✅新增） | service=`logic` + `logic-http` ✅ | `-id` 默认 `logic1` |
| job | `job/main.go` | `:7420`（HTTP 观测 ✅新增），无 RPC 监听 | service=`job-http` ✅（原本完全不注册） | 消费组 `danmu-job` |
| comet | `comet/main.go` | `:8080` WS/HTTP，`:7500` gRPC，`:6060` pprof | service=`comet` + `comet-http` ✅ | `-id` 默认 `comet1` |
| ops ✅新增 | `cmd/ops/main.go` + `ops/` | `:7900`（HTTP） | 不注册（纯消费方） | 旁路观测服务，见 §13 |
| consumer（落库） | `../monolith/consumer/` | 无监听端口 | 不注册 | 消费组 `danmu-storage` |
| kafka | compose `bitnami/kafka:3.7` | `:9092`（仅内网） | — | KRaft，auto-create，10 分区 |
| clickhouse | compose `clickhouse:24.3-alpine` | `:9000` | — | 落库 |
| nginx | compose `nginx:alpine` | 宿主 `:8088` → 容器 80 | — | 一致性哈希分流到 comet1/2 |

compose 内实际部署（`docker-compose.goim.yml`）：kafka、clickhouse、registry、logic（宿主 7410）、job（宿主 7420）、comet1（宿主 8080）、comet2（宿主 8081）、consumer、nginx（宿主 8088）、✅ ops（宿主 7900）。`Dockerfile.goim` 同时编出 ops 二进制。

**`*-http` 注册名约定**：业务服务把 RPC 地址注册为 `comet`/`logic`（供链路发现），把 HTTP 观测地址额外注册为 `comet-http`/`logic-http`/`job-http`（仅供 Ops Console 发现）。两者分开注册，观测面的增删不会污染消息链路的服务发现。地址推导统一走各自的 `advertiseHTTPAddr()`（`comet/main.go:363`、`logic/main.go:194`、`job/main.go:309`）：显式 `-advertise-http` 优先，否则取 advertise 主机名 + HTTP 端口，advertise 为空则主机名用 `localhost`。

⚠️ **job 必须显式给 `-advertise-http`**：它没有 `-advertise` 参数，缺省推导只能得到 `localhost:7420`，注册进 registry 后 ops 会拨到自己容器的 localhost。comet/logic 因为有 `-advertise`，缺省推导恰好正确，但 compose 里仍写全以免日后改 advertise 时静默失效。

## 2. 通信契约

**重要事实**：服务间 RPC 是 **gRPC**（`pb/danmu.proto`）。

> ✅ 2026-08-16：原先这里写的是「不是 minirpc 自有协议，minirpc 只贡献 registry 与 lb.Ring，XClient/熔断器/自定义协议帧是教学骨架、线上未使用」。那批教学骨架（`protocol`/`server`/`client`/`xclient`/`breaker`，1546 行含测试）已整体删除，`registry` 与 `lb` 上移为主模块的顶层包（`registry/`、`lb/`），`minirpc` 独立 module 与 go.mod 里的 `replace` 一并消失。仓库现在只有一个 Go module。

- `LogicService.OnMessage(req{room_id,uid,content,client_ts,client_ts_nano,source_comet,offset_ms}) → resp{msg_id,filtered}` — comet→logic，`comet/main.go:120` 调用（3s 超时），`logic/main.go:51-82` 实现。
- `CometService.PushRoom(req{room_id,payload}) → resp{delivered}` — job→comet，`job/main.go:106-120` 扇出（每 comet 2s 超时），`comet/main.go:91-97` 实现。
- msg_id 格式：`<instanceID>-<uint64 自增>`，logic 生成（`logic/main.go:46-48`）；standalone 模式 comet 自生成。**非全局有序，重启归零**。

**proto 未改动**：观测面全部走 HTTP 旁路，`pb/danmu.proto` 的两个 RPC 契约与消息体一字未动。

消息体 `core.Message`（`core/message.go:9-21`）自带 trace 素材：`msg_id, room_id, uid, client_ts, client_ts_ns, server_ts, source_server, offset_ms`。

## 3. 现有 HTTP API 清单

### comet（`comet/main.go:322-329`；改造前是唯一有 HTTP 面的业务服务，现在 logic/job 也各有一个）

| 端点 | 鉴权 | 说明 |
|------|------|------|
| `GET /health` | 无 | **静态** `{"status":"ok"}`，不检查任何依赖 |
| `GET /ws` | token | WebSocket 入口 |
| `GET /metrics` | 无 | Prometheus 格式（promhttp 默认 registry，含 go_* / process_*） |
| `GET /api/v1/stats` | Bearer `DANMU_AUTH_TOKEN` | `server_id, conn_count, room_count, qps, dropped_uplink, standalone, heap_mb, goroutines, uptime_ms`（`comet/api.go:58-72`）。✅ `conn_count`/`room_count` 已改读 O(1) 原子计数（`comet/api.go:63-64`），不再扫 256 分片 |
| `GET /api/v1/rooms?page&limit` | 同上 | 房间列表（上限 100/页），**每次全量枚举再内存分页**（`comet/api.go:74-87`） |
| `GET /api/v1/traces?limit=N` ✅新增 | 同上 | 本机采样到的 trace span（`comet/api.go:58-66`），见 §15 |
| `POST /api/v1/session-token` | 同上 | 签发会话令牌 |
| `GET /` | 无 | 静态托管 `web/`（联调页） |
| `/debug/pprof/*` | 无 | 独立端口 `:6060` |

### registry（`registry/registry.go:34-47`）

| 端点 | 说明 |
|------|------|
| `POST /register?service=X&addr=Y` | 注册=心跳，刷新租约（ttl 10s，客户端 ttl/3≈3.3s 续租，`registry/registry.go:109-128`） |
| `GET /services?service=X` | 存活地址 JSON 数组（惰性清过期、排序输出，`registry/registry.go:92-104`） |
| `GET /services`（无参）✅新增 | 返回**全部服务**的存活地址 map `{service: [addr...]}`，同样惰性清过期、地址排序；空 service 不出现在结果里（`registry/registry.go:70-90`）。这是 Ops Console 枚举拓扑的入口——原来不传 service 只会返回 `[]` |
| `GET /health` ✅新增 | `{"status":"ok"}`。registry 自身无外部依赖，活着即健康（`registry/registry.go:40-43`） |

仍然**没有注销接口**（有意设计：进程崩溃时来不及注销，活性只能靠心跳+租约过期）。数据结构仍是 `map[service]map[addr]expireAt` + 单 Mutex，无实例元数据。

新增行为有测试覆盖：`registry/registry_test.go`（+50 行）。

### logic（`logic/main.go:137-163`）✅新增

独立 goroutine 起 HTTP 观测面（`-http-addr` 默认 `:7410`），与 gRPC 面互不阻塞。**无鉴权**（观测面只读、无敏感数据；与 comet 的 Bearer 不一致，见 §14 遗留问题）。

| 端点 | 字段 |
|------|------|
| `GET /health` | `{"status":"ok"}`（静态，同 comet，不检查 Kafka） |
| `GET /api/v1/stats` | `server_id, uptime_ms, onmessage_total, filtered_total, onmessage_errors, kafka_produce_errors, goroutines, heap_mb` |
| `GET /api/v1/traces?limit=N` ✅新增 | `node, stats{enabled,rate,buffered,dropped}, spans[]`，见 §15 |

计数器都是 `atomic.Int64`，挂在 `logicServer` 上（`logic/main.go:38-43`），在 `OnMessage` 里递增（`:52`、`:55`、`:73`、`:78`）。`kafka_produce_errors` 特殊：Async writer 的失败不走返回值，所以在 `kafka.Writer.ErrorLogger` 回调里累加（`logic/main.go:109-112`），由 main 注入指针。

### job（`job/main.go:171-199`）✅新增

同样是独立 goroutine（`-http-addr` 默认 `:7420`），**刻意放在 Kafka 消费循环启动之前**监听——Kafka 不可达时观测面依然能回答问题。

| 端点 | 字段 |
|------|------|
| `GET /health` | `{"status":"ok"}` |
| `GET /api/v1/stats` | `server_id, uptime_ms, consumed_total, push_ok_total, push_err_total, delivered_total, comets[], goroutines, heap_mb` |
| `GET /api/v1/traces?limit=N` ✅新增 | 同 logic，见 §15 |

计数器是包级 `atomic.Int64`（`job/main.go:34-39`），在消费循环（`:254`）与 `pushRoom`（`:113-118`）里递增。`comets[]` 来自新增的 `cometPool.addrs()`（`job/main.go:123-131`，读锁快照）——这是唯一能看到「job 眼里的 comet 列表」的地方，用来诊断扇出漏推。

### consumer（落库）

**仍无任何 HTTP listener、无 /health、无 /metrics**，唯一观测信号是 stdout 日志。ClickHouse 落库链路目前在 Ops Console 上是盲区。

### ops（`ops/api.go:17-25`）✅新增

全部只读，读 Collector 的最新快照，**无鉴权**（旁路服务，默认只监听本机 `:7900`）。

| 端点 | 说明 |
|------|------|
| `GET /api/health` | ops 自身活性 |
| `GET /api/overview` | 系统总览：`health`/`health_detail`、`active_connections`、`active_rooms`、`msg_in_rate`、`msg_out_rate`、`comet_instances{total,healthy}`、`kafka` |
| `GET /api/services` | 按组件分组的实例列表，透传各实例 `/api/v1/stats` 原始 JSON |
| `GET /api/topology` | 拓扑 `nodes[]`/`edges[]`，节点带 `healthy`（未观测的 nginx/clients 为 `null`） |
| `GET /api/events?limit=N` | 最近事件（默认 100，上限 `eventBufferSize=500`） |
| `GET /api/traces?limit=N` ✅新增 | 汇聚好的跨服务消息链路（默认 50，上限 `traceMaxKept=200`）+ 各节点采样自述，见 §15 |

所有响应都带 `mock` 与 `ts` 字段。

## 4. Metrics 现状（`core/metrics.go:9-33`）

Prometheus 指标，**仍然只有 comet 暴露 `/metrics`**（logic/job 新增的 HTTP 面只有 `/health` + `/api/v1/stats`，没接 promhttp）：

| 指标 | 类型 | 说明 / 缺陷 |
|------|------|------------|
| `danmu_connections_total` | Counter | **只增不减**（RemoveClient 不扣），是累计值不是在线数 |
| `danmu_messages_total{direction=in,out}` | CounterVec | 上行/下行计数 |
| `danmu_broadcast_latency_seconds` | Histogram | **仅 standalone 模式打点**（`comet/main.go:154`），分布式 PushRoom 路径恒空 |
| `danmu_broadcast_dropped_total` | Counter | sendCh 满丢弃计数 |

刻意不带 room_id 标签（防基数爆炸）。**`core/metrics.go` 本次未改动**，四个指标的上述缺陷全部原样保留——特别是 `danmu_connections_total` 仍是只增不减的累计值，不能当在线数用。

在线连接数改从 `/api/v1/stats` 的 `conn_count` 取（现在是 O(1) 原子读，见 §5）。**没有新增 Prometheus Gauge**：ops 侧走 `/api/v1/stats` 而非 `/metrics` 拿在线数，`/metrics` 只被用来算消息速率（`danmu_messages_total` 的 Δcounter/Δt，`ops/collector.go:382-424`；识别到计数器回退即判定进程重启，本轮不出速率）。

## 5. Hub / Room / Client 数据结构（`core/hub.go`）

- 256 分片（`fnv32(roomID) % 256`），每片 `RWMutex + map[room]map[uid]*Client`。
- ✅ 新增 `connCount` / `roomCount` 两个 `atomic.Int64`（`hub.go:25-27`），在 `AddClient`（`hub.go:70-79`）/ `RemoveClient`（`hub.go:91-99`）里与分片 map 同步维护；`OnlineCount()` / `RoomCountFast()` O(1) 读（`hub.go:243`、`hub.go:246`）。
- **已知不精确**（代码注释里写明并接受）：`KickClient` / `CloseRoom` 直接删分片条目、不走 `RemoveClient`，这两条路径下原子计数会永久偏高。判断是「管理操作低频，用近似换 stats 零扫描」。旧的 `GetConnCount()` / `GetRoomCount()`（`hub.go:217-237`，256 分片全扫）保留，需要精确值时仍可用——**但目前没有任何调用方**，也没有把两者做对账的机制。
- 同 uid 顶号不重复计数：顶号时旧连接走 `cancel()` 退出、不会再触发一次 `RemoveClient` 扣减，所以 `AddClient` 只在「该 uid 原本不存在」时 +1。
- `GetRoomList()`：仍是全表扫描（`hub.go:148`），`/api/v1/rooms` 的内存分页未改。
- 已有但未暴露 HTTP 的管理能力：`CloseRoom`（`hub.go:175`）、`KickClient`（`hub.go:195`）。
- Client：sendCh cap 256；令牌桶 20/s 容量 50；**限流命中无 metric**。

## 6. Registry / 服务发现

- 数据结构：`map[service]map[addr]expireAt`，单 Mutex。**仍无元数据**（无实例 id、无启动时间），advertise 地址是唯一标识。无注销接口（有意设计）。
- ✅ 观测地址通过**并列注册一个 `*-http` 服务名**解决，而不是给 registry 加元数据字段——registry 的数据模型完全没动。代价：ops 侧要把 `comet-http` 的实例和 `comet` 的实例重新对上，做法是**按主机名匹配**（`ops/collector.go:315` `rpcAddrOf`，如 `comet1:8080` ↔ `comet1:7500`）。同机多实例（同主机名不同端口）会对错，目前部署形态下不触发。
- comet 侧 `logicPool`：每 3s 刷新 registry，按 roomID 一致性哈希选 logic（`comet/logicpool.go`）；`logicPool.empty()` 已实现但**仍未接入 health**（comet `/health` 依旧是静态 ok）。
- job 侧 `cometPool`：每 3s 刷新，扇出全部 comet（`job/main.go:58-93`），实例增删有日志；✅ 新增 `addrs()` 供 stats 暴露当前列表。
- 三个服务的 `*-http` 心跳都是 ttl 10s / 续租 3.3s，与 RPC 注册同参数。job 原先不注册任何东西，现在为观测起了一个 `registryKeepAlive` 包装（`job/main.go:304-306`）。

## 7. Kafka

| 项 | 值 |
|----|----|
| 广播 topic | `danmu-broadcast`（logic produce，key=roomID，Hash 保序，BatchSize 500/10ms，Async，RequireOne） |
| 分区 | 10（compose auto-create） |
| 消费组 | `danmu-job`（job，CommitInterval 1s）、`danmu-storage`（落库 consumer） |
| lag 监控 | ✅ 已实现，在 **ops 侧**（`ops/collector.go:543-622`）：ListOffsets 取各分区最新 offset − OffsetFetch 取消费组已提交 offset，按 topic 求和。观测组 = `danmu-job`、`danmu-storage`（`cmd/ops/main.go:42`）。独立循环、周期 `poll*3`，不阻塞主采集。业务代码零改动 |

lag 的失败语义是明确的「不伪造」：任一步失败 → `KafkaInfo.Available=false` 且 lag 全 `null`；单个消费组失败只让该组 `null`，其余照算；组无提交记录同样是 `null` 而不是 0。

## 8. ClickHouse

`danmu_history(room_id, uid, content, client_ts, server_ts, source_server, event_date)`，MergeTree，`ORDER BY (room_id, server_ts)`（`../monolith/consumer/db.go:43-55`）。现成分页查询 SQL（`db.go:94-123`）可支撑历史消息面板。

## 9. loadtest（`../monolith/loadtest/main.go`，536 行，`package main`）

- 不可 import，复用方式 = **子进程**。
- flags：`-server`（逗号分隔多地址）、`-conns`、`-rooms`、`-rate`、`-duration`、`-ramp`、`-token`、`-output-json`、`-output-csv`、`-pprof`。
- **stdout 每秒快照行**（conns/sendQPS/recvQPS/e2e p50/p90/p99/errs/goroutines/heap）→ 可管道解析驱动实时曲线。
- `--output-json` 输出 `{summary, snapshots[]}` → 结束后归档报告。
- E2E 延迟用 HDR Histogram（按 `client_ts_ns` 回算）。

## 10. chaintest（`cmd/chaintest/main.go`）

现成四步链路自检：registry 发现 → Logic.OnMessage → PushRoom→WS 下行 → 上行后抓 comet `/metrics` 验证计数递增。退出码非零=失败。可直接作为 Ops Console「链路自检」功能的后端。

## 11. 日志

全标准库 `log`，带 `[comet]/[logic]/[job]/[registry]/[kafka]` 前缀，无结构化、无级别。成功路径**零逐条日志**，无 OpenTelemetry。

**本次未改动**：消息 trace 最终没走日志方案（见 §15），所以日志现状与改造前一致。

## 12. 可复用能力 vs 缺失能力

### [existing] 可直接消费
- registry `GET /services?service=comet|logic` → 拓扑/实例清单数据源
- comet `/metrics`（Prom 抓取）、`/api/v1/stats`、`/api/v1/rooms`（Bearer）
- comet `/health`（弱活性）、pprof
- `core.Message` 时间戳字段（client_ts / server_ts / source_server）→ trace 素材
- loadtest 子进程 + stdout 快照 + JSON 报告 → 压测集成
- chaintest → 链路自检
- ClickHouse `danmu_history` + 分页查询 → 历史消息
- Kafka（kafka-go 已在 go.mod）→ OffsetFetch/ListOffsets 算 lag，无需新依赖

### [new] 需新增的最小观测能力 —— 落地状态

| # | 能力 | 状态 | 实现位置 / 说明 |
|---|------|------|----------------|
| 1 | logic / job 补 HTTP 观测面 | ✅ 已实现 | `logic/main.go:137-163`（:7410）、`job/main.go:171-199`（:7420）。计数项与原计划一致，另加了 `job` 的 `comets[]` 与 `logic` 的 `filtered_total` |
| 2 | registry 补 `/health`；`/services` 无参列全部 | ✅ 已实现（两项都做了） | `registry/registry.go:40-43`、`registry/registry.go:70-90`。无参返回的是完整 `{service: [addr]}` map，比原计划的「只列 service 名」更进一步 |
| 3 | comet 在线连接 O(1) 计数 | ✅ 已实现，**但不是 Prom Gauge** | `core/hub.go:25-27`/`243`/`246`，只接进了 `/api/v1/stats`；`core/metrics.go` 未动，`danmu_connections_total` 的只增不减缺陷仍在 |
| 4 | Kafka lag（ops 侧算，不改业务） | ✅ 已实现 | `ops/collector.go:543-622`，独立循环 `poll*3` |
| 5 | Message trace（采样 + ops 汇聚） | ✅ 已实现，**但方案与原计划不同** | 见 §15。原计划是"打结构化日志、ops 汇聚查询"，实际改成**各服务内存环形缓冲 + `/api/v1/traces` 端点 + ops 轮询拉取**：日志方案要么得给 ops 挂 docker socket 读别人 stdout，要么得引入日志采集器，两者都超出"轻量"的边界。§11 的日志现状因此未变 |
| 6 | 事件流（diff + 有界 ring buffer） | ✅ 已实现 | `ops/collector.go:478-524` diff，`eventBuffer` 上界 500（`ops/collector.go:92-121`）。事件类型见 §13 |
| 7 | Ops 服务本体 | 🟡 **部分** | `cmd/ops` + `ops/` 已建，REST 5 个端点可用；**SSE 未做**（前端只能轮询），**loadtest 子进程控制未做**（`Event.Kind` 已预留 `loadtest` 取值但无产出方） |

### 明确不做 —— 复核结果

- 不改 minirpc 协议/帧格式；不把 XClient/熔断器接入线上链路 —— ✅ 观测面改造期间遵守（当时只动了 `registry`）。**该约束已于 2026-08-16 失效**：协议/帧格式与 XClient/熔断器整体删除，不再存在，见 §2。
- 不给 metric 加 room_id 标签 —— ✅ 遵守，`core/metrics.go` 完全未改。
- 不实现 kill service / 删 topic / 清库 / 踢全站等危险动作 —— ✅ 遵守，ops 全部端点只读，无任何写/控制动作。
- 不为 UI 虚构数据；拿不到的数据显示 `N/A` —— ✅ 遵守且做成了强约束，见 §13「数据真实性」。

## 13. Ops Console 后端（✅ 新增，`cmd/ops` + `ops/`）

约 1200 行：`ops/collector.go` 642、`ops/api.go` 236、`ops/collector_test.go` 164、`ops/mock.go` 98、`cmd/ops/main.go` 65。

**定位**：纯旁路观测者，只读 + 聚合，不在消息链路上，自身挂掉不影响弹幕系统（`ops/collector.go:1-7` 写明）。

**采集链路**（`Collector.pollOnce`，`ops/collector.go:189`）：
1. `GET registry /services`（无参）拿全部服务 map；registry 掉线则**沿用上一轮实例清单继续探测**（`ops/collector.go:207`），不至于一掉线整个面板变空。
2. 并发探测各 `*-http` 实例（`probeServices`，`ops/collector.go:263`）：`/health` 判活 → `/api/v1/stats` 取原始 JSON → comet 额外抓 `/metrics` 算速率。httpClient 超时 2s，「任何实例慢都不能拖垮采集循环」。
3. `rpcAddrOf` 按主机名把 HTTP 地址和 RPC 地址对上（见 §6 的同机多实例限制）。
4. 健康判定 `evalHealth`（`ops/collector.go:429`）：registry 不可达 / 所有 comet 不可达 / Kafka 启用观测但不可用 → `critical`；任一实例不可达 → `degraded`；否则 `healthy`。
5. `diffEvents`（`ops/collector.go:479`）对比前后快照产出事件：实例出现/消失、registry 掉线/恢复、健康状态翻转。首轮不发事件（全是「出现」，无信息量）。

**默认参数**（`cmd/ops/main.go:20-27`）：`-addr :7900`、`-registry http://localhost:7350`、`-kafka localhost:9092`、`-poll 2s`、`-mock false`。`-token` 为空时依次回退 `DANMU_AUTH_TOKEN` → 硬编码 `danmu-secret-token`（回退时打 WARN 日志）。

**数据真实性约定**（`ops/collector.go:5-7`，这是整个 ops 的设计红线）：
- 默认模式**严禁伪造数据**，拿不到的值一律 `null`，前端显示 N/A。
- 只有显式 `-mock` 才喂假数据，且每个响应都带 `"mock": true`（`ops/mock.go`）。
- 具体体现：无速率样本时 `msg_in_rate`/`msg_out_rate` 保持 `null` 而非 0（`ops/api.go:68-71`）；nginx/clients 无观测端点，拓扑节点 `healthy: null`；Kafka 任一步失败 lag 全 `null`。
- 探测到「健康但 stats 拉不到」（如鉴权配置不一致）标记 `err` 但**不判死**（`ops/collector.go:346`）。

**测试**：`ops/collector_test.go` 164 行。

## 14. 本轮改造的遗留问题

按影响排序，都是有意接受或尚未处理的，不是 bug 报告：

1. ~~**compose 未纳入 ops**~~ —— ✅ 2026-08-16 已解决：`Dockerfile.goim` 加编 ops 二进制，compose 加 ops 服务（宿主 7900）、映射 logic `:7410` / job `:7420`、四个业务服务补齐 `-advertise-http`。**尚未在真机容器里跑通**（当时 Docker daemon 未启动，只做了 `docker compose config` 语法校验）；等价接线已用宿主进程验证：registry 列出全部 5 个服务名，ops 正确发现 comet/logic/job 三组件并对上 RPC 地址。首次 `docker compose up` 时仍需确认一次。
2. **观测面鉴权不一致**：comet `/api/v1/stats` 要 Bearer，logic/job 的同名端点裸奔。ops 对两者都发 Bearer 头（多余但无害）。若 logic/job 端口暴露到不可信网络，stats 是裸露的。
3. **原子计数与全扫计数无对账**：`OnlineCount()` 在 Kick/CloseRoom 后会偏高（§5），而精确的 `GetConnCount()` 已无调用方，偏差无法被发现。
4. **`*-http` 按主机名对齐**：同主机多实例会把 RPC 地址对错（§6）。
5. **consumer 仍是盲区**：ClickHouse 落库进程无观测端点，只能靠 `danmu-storage` 的 Kafka lag 间接推断死活。
6. **`/health` 全是静态的**：comet/logic/job/registry 四家的 `/health` 都不检查依赖，只证明进程还在。`logicPool.empty()` 这类现成信号仍未接入。这次排查 trace 时被它坑了一下：Kafka 不可达、900 条消息全部写失败，四个 `/health` 依然全绿。
7. ~~**消息级 trace 完全缺失**~~ —— ✅ 2026-08-16 已实现，见 §15。但**全五段链路尚未端到端验证过**（本机无 Kafka），仅两个跨进程接缝有单测覆盖。

## 15. 消息 trace（✅ 新增，`core/trace.go` + 三服务接入 + `ops/trace.go`）

回答「这条弹幕卡在哪一段」。五个环节：

```
comet.uplink → logic.produce → job.consume → job.push → comet.deliver
```

### 两个决定方案形态的约束

**1）采样决策必须全链路一致。** 各环节独立随机采样的话，同一条消息只会在个别环节留痕，永远拼不出完整链路。所以采样是 msg_id 的确定性哈希（`core/trace.go` `Sampled`，fnv32 % rate），任何进程算出的结果相同。代价：**comet 与 logic 的 `-trace-sample` 必须取同一个值**，否则各采各的——compose 里因此把三处都写死成 100，而不是依赖默认值恰好相同。

**2）判断"要不要记"本身不能有成本。** msg_id 藏在 JSON 消息体里，job/comet 若为了取它而反序列化每条消息，热路径代价就超过 trace 本身的价值。所以采样结果由 logic 沿途传下去：

| 环节 | 传播方式 | 为什么不用别的 |
|------|---------|--------------|
| logic → job | Kafka header `x-danmu-trace`（值即 msg_id） | 落库 consumer 忽略 header，完全不受影响 |
| job → comet | gRPC metadata `danmu-trace-msgids`（逗号分隔） | **不用改 `pb/danmu.proto`**；旧版 comet 直接忽略 |

下游只读 header/metadata，不碰 payload。未采样消息在热路径上零额外成本。

### 存储与汇聚

- 各服务：进程内**有界环形缓冲**（`-trace-buffer`，默认 512），满了丢最旧的并计数。`/api/v1/traces` 一并返回 `dropped`。
- ops：`traceLoop` 与主采集同频轮询各健康实例，按 msg_id 并入 `traceStore`（`ops/trace.go`），保留最近 `traceMaxKept=200` 条链路，按 `hop@node` 去重（同一 span 会被反复拉到；但同一环节来自不同 comet 是合法的，不能去掉）。
- **拉而不推**：推的话业务服务要感知 ops 地址、失败要重试要缓冲，等于在消息链路旁再建一条链路。拉的话 ops 挂了业务侧毫无感知——与 ops「旁路观测者」的定位一致。
- 每条链路给出 `complete` 与 `missing_hops`：缺哪段就说明消息停在那之前。

### 已知局限（都是有意接受的）

1. **链路只能从「logic 成功返回」起记**。msg_id 由 logic 生成，上行 RPC 失败的消息压根没有 msg_id，因此**不会留下任何 trace**——表现为「查无此消息」而不是「链路缺后几段」。这是本方案最大的盲区：最该被诊断的那类故障（上行就断了）恰恰不可见。
2. **跨节点耗时受时钟偏差影响**。span 用各机器的 `UnixNano`，没做任何时钟校正——不引入 OTel 也就没有更好的处理。段间耗时只宜当数量级参考。
3. `logic.produce` 的时刻是「交给 Async writer 入队」，不是「broker 已确认」。真实 produce 耗时不在链路里。
4. 缓冲溢出即永久丢失（拉取间隔内溢出的看不到了）。`sources` 里透传 `dropped`，让「没采全」可见。
5. `-mock` 模式不产生 trace。

### 验证状态

- **单测**：`core/trace_test.go`（采样确定性跨实例一致、rate 边界、缓冲有界与丢弃计数、nil 安全）、`ops/trace_test.go`（完整性判定、缺失环节上报、重复轮询去重、同环节多节点保留、淘汰、排序）、`job/trace_test.go` + `comet/trace_test.go`（两个跨进程接缝：Kafka header 键、gRPC metadata 键与逗号拼接）。全绿。
- **未验**：五段完整链路从未真实跑通过。本机 Kafka 起不来（Docker daemon 未运行），而 kafka-go 即使 `Async:true` 首次仍需同步取 partition 元数据，Kafka 不在则 `WriteMessages` 直接返回错误——实测 900 条上行全部止步于 logic（`onmessage_errors=900`），链路按设计不产生任何 span。**首次带 Kafka 跑起来后必须复验一次**。
