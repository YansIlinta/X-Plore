# X-Plore · 高并发直播弹幕系统（统合文档）

> 本文档把仓库内分散的 README / DESIGN / REVIEW 等材料，**统合成一份可阅读、可演示**的「高并发直播弹幕」主项目说明。  
> 旁支项目（WaveHub 音乐站等）见文末「仓库边界」，不纳入本主线。

---

## 1. 一句话定位

**面向直播场景的百万级 WebSocket 长连接弹幕系统**：支持多房间、多机部署、实时跨机广播、消息持久化与历史回放，并提供「单体」与「goim 式微服务」两套可运行架构做对比。

| 维度 | 内容 |
|------|------|
| 语言 / 模块 | Go 1.25，`module danmu` |
| 连接协议 | WebSocket（gorilla/websocket） |
| 实时广播（单体） | Redis Pub/Sub（按房间批量 publish） |
| 实时广播（微服务） | Kafka 削峰 + Job 定向 RPC 推 Comet |
| 持久化 | Kafka → ClickHouse（MergeTree） |
| 负载均衡 | Nginx 一致性哈希（优先 `X-Forwarded-For`） |
| 可观测 | Prometheus `/metrics` + pprof |
| 压测 | 自研 loadtest + HDR Histogram |

---

## 2. 为什么要做两套架构

| 形态 | 目录 | 跨机广播 | 适用场景 |
|------|------|----------|----------|
| **单体基线** | `monolith/server/` | Redis Pub/Sub | 开发讲清「长连接 + 扇出」；少依赖快速演示 |
| **goim 微服务** | `distributed/comet/` + `distributed/logic/` + `distributed/job/` + `distributed/core/` + `distributed/etcdreg/`（etcd 注册/发现） | Kafka → Job → Comet.PushRoom | 连接/逻辑/扇出独立扩缩；对齐 Bilibili goim 思路 |

**设计取舍**：

- Redis Cluster Pub/Sub：每条消息会扩散到集群各节点，吞吐随规模**负向扩展**。
- goim 路径：Kafka 削峰 + Job **定向**推送到持有房间连接的 Comet，Comet 无该房间则丢弃，扇出更可控。
- 单体不删：作为对照基线，压测与代码路径更短，便于定位性能问题。

---

## 3. 系统架构

### 3.1 单体架构（`monolith/server/`）

```
客户端 ──WS──► Nginx(一致性哈希)
                  │
          ┌───────┴───────┐
       srv1             srv2
     Hub+Worker       Hub+Worker
          │               │
     ┌────┼────┐     ┌────┼────┐
   本机广播  Redis   Kafka     …
           Pub/Sub  (落库/多消费组)
                      │
                 consumer → ClickHouse
```

**数据流要点**：

1. 客户端 WebSocket 上行弹幕 → 入 `msgQueue`（削峰）。
2. Worker 池批量聚合（2000 条 / 20ms，见 `monolith/server/worker.go`）→ 本机 `Hub.BroadcastToRoom`。
3. 同时：Redis 按房间批量 Pub（跨机实时）+ Kafka Produce（持久化）。
4. 其他 server 经 Redis Sub 收到后，过滤本机 `SourceServer`，再广播到本机房间连接。
5. `consumer` 消费 Kafka 写入 ClickHouse；可选给 server 配 ClickHouse 只读以提供 `/api/v1/history`。

### 3.2 goim 微服务架构（`comet` / `logic` / `job`）

```
                    ┌──────────┐  register(lease+keepalive)
                    │   etcd   │◄──────────────── comet-i / logic-i
                    └────▲─────┘
                    watch comets
 client ──WS──► comet ──RPC:Logic.OnMessage──► logic ──produce──► Kafka
   ▲                                                              │
   │ RPC:Comet.PushRoom                                           │ consume
   └──────────────────────────── job ◄────────────────────────────┘
              （对每个 comet 定向 PushRoom；无该房间则 delivered=0）
```

**一条弹幕完整链路**：

`client → comet(收) → Logic.OnMessage(敏感词 + msg_id) → Kafka → job → 全部 comet PushRoom → 本机 BroadcastToRoom → 房间内 client`

- 发送者自己的弹幕也走回路（不在 comet 本地 echo），前端靠 `msg_id` 滑动窗口去重。
- 落库：`monolith/consumer/` 独立消费同一 Kafka topic 写 ClickHouse，与广播解耦。
- Comet 可 `-standalone` 脱离 logic/job/Kafka，本机过滤+广播，便于冒烟。

### 3.3 组件职责一览

| 组件 | 路径 | 职责 |
|------|------|------|
| **server** | `monolith/server/` | 单体：WS + Hub + Worker + Redis/Kafka + REST API |
| **core** | `distributed/core/` | 共享：Hub 分片、Client、消息、限流、敏感词、令牌、指标 |
| **comet** | `distributed/comet/` | 连接层：WS、房间连接、上行转发 Logic、暴露 PushRoom |
| **logic** | `distributed/logic/` | 无状态逻辑：过滤、msg_id、Kafka produce |
| **job** | `distributed/job/` | 消费 Kafka，发现全部 Comet 并定向 PushRoom |
| **etcd** | 外部组件（compose / `k8s/` 部署） | 注册中心：租约 + 前缀 Get/Watch；客户端封装 `distributed/etcdreg/` |
| **etcdreg** | `distributed/etcdreg/` | 注册/发现约定：key 规范、续租、List/Watch、可选 TLS |
| **consumer** | `monolith/consumer/` | Kafka → ClickHouse 落库 |
| **loadtest** | `monolith/loadtest/` | 多连接压测、E2E 延迟（纳秒透传）、HDR 报告 |
| **web** | `monolith/web/` | 前端（虚拟滚动弹幕列表） |
| **pb** | `distributed/pb/` | gRPC/Protobuf 契约（Comet 等） |

---

## 4. 核心设计点（可讲解）

### 4.1 连接与房间管理

- Hub 使用 **256 分片锁**（每分片独立 `RWMutex`），避免全局锁在高连接数下成为瓶颈。
- 每个连接独立 `readPump` / `writePump`；`sendCh` 有界，满则**静默丢弃**（弹幕允许丢，保实时性）。
- goim 路径：core 去掉「单 goroutine 串行 register/unregister」，直接分片安全 `AddClient/RemoveClient`。

### 4.2 削峰与批量

- 上行：channel 队列 + 固定 worker 数（约 `CPU*4`）。
- 批量：按条数阈值或时间窗口聚合后再广播 / 发布，减少 syscall 与网络次数。
- Redis：按房间批量 publish，降低 Pub/Sub 开销。

### 4.3 Redis vs Kafka 分工（单体）

| | Redis Pub/Sub | Kafka |
|--|---------------|-------|
| 职责 | 跨机**实时**广播 | 持久化 / 回放 / 多下游 |
| 延迟 | 亚毫秒级 | 批量消费，更高 |
| 语义 | Fire-and-forget | 可回放、消费组 |
| 不能单用的原因 | 无持久化、难多下游 | 不适合纯广播语义与极低延迟 |

微服务路径用 **Kafka + Job 定向推** 替代 Redis 全量扇出，解决「每机都要反序列化全网消息」的扩展问题。

### 4.4 可靠与正确性

| 点 | 做法 |
|----|------|
| 全局 `msg_id` | Logic/单体生成，前端去重，防双路径重复投递 |
| 会话令牌 | 限时 session + `reauth` / `/api/v1/session-token`，到期断开 |
| 敏感词 | 本地 AC 自动机 |
| 限流 | 无锁令牌桶（单次 CAS 打包更新） |
| 鉴权 | Bearer Token；比较使用恒定时间比较（防侧信道） |
| 优雅退出 | context + 信号，关闭 listener / 连接 / 中间件 |

### 4.5 存储

- ClickHouse MergeTree：按天分区，排序键 `(room_id, server_ts)`。
- 落库与广播解耦；consumer 宜 **先写成功再提交 offset**（at-least-once），避免「offset 已提交、批未 flush」丢数据。
- 历史查询：server 可选 `-clickhouse-addr` 只读接入；未配置时不应假装有数据。

### 4.6 可观测与运维

- Prometheus：连接数、消息 in/out、广播延迟、队列长度等；**禁止**把 `room_id` 当高基数 label。
- pprof 默认独立端口（如 `:6060`）。
- Nginx：`hash $client_identifier consistent`，优先 `X-Forwarded-For`，避免 CDN 后 `ip_hash` 失效。

### 4.7 已知取舍（诚实边界）

- 同房间消息跨 worker **可能无序**（弹幕场景可接受）。
- 单体下关房/踢人控制面默认**本机生效**（管理员广播已走跨机通路）。
- 超大房间 `sendCh` 满丢消息是有意设计；可调大缓冲或分级广播。
- 已落地（2026-08）：服务发现换 **etcd**、comet→logic 用 **gRPC 标准 round_robin**（自研 registry/lb/minirpc 全删）、etcd TLS（`k8s/overlays/etcd-tls`）。待挖：连接层 epoll 事件循环（百万单机）、Kafka 多分区长期压测。

---

## 5. 压测结论（已验证摘要）

环境摘录：AutoDL 256 核 / 755G；单体 + 同机 loadtest；Redis 缺失时降级本机广播。

| 场景 | 连接 | 房间/扇出 | E2E P50 | E2E P90 | 读写错误 |
|------|------|-----------|---------|---------|----------|
| 低扇出 | 10k | 1000 房 / 扇出 ~10 | **1.6ms** | **5.3ms** | 0 / 0 |
| 高扇出 | 10k | 100 房 / 扇出 ~100 | 510ms | 1.6s | 有丢弃 |

standalone comet + loadtest（本地）：200 连接，E2E P50 ~0.6ms / P90 ~1.1ms / P99 ~1.7ms。

goim 链路：`distributed/cmd/chaintest` 已验证 etcd 发现、Logic.OnMessage、Job→Comet.PushRoom→WS 收包、trace span 落盘；全链路含 Kafka 可用 `distributed/scripts/run-goim-local.sh` 或 `distributed/docker-compose.goim.yml`。

**压测注意**：压测机与 server 宜分机；高扇出秒级延迟常含 CPU 争抢与下游 write 路径，不全是业务逻辑回归。

---

## 6. 目录结构（弹幕主线）

```
X-Plore/
├── README.md                     # 仓库总览
└── danmu/                        # 弹幕主线（两个独立 Go module）
    ├── PROJECT.md                # 本统合文档
    ├── monolith/                 # 单体基线（一个 server 进程）
    │   ├── README.md             # 单体运维/压测/调优详版
    │   ├── REVIEW.md             # 代码审查与修复记录
    │   ├── server/               # 单体弹幕服务
    │   ├── consumer/             # Kafka → ClickHouse 落库
    │   ├── loadtest/             # 架构无关压测
    │   └── web/                  # 前端
    └── distributed/              # goim 微服务版
        ├── DESIGN.md             # goim 拆分设计（含验证状态）
        ├── ARCHITECTURE_ANALYSIS.md  # Ops Console 勘察报告
        ├── OPS.md                # Ops Console 使用说明
        ├── core/                 # 微服务共享内核
        ├── comet/  logic/  job/  # goim 三层
        ├── etcdreg/              # etcd 注册/发现封装（含 TLS）
        ├── cmd/chaintest/        # 链路集成测试
        ├── ops/  cmd/ops/        # Ops Console 后端 + 前端
        ├── pb/                   # gRPC 契约
        ├── web/                  # 联调页（comet 托管）
        ├── scripts/run-goim-local.sh
        ├── docker-compose.goim.yml / nginx.conf
        ├── k8s/                  # K8s 清单（base/ + overlays/etcd-tls）
        └── helm/danmu/           # Helm chart
```
> 注：`docker-compose.yml`、`Dockerfile.*`、`nginx.conf` 分别位于 `monolith/` 与 `distributed/` 目录内（README 顶部「两个 Go module」表已说明），根目录不再放部署文件。

---

## 7. 快速启动

### 7.1 单体（最少依赖可跑）

```bash
# 中间件（可选；无 Redis/Kafka 时 server 会降级本机广播）
docker compose up -d   # 或手动起 Redis / Kafka / ClickHouse，见 README

export DANMU_AUTH_TOKEN=danmu-secret-token
go build -o bin/server ./server/
./bin/server -addr=:8081 -id=srv1 -redis=localhost:6379 -kafka=localhost:9092 -mq=both

# 浏览器
# http://localhost:8081
```

### 7.2 goim 本地全链路

```bash
# 需本机 Kafka :9092
bash distributed/scripts/run-goim-local.sh
# WS: ws://localhost:8080 与 :8081
# 停止: bash distributed/scripts/run-goim-local.sh stop
```

### 7.3 容器 goim 全链路

```bash
docker compose -f docker-compose.goim.yml up -d --build
# Nginx 入口常见为 :8088（见 compose 映射）
```

### 7.4 压测

```bash
go build -o bin/loadtest ./loadtest/
./bin/loadtest \
  --server=ws://localhost:8080,ws://localhost:8081 \
  --conns=2000 --rooms=50 --rate=2 --duration=30s \
  --token=danmu-secret-token
```

分阶段目标（1k → 1 万 → 10 万 → 百万）与 `ulimit`/TCP/`GOGC` 调优清单见 `README.md`。

### 7.5 常用 API

```bash
TOKEN=danmu-secret-token
curl http://localhost:8081/health
curl -H "Authorization: Bearer $TOKEN" http://localhost:8081/api/v1/stats
curl -H "Authorization: Bearer $TOKEN" http://localhost:8081/api/v1/rooms
curl http://localhost:8081/metrics
```

---

## 8. 文档索引

| 文档 | 内容 |
|------|------|
| **PROJECT.md（本文）** | 统合总览：架构、取舍、启动、边界 |
| `danmu/monolith/README.md` | 单体详解、压测剧本、内核调优、排障 |
| `danmu/distributed/DESIGN.md` | 微服务拆分契约与数据流、验证状态 |
| `danmu/monolith/REVIEW.md` | 问题清单、修复、压测前后对比 |

---

## 9. 状态快照（截至仓库内记录 2026-07）

- [x] 单体可构建、可压测；H1–H2 / M1–M6 / L1–L3 等审查项已处理一轮  
- [x] core 单测（AC、令牌、令牌桶）  
- [x] standalone comet + loadtest  
- [x] goim chaintest（etcd 发现 / Logic / PushRoom→WS / trace span）  
- [ ] 有 Kafka 环境下全链路长时间压测（按需用 compose / run-goim-local 补）  

---

*统合目的：打开一份文档即可理解「这是什么项目、怎么跑、为什么这样设计、讲到哪里算讲完」。细节实现与命令仍以 `danmu/monolith/README.md` / `danmu/distributed/DESIGN.md` 为准。*

---

## 10. 视频平台演进（2026-07 起，已归档移除）

> 原 WaveHub 点播平台（user/video/media 等 Kratos 微服务 + React 前端）已从仓库移除，
> 历史内容保留在 git 历史中。本文 1–9 节（连接层与压测叙事）为弹幕主线核心。
