# X-Plore —— 直播弹幕系统

高并发直播弹幕系统，**单体**与**分布式（goim 式）**两种架构实现，互为对照。

```text
X-Plore/
├── danmu/                    百万 QPS 直播弹幕系统
│   ├── monolith/             单体：一个 server 进程 = 接入 + 逻辑 + 广播
│   └── distributed/          分布式：goim 式 Comet / Logic / Job + etcd
```

## 主线

| 主线 | 说明 | 从哪里开始 |
|------|------|-----------|
| **高并发弹幕** | WebSocket 长连接、单体 vs goim 微服务、百万级压测 | [danmu/PROJECT.md](./danmu/PROJECT.md)（统合总览） |

## 两个 Go module

拆分后每个架构版本是独立 module，依赖互不干扰，各自 `go build ./...` 即可。

| 目录 | module | 主要产物 |
|------|--------|---------|
| `danmu/monolith` | `github.com/YansIlinta/danmu-monolith` | `server` `consumer` `loadtest` |
| `danmu/distributed` | `github.com/YansIlinta/danmu-distributed` | `comet` `logic` `job` `chaintest`（服务发现用 etcd） |

> **公共组件说明**：弹幕的 `consumer/`（Kafka→ClickHouse 落库）和 `loadtest/`（压测客户端）
> 与架构无关，源码只放在 `danmu/monolith/`，分布式版通过 compose 的 `../monolith` build context
> 和相对路径构建来复用，不做第二份拷贝。

## 快速上手

```powershell
# 弹幕 · 单体（Redis + Kafka + ClickHouse + 2×server + nginx）
cd D:\X-Plore\danmu\monolith
docker compose up -d --build          # 前端 http://localhost:8080

# 弹幕 · 分布式（etcd + logic + job + 2×comet）
cd D:\X-Plore\danmu\distributed
docker compose -f docker-compose.goim.yml up -d --build
bash scripts/run-goim-local.sh        # 或本地进程方式（需本机 Kafka）
kubectl apply -k k8s/                 # 或 Kubernetes（清单与说明见 k8s/README.md）
```

## 测试

两个 Go module 独立，各自跑（需要 Go 1.25+，distributed 会按 go.mod 自动拉取对应 toolchain）：

```bash
cd danmu/monolith && go build ./... && go vet ./... && go test ./...
cd danmu/distributed && go build ./... && go vet ./... && go test ./...
# 竞态检测（monolith 含 WS 端到端与并发用例）
cd danmu/monolith && go test -race ./server/
```

覆盖要点：

- **monolith/server**：WS 端到端（发弹幕→同房间接收、敏感词过滤、错误 token 401、管理员广播、会话过期断开）、Hub 分片/顶号/关房/踢人、TokenBucket、消息合并与 UTF-8 截断
- **distributed**：core（Hub 计数一致性、令牌、令牌桶、trace）、etcdreg（embed etcd 注册/发现/Watch）、comet（etcd resolver + round_robin 负载分摊）、ops（采集聚合/拓扑/事件）、job/comet 的 trace 链路
- **链路集成**：`danmu/distributed/cmd/chaintest`（需 etcd + logic + comet，见 [DESIGN.md](./danmu/distributed/DESIGN.md)）
- **ClickHouse 冒烟**：`go test -tags smoke ./consumer/`（需本地 ClickHouse）

## 验证状态

| 路径 | 状态 |
|------|------|
| 单体：无中间件降级本机广播（WS 收发闭环） | ✅ 实测 |
| 分布式：etcd 发现 + Logic.OnMessage + PushRoom→WS + trace | ✅ 实测（chaintest） |
| Ops Console 聚合与健康判定 | ✅ 实测 |
| standalone comet 压测（P50 ~0.4ms @ 200 连接） | ✅ 实测 |
| **Realtime Systems Lab（实验/对比/证据）** | ✅ 实测（200c/100r 与 400c/10r 各一次，见 [EXPERIMENTS.md §9](./danmu/distributed/EXPERIMENTS.md)） |
| **Realtime Systems Lab Phase 1.5（重复基准 / 参数扫描 / 跨 regime 分析 / Adaptive-Go-No-Go）** | ✅ 实测（3 regimes × 3 configs × 5 reps acceptance，见 [EXPERIMENTS.md §15](./danmu/distributed/EXPERIMENTS.md)） |
| Kafka 段（logic→Kafka→job 扇出）、Redis 跨机广播、ClickHouse 落库/历史 | ⏳ 需中间件环境（compose / run-goim-local.sh） |

> 说明：©上面 "✅ 实测" 是不同日期、不同机器上的记录；每一行都非「当前版本自动证明」。
> Realtime Systems Lab 的 Evidence 页只认实验存储、不认文档——想证明一个性能数字，
> 必须在当前版本上跑一次实验让引擎判定（VERIFIED / PARTIALLY VERIFIED / TARGET）。

## 文档地图

| 文档 | 内容 |
|------|------|
| [danmu/PROJECT.md](./danmu/PROJECT.md) | 弹幕主线统合：架构取舍、压测数据 |
| [danmu/monolith/README.md](./danmu/monolith/README.md) | 单体版部署、压测剧本、系统调优清单、接口文档 |
| [danmu/monolith/REVIEW.md](./danmu/monolith/REVIEW.md) | 单体版代码审查报告（H/M/L 逐条问题与修复） |
| [danmu/distributed/DESIGN.md](./danmu/distributed/DESIGN.md) | goim 式微服务化设计：Comet/Logic/Job 分层与 etcd |
| [danmu/distributed/EXPERIMENTS.md](./danmu/distributed/EXPERIMENTS.md) | Realtime Systems Lab：实验模型 / API / 复现命令 / Phase 1.5 实测记录 |
| [danmu/distributed/BENCHMARKING.md](./danmu/distributed/BENCHMARKING.md) | 基准正确性语义：Run/Experiment/Sweep、warm-up/测量窗、统计、投递核算 |
| [danmu/distributed/SWEEPS.md](./danmu/distributed/SWEEPS.md) | 参数扫描 / 跨 workload regime 分析 / Adaptive-Control Go-No-Go |
| [danmu/distributed/EVIDENCE.md](./danmu/distributed/EVIDENCE.md) | Evidence/Claims 语义：VERIFIED / PARTIAL / CODE / TARGET / UNKNOWN + Phase 1.5 claims |
