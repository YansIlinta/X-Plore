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
| Kafka 段（logic→Kafka→job 扇出）、Redis 跨机广播、ClickHouse 落库/历史 | ⏳ 需中间件环境（compose / run-goim-local.sh） |

## 文档地图

| 文档 | 内容 |
|------|------|
| [danmu/PROJECT.md](./danmu/PROJECT.md) | 弹幕主线统合：架构取舍、压测数据、面试讲解 |
| [danmu/monolith/README.md](./danmu/monolith/README.md) | 单体版部署、压测剧本、系统调优清单、接口文档 |
| [danmu/monolith/REVIEW.md](./danmu/monolith/REVIEW.md) | 单体版代码审查报告（H/M/L 逐条问题与修复） |
| [danmu/distributed/DESIGN.md](./danmu/distributed/DESIGN.md) | goim 式微服务化设计：Comet/Logic/Job 分层与 etcd |
