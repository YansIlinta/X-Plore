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

## 文档地图

| 文档 | 内容 |
|------|------|
| [danmu/PROJECT.md](./danmu/PROJECT.md) | 弹幕主线统合：架构取舍、压测数据、面试讲解 |
| [danmu/monolith/README.md](./danmu/monolith/README.md) | 单体版部署、压测剧本、系统调优清单、接口文档 |
| [danmu/monolith/REVIEW.md](./danmu/monolith/REVIEW.md) | 单体版代码审查报告（H/M/L 逐条问题与修复） |
| [danmu/distributed/DESIGN.md](./danmu/distributed/DESIGN.md) | goim 式微服务化设计：Comet/Logic/Job 分层与 etcd |
