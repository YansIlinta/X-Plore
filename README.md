# X-Plore —— 直播弹幕系统 + WaveHub 视频平台

一个仓库，两套系统，每套都有**单体**和**分布式**两种架构实现，互为对照。

```text
X-Plore/
├── danmu/                    百万 QPS 直播弹幕系统
│   ├── monolith/             单体：一个 server 进程 = 接入 + 逻辑 + 广播
│   └── distributed/          分布式：goim 式 Comet / Logic / Job + 自研 minirpc
├── wavehub/                  类 B 站视频点播平台（WaveHub）
│   ├── monolith/             单体：Gin 音乐站（学习基线）
│   ├── micro/                微服务：Kratos user/video/media/social/search/gateway
│   ├── web-app/              产品前端（React + Vite）
│   ├── deploy/platform/      平台依赖 compose（PG/Redis/MinIO/Kafka/CH/etcd）
│   └── scripts/              本地起服务 / 健康检查 / 点播冒烟
├── docs/                     跨主线文档（演示、面试材料）
└── musicviz/                 SoundCanvas 本地音乐可视化（仅设计稿，未开工）
```

## 两条主线

| 主线 | 说明 | 从哪里开始 |
|------|------|-----------|
| **高并发弹幕** | WebSocket 长连接、单体 vs goim 微服务、百万级压测 | [danmu/PROJECT.md](./danmu/PROJECT.md)（统合总览） |
| **点播平台 MVP** | Kratos 用户/稿件/HLS 转码、React 播放弹幕互动、Gateway | [docs/DEMO.md](./docs/DEMO.md)（怎么起、怎么演） |
| **面试题库 ~100 题** | 题目 + 代码锚点 / 答案分册 | [docs/interview/](./docs/interview/) |

## 四个 Go module

拆分后每个架构版本是独立 module，依赖互不干扰，各自 `go build ./...` 即可。

| 目录 | module | 主要产物 |
|------|--------|---------|
| `danmu/monolith` | `github.com/YansIlinta/danmu-monolith` | `server` `consumer` `loadtest` |
| `danmu/distributed` | `github.com/YansIlinta/danmu-distributed` | `comet` `logic` `job` `registry` `chaintest`（含内嵌 `minirpc`） |
| `wavehub/monolith` | `github.com/YansIlinta/wavehub` | Gin 音乐站单体 |
| `wavehub/micro` | `github.com/YansIlinta/wavehub-micro` | 六个 Kratos 服务 |

> **公共组件说明**：弹幕的 `consumer/`（Kafka→ClickHouse 落库）和 `loadtest/`（压测客户端）
> 与架构无关，源码只放在 `danmu/monolith/`，分布式版通过 compose 的 `../monolith` build context
> 和相对路径构建来复用，不做第二份拷贝。

## 快速上手

```powershell
# 弹幕 · 单体（Redis + Kafka + ClickHouse + 2×server + nginx）
cd D:\X-Plore\danmu\monolith
docker compose up -d --build          # 前端 http://localhost:8080

# 弹幕 · 分布式（registry + logic + job + 2×comet）
cd D:\X-Plore\danmu\distributed
docker compose -f docker-compose.goim.yml up -d --build
bash scripts/run-goim-local.sh        # 或本地进程方式（需本机 Kafka）

# 视频平台（依赖 + 六个微服务 + 前端）
cd D:\X-Plore\wavehub
docker compose -f deploy\platform\docker-compose.yml up -d
.\scripts\dev-platform.ps1 build
.\scripts\dev-platform.ps1 gateway
.\scripts\dev-platform.ps1 frontend   # http://localhost:5173
.\scripts\check-platform.ps1          # 无副作用健康检查
```

## 文档地图

| 文档 | 内容 |
|------|------|
| [danmu/PROJECT.md](./danmu/PROJECT.md) | 弹幕主线统合：架构取舍、压测数据、面试讲解 |
| [danmu/monolith/README.md](./danmu/monolith/README.md) | 单体版部署、压测剧本、系统调优清单、接口文档 |
| [danmu/monolith/REVIEW.md](./danmu/monolith/REVIEW.md) | 单体版代码审查报告（H/M/L 逐条问题与修复） |
| [danmu/distributed/DESIGN.md](./danmu/distributed/DESIGN.md) | goim 式微服务化设计：Comet/Logic/Job 分层与 minirpc |
| [wavehub/EVOLUTION.md](./wavehub/EVOLUTION.md) | 点播平台演进决策与里程碑（M1–M6） |
| [wavehub/ROADMAP-V2.md](./wavehub/ROADMAP-V2.md) | V2 类 B 站升级：前端重做 + search/social 微服务 |
| [wavehub/micro/MICROSERVICES.md](./wavehub/micro/MICROSERVICES.md) | Kratos 微服务学习笔记 |
| [wavehub/deploy/platform/PRODUCTION.md](./wavehub/deploy/platform/PRODUCTION.md) | 上线清单 |
| [docs/DEMO.md](./docs/DEMO.md) | 15 分钟演示脚本（两条主线一起讲） |
| [docs/interview/](./docs/interview/) | 题库 / 答案 / 口述稿 |
| [docs/SPRINT-2DAY.md](./docs/SPRINT-2DAY.md) | 两天面试冲刺方案 |
