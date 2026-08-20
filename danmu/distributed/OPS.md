# OPS.md — Danmu Ops Console

旁路观测控制台：只做 read / aggregate / 有限的开发操作（压测启停）。**不参与消息链路**——
ops 挂掉，弹幕系统照常工作。

- 后端：`cmd/ops/` + `ops/`（Go，无第三方 Web 框架，标准库 `net/http`）
- 前端：`ops/web/`（React 18 + TypeScript + Vite 5 + uPlot），构建产物 `ops/web/dist` 经
  `go:embed` 内嵌进 ops 二进制，单文件部署
- 默认地址：`http://localhost:7900`

---

## 1. 架构

```text
Browser ──► ops (:7900)
              ├── etcd ListAll（danmu/services/ 前缀）→ 服务发现与实例清单
              ├── GET 各实例 /health + /api/v1/stats → 实例健康与原始计数
              ├── GET comet /metrics                → danmu_messages_total 差分算速率
              ├── GET 各实例 /api/v1/traces         → 按 msg_id 汇聚消息链路
              ├── GET 各 comet /api/v1/rooms        → 房间聚合（按需，不进周期采集）
              ├── Kafka Metadata/ListOffsets/
              │   OffsetFetch                        → consumer lag
              └── 子进程 bin/loadtest                → 压测启停与快照解析
```

采集周期 `-poll`（默认 2s），实例探测并发执行、单实例 2s 超时；Kafka lag 单独以 3×poll 周期
计算，不阻塞主循环。所有请求失败都降级为"不可用/跳过"，绝不用上次的值冒充本轮数据。

## 2. 快速开始

```bash
# 本地全链路 + console（需 Kafka 在 localhost:9092；etcd:2379 logic:7410 comet×2 ops:7900）
bash scripts/run-goim-local.sh
# 停止
bash scripts/run-goim-local.sh stop

# 本地无 Kafka 也能起（Kafka 页显示不可用、链路停在 logic，见 §7 限制）
# 各服务可单独启动，端口自定，例如冒烟用的 17xxx 段：
#   etcd（本机二进制，默认 localhost:2379）
#   bin/logic -addr=:17400 -http-addr=:17410 -etcd=localhost:2379 \
#     -advertise=localhost:17400 -kafka=localhost:9092 -trace-sample=1
#   bin/comet -ws-addr=:17080 -rpc-addr=:17500 -advertise=localhost:17500 \
#     -id=comet1 -etcd=localhost:2379 -trace-sample=1
#   bin/ops -addr=:17900 -etcd=localhost:2379 -kafka=localhost:9092 -poll=1s

# Docker（含 Kafka；镜像拉不动时可跳过，见 §7）
docker compose -f docker-compose.goim.yml up -d --build
```

浏览器打开 `http://localhost:7900`。首页 Overview 直接回答"系统活着吗 / 谁有问题"。

## 3. 观测面：已有能力 vs 本项目新增

| 能力 | 来源 | 状态 |
|------|------|------|
| etcd `danmu/services/` 服务发现（etcdreg.ListAll） | 项目已有（etcd 迁移后） | 直接复用 |
| comet `/health`、`/api/v1/stats`（O(1) 原子计数）、`/metrics`（Prometheus 格式） | 已有（Ops Console 之前为观测各服务补的） | 直接复用 |
| logic `/health`、`/api/v1/stats` | 已有（同上） | 直接复用 |
| comet `/api/v1/rooms`（分页枚举，Hub 全量 + 内存分页） | 已有 | ops 按需扇出合并 |
| Kafka Admin（lag） | 新增于 `ops/collector.go:647`（`kafkaLoop`/`kafkaOnce`，kafka-go Client，只读 Metadata/ListOffsets/OffsetFetch） | 无 broker 时 Available=false |
| 消息 trace（`core/trace.go` + 各服务 `/api/v1/traces`） | 新增（见 §5） | 需 `-trace-sample` 一致 |
| ops HTTP API / SSE 前端轮询 | 新增 | 实时数据前端 2s 轮询，无 WebSocket |
| loadtest 集成 | 复用 `../monolith/loadtest` 二进制，ops 只做启停/解析 | 二进制不存在时页显"不可用" |

## 4. API

`GET` 全部只读；仅两个 `ACTION` 端点会改变状态（启停压测子进程），前端对 ACTION 有二次确认。

```text
GET  /api/health               ops 自身探活
GET  /api/overview             核心 KPI + 系统健康（health/degraded/critical + 明细）
GET  /api/services             组件分组实例列表（透传各实例 stats 原始 JSON）
GET  /api/topology             架构拓扑节点（clients/nginx 无法观测 → healthy=null，诚实 N/A）
GET  /api/events               系统事件流（实例出现/消失/恢复、健康翻转、Kafka 状态）
GET  /api/traces               msg_id 汇聚链路（sources 附带各节点采样状态/丢弃计数）
GET  /api/rooms                跨 comet 房间聚合（按需查询，limit≤100/实例）
GET  /api/rooms/{id}           单房间定位（在哪些 comet、在线数）

# Realtime Systems Lab（实验 / 对比 / 证据，见 EXPERIMENTS.md / EVIDENCE.md）
GET  /api/presets              preset 模板（含默认 workload 与想回答的问题）
GET  /api/experiments          历史实验（有界）
POST /api/experiments          create（400 校验失败 / 422 未知 preset）
GET  /api/experiments/{id}     详情（running 时带 live 实时快照）
POST /api/experiments/{id}/start  ACTION：启动（409 已在跑 / 404 不存在）
POST /api/experiments/{id}/stop   ACTION：停止（409 未在跑）
GET  /api/experiments/{id}/report  完整报告 + 复现元数据 + 关联 claims
GET  /api/compare?left=&right=  对比（两侧必须 completed，否则 422）
GET  /api/evidence              claims 当前状态（VERIFIED 仅来自实验存储）

# 兼容入口：旧 Load Test 页已并入 Experiments 页；下面三个端点保留，委托同一状态机
GET  /api/loadtest/status      压测状态/最近快照/结束报告（附 active_experiment）
POST /api/loadtest/start       ACTION：启动压测（409 已运行 / 503 二进制缺失）
POST /api/loadtest/stop        ACTION：终止压测
```

状态机唯一主人是 **ExperimentManager**（`ops/experiment_manager.go`）：全局同时
只能有一个实验/压测在跑（loadtest 子进程单例），旧的 `/api/loadtest/*` 只是它
的兼容入口，绝不维护两套压测状态机。（*本次修改前 Load Test 页报告"无 e2e 延迟"
等行为已并入 Experiments 页，见 EXPERIMENTS.md 已知限制。）

数据真实性约定：拿不到的数据一律 `null`（前端渲染 N/A），不伪造 0；`-mock` 显式开启后
所有响应带 `"mock":true`，前端显示 **MOCK DATA** 横幅。

## 5. Message Trace

- **采样**：msg_id 由 logic 生成，各环节用同一套 FNV 确定性哈希判定采样
  （`-trace-sample=N`，1/N 采样，0=关闭）。logic / comet 的 N 必须一致，否则链路拼不齐。
- **传递**：logic 把采样中的 msg_id 写进 Kafka header（`x-danmu-trace`），job 经 gRPC
  metadata（`danmu-trace-msgids`）传给 comet——下游不反序列化 payload。
- **环节**：`comet.uplink → logic.produce → job.consume → job.push → comet.deliver`，
  ops 侧按此顺序渲染，缺哪段就标 `missing_hops`（消息停在哪一目了然）。
- **存储**：各服务有界环形缓冲（默认 512 条，溢出丢最旧并计数），ops 周期拉取、
  最多保留 200 条 msg_id。`/api/traces` 的 `sources` 透传各节点 dropped 计数——
  "没采全"这件事是可见的，不会默默给残缺结论。
- **精度上限**：段间耗时基于各节点时钟（UnixNano），节点间时钟偏差会让耗时失真；
  没有引入 OTel，这是已知取舍。

## 6. 前端

```bash
cd ops/web
npm install
npm run dev      # Vite 开发服务器（API 代理到 :7900）
npm run build    # 产物到 ops/web/dist，由 ops 的 go:embed 内嵌
```

- 路由：hash 路由（无 react-router 依赖），页面：Overview / **Experiments(实验) /
  Compare(对比) / Evidence(证据)** / Topology / Traffic / Rooms / Kafka / Messages /
  Events / Services。旧 Load Test 页并入 Experiments（`#/loadtest` 仍可直达，跳转
  到 Experiments）。
- 图表：uPlot（轻量 canvas），拓扑自绘 SVG，节点按健康上色、连线按实时速率做流动动画。
- 前端只消费 §4 的 API，不直接访问任何业务服务，不把分布式逻辑塞进浏览器。
- 实验数据目录：`ops -data-dir=...`（默认 `./data`，子目录 `experiments/` 存 JSON）；
  git 复现元数据：`ops -repo-dir=<仓库路径>`（空 = 不含 git 字段，见 EXPERIMENTS.md）。

## 7. 已知限制与已验证场景

- **无 Kafka broker**：logic 的 `WriteMessages` 同步报 dial 错误 → 上行 RPC 失败 →
  链路停在 logic，Kafka 页显示 `available:false` + err，Overview health=critical
  （Kafka 是消息总线，判 critical 是设计行为）。trace 只会有可记录到的环节。
- **同主机多实例**：comet 的 HTTP↔gRPC 地址配对在 host 相同（本地/测试常见）时按
  注册顺序配对，是启发式；跨主机时按主机名精确配对。
- **Docker 中的 Load Test**：loadtest 二进制在 `../monolith`，不在 Dockerfile 构建上下文
  内，容器里该页显示"不可用"（诚实降级）；本地运行时可用。
- **房间枚举**：`/api/v1/rooms` 是 Hub 全量扫描（`core/hub.go GetRoomList`），ops 只在
  打开 Rooms 页时按需调用，不进周期采集；百万连接时避免高频打开该页。
- **压测快照**：ops 解析 loadtest 的秒级 stdout 行；e2e 延迟在无下行（无 Kafka）时为 0。

验证记录（本地 17xxx 端口实链路，2026-08-16）：

| 场景 | 结果 |
|------|------|
| A ops 发现 etcd/logic/comet | ✓ services/topology 正确 |
| B WS 建连 → Active Connections | ✓ 0 → 20/30，实时变化 |
| C 发弹幕 → message rate | ✓ msg_in/s=40，与 loadtest sendQPS 一致 |
| D 停 comet2 | ✓ 事件 ERROR unavailable，comet 1/2 healthy，health degraded（Kafka 同时不可用时为 critical） |
| E 恢复 comet2 | ✓ 事件 registered，2/2 healthy |
| F 经 ops API 跑 loadtest | ✓ running/params/latest 快照/结束 report 全链路可见 |
| trace | ✓ comet 记录 span → ops 汇聚 → missing_hops 正确标注（Kafka 段需真实 broker） |

## 8. 开发约定

- 观测代码必须 O(1)/聚合/有界：禁止为 Dashboard 遍历全部连接；实时消息、事件、trace
  全部是带上限的环形缓冲。
- ops 不依赖任何业务服务才能启动；etcd 掉线用上次清单继续探测并在事件流里报警。
- 测试：`go test ./...`（ops 含 rate 量级、分组去重、loadtest 解析、**实验状态机/
  持久化恢复/损坏容忍/对比/证据**等回归）；前端 `npm run build` + typecheck。
  改动后两者都要跑。
