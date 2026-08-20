# EXPERIMENTS.md — Realtime Systems Lab（实验 / 对比 / 证据层）

> 这是 X-Plore 的**旁路实验层**：在既有消息主链之外，让你
> ① 运行 workload、② 观察系统行为、③ 保存结果、④ 对比架构、⑤ 分清
> "验证过的数字" vs "设计目标"。它**不参与消息链路**——实验管理器/证据层
> 即使挂掉，弹幕系统照常工作。

- 后端：`distributed/ops/`（experiment_*.go、compare.go、evidence.go）
- 前端：`distributed/ops/web/src/pages/{Experiments,Compare,Evidence}.tsx`
- 存储：`<data-dir>/experiments/<experiment-id>.json`（无数据库，temp+rename 原子写）

## 1. 为什么做成旁路

| 主链（不可动） | 实验层（新增） |
|---|---|
| monolith：Client→WS→Hub→Worker→Broadcast | 只读聚合 Ops 采集快照 |
| distributed：Client→Comet→Logic→Kafka→Job→Comet.PushRoom→WS | 只启停 loadtest 子进程 |
| Ops 的 Overview/Topology/… 页面 | 复用其 API 数据 + 抽取 RateChart 组件 |

实验层唯一的"副作用"是启动/停止 loadtest 子进程（这是仓库既有的压测实现，
不是第二套引擎）。**不接收任意 shell command**：Manager 只接受 workload 参数，
映射到 loadtest 已支持的 flag（server/conns/rooms/rate/duration）。

## 2. 数据真实性约定（最重要的原则）

**未测量的字段必须是 N/A（null），绝不填 0。** 0 表示真实测量值为零，null 表示
没有测量——两者语义不可混。

当前 loadtest（`monolith/loadtest/main.go`）真实输出 `--output-json` 报告：

- ✅ 测得到：connections requested/established/failed、messages sent/received、
  e2e p50/p90/p99/max（HDR Histogram）、（由每秒快照汇总的）write/read errors。
- ⛔ **drops 未测量**（`dropCount` 从未递增）→ 恒为 `null`，报告中注明原因。
- ⛔ Kafka lag / trace completion / 服务面快照：只有 `architecture=distributed` 且
  旁路 Collector 能看到分布式服务时才记录；monolith 或不达通时全为 `null`。
  （分布式旁路观测在实验结束时从 collector 抓取：comet/logic/job 计数、Kafka
  available/lag、trace 采样数与完整率——详见 `experiment_manager.go:captureDistributed`。）

因此**两个架构不会为了"字段看起来一致"而互相填假数据**。

## 3. 一次实验怎么跑（回到产品）

```text
Experiments 页：
  1. 选 preset（Low Fan-out / Hot Room / Custom）→ 预填 workload
  2. 选 architecture（monolith / distributed）
  3. 可改 workload：connections / rooms / rate / duration / target
  4. Run Experiment → 实时 KPI（连接数、send/recv QPS、E2E 延迟、错误、elapsed）
     - distributed 时额外展示旁路采集面（comet/Kafka/msg rates）
  5. Stop or 等待完成 → 结果 + 复现元数据 + 支撑的 claims 一并落盘
```

## 4. API

```text
GET  /api/presets                      preset 模板（含默认 workload 与想回答的问题）
GET  /api/experiments                 历史（有界，默认 50）
POST /api/experiments                 create（400 校验失败 / 422 未知 preset）
GET  /api/experiments/{id}            详情（running 时带 live 实时快照）
POST /api/experiments/{id}/start      启动（409 已在跑/状态非法 / 404 不存在）
POST /api/experiments/{id}/stop       停止（409 未在跑）
GET  /api/experiments/{id}/report     报告：完整记录 + 复现元数据 + 关联 claims
GET  /api/compare?left=&right=        对比（两侧必须 completed，否则 422）
GET  /api/evidence                    claims 当前状态（VERIFIED 仅来自实验存储）

# 兼容入口：旧 Load Test 页已并入 Experiments；/api/loadtest/* 保留委托同一状态机
POST /api/loadtest/start | stop ；GET /api/loadtest/status
```

约束：全局同时**只能有一个实验在跑**（loadtest 子进程单例）；POST 是 ACTION；
handler 同步执行、受请求 context 约束、不产生额外 goroutine；冲突返回 409 并说明原因。

## 5. 状态机

```text
created ──start──► running ──完成──► completed
                          ├─出错──► failed
                          └─stop──► stopped
completed / failed / stopped ──start──► running
```

重新 start 会覆盖同一实验 ID 的历史结果；需要保留多个版本时**新建实验**。
Compare 只允许 completed 实验（避免与未跑完的实验对比得到误导性"无差异"）。

## 6. Preset（只是参数模板，不写第二套压测引擎）

| Preset | workload 默认 | 系统问题 |
|---|---|---|
| Low Fan-out | 2000c / 1000r / 1/s / 60s | 基础 WS + 广播路径低扇出是否闭环 |
| Hot Room | 1000c / 10r / 2/s / 60s | 高扇出下延迟 / send queue / 广播放大 |
| Custom | 用户全自定 | 任意 workload |

## 7. Compare 与 So what

逐指标对比：**只有语义相同的指标才计算 delta**；一侧为 N/A 时该行 Delta/Δ% 也是 N/A。
Delta = Run B − Run A。verdict 语义：latency/errors/drops 低更好，
throughput/established 高更好。Summary 是**规则化、确定性**文本（无 LLM）；
workload 不同时先加一句"不是同负荷对比"的提示。

## 8. 复现一条实验的确切命令（本地，无中间件 monolith 也可跑）

```bash
# 1) 构建
export PATH=/home/ubuntu/sdk/go/bin:$PATH
cd danmu/monolith && go build -o bin/loadtest ./loadtest/ && go build -o bin/server ./server/
cd danmu/monolith
DANMU_AUTH_TOKEN=danmu-secret-token ./bin/server -addr=:18081 -id=srv1 -mq=none   # 无中间件降级本机广播

# 2) Ops（实验/证据/对比后端）
cd ../distributed && go build -o bin/ops ./cmd/ops/
DANMU_AUTH_TOKEN=danmu-secret-token ./bin/ops -addr=:17900 -etcd=localhost:2379 -kafka="" \
  -loadtest-bin=/home/ubuntu/X-Plore/danmu/monolith/bin/loadtest \
  -data-dir=/tmp/xplore-lab/data -repo-dir=/home/ubuntu/X-Plore

# 3) 浏览器 http://localhost:17900 → Experiments → Run
#    或命令行：
ID=$(curl -s -X POST localhost:17900/api/experiments -H 'Content-Type: application/json' -d '{
  "name":"repro","architecture":"monolith","preset":"low-fanout",
  "workload":{"connections":200,"rooms":100,"message_rate":2,"duration":"8s","target":"ws://localhost:18081"}}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["experiment"]["id"])')
curl -s -X POST localhost:17900/api/experiments/$ID/start
curl -s localhost:17900/api/experiments/$ID/report | python3 -m json.tool
```

## 9. 本会话实测记录（仅列真实跑过的）

环境：本机 2 vCPU / Linux amd64 / go1.26.6；monolith `-mq=none` 本机广播；
loadtest 与 server 同机（共 2 vCPU → 延迟偏保守）。repo 主链 commit `e124800a`，
工作区 dirty（未被管理的 `.claude/`）。

| 实验 | workload | established | sent | recv | P50 | P90 | P99 | errors(w/r) |
|---|---|---|---|---|---|---|---|---|
| acceptance-low-fanout | 200c/100r @2/s 8s | 200 | 2075 | 3650 | ~10.1ms | ~17.8ms | ~20.7ms | 0/0 |
| acceptance-low-fanout-2 | 200c/100r @2/s 8s | 200 | – | – | – | ~17.5ms | – | 0/0 |
| acceptance-hot-room | 400c/10r @3/s 8s | 400 | 6256 | 212276 | ~13.5ms | ~21.6ms | ~29.2ms | 0/0 |

> 同配置 A/B（run1 vs run2）：P90 17823→17455 µs（-2.1%），落在 ±10% 内 → 判定
> "无显著差异"，说明规则不是拿噪声刷结论。Hot Room 比 Low Fan-out P90 +21%、广播
> 放大 ~34×（recv≈34×sent）——"热门房间更难"是数字可证的。
>
> 注意：这些是**当前机器 + 当前 commit** 的一次观测，不必代表任何其他环境；
> 想复现请按 §8 在你自己机器上跑 → Evidence 页会自动把 claim 从 TARGET 升级为
> VERIFIED（或 PARTIALLY VERIFIED）。

## 10. 已知限制（诚实边界）

- drops 未测量（loadtest 无投递丢失计数器）→ N/A。想做 drop 研究需先给 loadtest
  增加每消息去重/缺口计数（新能力，需单独实现 + 测试 + 文档）。
- Kafka lag / trace 完成率只在 `architecture=distributed` 且 Ops 能观测到分布式服务时
  记录；无 Kafka broker 时 kafka 段保持 N/A（页面显示 Unavailable）。
- 分布式实验需要你先自行把 Comet/Logic/Job/Kafka/etcd 跑起来（`run-goim-local.sh` 或
  compose），Ops 只旁路观测，不会替你拉起服务。
- 实验层不引入 PostgreSQL/ClickHouse 等新存储；持久化就是 `<data-dir>/experiments/*.json`。
- 同机压测（loadtest 与 server 共享 CPU）测得的延迟偏低可信度——README 已提示压测机宜分机。

---

# Phase 1.5 — Reproducible Benchmark & Design-Space Lab

本阶段把 `Single Run → Result → Compare → Evidence` 升级为
`Experiment Spec → Repeated Runs → Statistical Aggregate → Parameter Sweep →
Workload Regime Comparison → Reproducible Evidence`。配套文档：
[BENCHMARKING.md](./BENCHMARKING.md)（测量/统计语义）、[SWEEPS.md](./SWEEPS.md)（扫描/跨 regime/Go-No-Go）、
[EVIDENCE.md](./EVIDENCE.md)（新 claims）。

## 11. 领域模型

| 概念 | 说明 | 存储 |
|---|---|---|
| Run | 一次真实执行（独立 environment/resource/result/error） | experiment 内 `runs[]` |
| Experiment | spec 不可变 + Repetitions + 顺序执行的 Runs + Aggregate | `<data-dir>/experiments/<id>.json` |
| Sweep | 多个 Experiment（Cartesian product: configs × regimes × repetitions） | `<data-dir>/sweeps/<id>.json` |

`spec_hash` = SHA-256(canonical spec)：相同 spec ⇒ 相同 hash；workload/system 任意变化 ⇒ hash 变化。
`schema_version` 从 Phase 1 老文件 0 迁移到 1，老实验（无 repetitions）视作 repetitions=1。

## 12. 状态机（Phase 1.5）

```text
created ──start──► running ──(全部成功)──► completed
                          ├─(部分成功)──► partial          # 4/5 成功不许伪装全成功
                          ├─(全部失败)──► failed
                          └─(用户 stop)──► stopped
partial/failed/stopped ──start──► running（恢复：跳过已完成 rep，重试其余）
```

## 13. API（新增）

```text
GET  /api/regimes                             4 个 workload regime 默认模板 + 排名目标
POST /api/experiments                         新增 regime/warmup/duration/repetitions/system_config
GET  /api/experiments/{id}                    含 spec/spec_hash/runs/aggregate（向后兼容）
POST /api/sweeps  ·  GET /api/sweeps  ·  /api/sweeps/{id}  ·  {id}/start  ·  {id}/stop  ·  {id}/report
GET  /api/regime-analysis                      基于已完成实验的确定性 cross-regime 视图
```

`schema_version` 落盘；老文件读时自动迁移（不写回污染历史）。

## 14. 本地复现（Phase 1.5）

```bash
# 构建
cd danmu/monolith && go build -o bin/loadtest ./loadtest/ && go build -o bin/server ./server/
cd ../distributed && go build -o bin/ops ./cmd/ops/

# 1) 一个受控 monolith server 作为独立目标（也可让 ops 为系统参数 sweep 自动拉起）
DANMU_AUTH_TOKEN=danmu-secret-token ./monolith/bin/server -addr=127.0.0.1:18081 -mq=none -pprof=127.0.0.1:0

# 2) Ops（含 sweep/server 受控进程）
DANMU_AUTH_TOKEN=danmu-secret-token ./bin/ops -addr=:17900 -etcd=localhost:2379 -kafka="" \
  -loadtest-bin=../monolith/bin/loadtest -server-bin=../monolith/bin/server \
  -data-dir=/tmp/xplore-lab/data -repo-dir=/home/ubuntu/X-Plore

# 3) 带 regime / repetitions 的实验
curl -s -X POST localhost:17900/api/experiments -H 'Content-Type: application/json' -d '{
  "name":"repro","regime":"hot-room","architecture":"monolith",
  "warmup":"2s","duration":"8s","repetitions":5,"workload":{"target":"ws://127.0.0.1:18081"}}'
# 4) system-config sweep（batch_size × batch_timeout × 3 regimes × 5 reps）
curl -s -X POST localhost:17900/api/sweeps -H 'Content-Type: application/json' -d '{
  "name":"acceptance","regimes":["low-fanout","hot-room","skewed-hot-room"],
  "params":[{"name":"batch_size","values":["2000","5000","10000"]},
            {"name":"batch_timeout","values":["5ms","20ms"]}],
  "repetitions":5,"warmup":"2s","duration":"8s","target":"ws://127.0.0.1:18181"}'
```

## 15. Phase 1.5 acceptance（真实运行，见最终交付报告）

- 3 个 workload regime：Low Fanout / Hot Room / Skewed Hot Room
- ≥3 个 system configs（真实可调的 startup 参数）
- 每个 5 repetitions —— 结构不可缩水；规模可按本机 2 vCPU 调小
- 交付物：每个 Experiment 的方差/CV、稳定性结论、每 regime best static config、
  cross-regime 是否 shift、Adaptive-Control Gate（GO / NOT YET JUSTIFIED）+ 每条条件的证据。

> 最终原则：**不超过实验证据的范围下结论**。写 "best observed configuration"，
> 不写 "optimal"；写 "observed static optimum varies across tested regimes"，
> 不写 "adaptive control is necessary"。
