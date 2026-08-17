# Go 1.26 Baseline（本机 2 核 / 1.9G）

> 生成：2026-08-17 ｜ toolchain：`go1.26.6 linux/amd64`（Green Tea GC 默认启用）
> 机器约束：压测端与 server 同机，数字**不可与**原 256 核基准机（P50 1.6ms @10k）直接对比。
> 正式 10k 低扇出回归须在原基准机复测。

## 工具链

| 项 | 值 |
|---|---|
| go.mod | `go 1.26.0`（monolith；distributed 已是 1.26） |
| runtime | go1.26.6 |
| GC | Green Tea GC（Go 1.26 默认） |

## 高基数指标

Prometheus 标签已在既有代码中规避房间/用户高基数（`metrics.go` 注释明确：
「不以 room_id 为标签」）。`danmu_messages_total` 仅 `direction=in|out`。
OTel SDK Views 在此前提下无额外改造必要——既有方案即低基数正解。

## 本机压测快照（Go 1.26.6）

| 场景 | conns | rooms | rate | duration | Sent | Recv | Ack Rate | E2E P50 | E2E P90 | 错误 |
|---|---|---|---|---|---|---|---|---|---|---|
| 低扇出 sanity | 300 | 1 | 2 | 15s | 7885 | 2.29M | — | 17.1ms | 28.4ms | 0 |
| P1 A/B 旧版 | 300 | 1 | 2 | 15s | — | — | — | 17.0ms | 28.1ms | 0 |
| P1 A/B 新版 | 300 | 1 | 2 | 15s | — | — | — | 17.1ms | 28.4ms | 0 |
| P3 + priority 5% | 200 | 1 | 4 | 20s | 15054 | 2.96M | **100%** | 16.8ms | — | 0 |
| 双机 sharded Redis | 200 | 2 | 2 | 20s | 7272 | 711k | — | 10.4ms | 18.9ms | 0 |
| 双机 classic Redis | 200 | 2 | 2 | 20s | 7275 | 711k | — | 10.4ms | — | 0 |

结论：
1. Go 1.26 下构建/测试全绿（含 `-race`）。
2. P1 热历史改动相对旧版 P50 回退 <1%（本机噪声范围内）。
3. Redis sharded vs classic 在本机规模下 E2E 持平（吞吐优势需集群高扇出）。
4. 10k @1.6ms 基线须原机复测——本机 2 核同机压测无法复现该量级。

## 远程复测（SeetaCloud 容器：128 核可见 / cgroup 限 12 CPU，GOMAXPROCS=12）

> 2026-08-17。压测端 = 优化后的 loadtest（scanFrame 快速扫描器，无整帧 JSON 解析）。

### 官方 10k 剧本（10000 连接 / 50 房间 / rate 0.5 / 120s）

| 结果 | 值 |
|---|---|
| 连接 | **10000/10000 建立**，0 失败 |
| Dropped | **0**（server 全程无丢弃） |
| 帧率墙 | 12-CPU 配额下客户端 ~50 万帧/s / ~1.05M msg/s 封顶；满 10k 后 P50 100ms+（**客户端测量上限**，server 侧无死锁——goroutine dump 健康） |

> 注：worker 每 20ms 批量一帧 × 10k 连接 = 50 万帧/s，容器 12-CPU 配额把客户端压死在帧率墙上；
> 原 256 核基准机的 1.6ms 基线需在该机器复测（负载脚本已就绪）。

### 5000 连接 × 50 房间 × rate 0.5 新旧 A/B（同机同参数）

| 版本 | P50 | P90 | P99 | Dropped | Ack Rate |
|---|---|---|---|---|---|
| OLD（37f1681，本轮前） | 5.7ms | 15.4ms | 31.4ms | 0 | —（无 ack 功能） |
| NEW（HEAD，复跑 2 次） | **6.9 / 7.0ms** | 18.9 / 17.3ms | 39.5 / 30.0ms | 0 | **98.3%**（尾损 1.7% = 测试结束时在途） |

结论：
- NEW 相对 OLD +1.2ms P50（~+20%）——本轮功能开销（ack 帧 + seq/热历史 + 幂等 + 词库/慢速逐消息检查）。
- 绝对量级（5.7~7ms）远高于基准机 1.6ms（12-CPU 配额 + 帧率墙），**10% 回归门槛必须在基准机复测**；
  本机 A/B 的意义：功能开销有界（毫秒级）、无死锁、0 丢弃。

## 持续剖析

- 可选：`PYROSCOPE_ADDR=http://localhost:4040` 启动时接入 Pyroscope agent。
- compose 已加 `pyroscope` 服务（`:4040`）。
- pprof 仍在 `-pprof :6060` 常开。
