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

## 持续剖析

- 可选：`PYROSCOPE_ADDR=http://localhost:4040` 启动时接入 Pyroscope agent。
- compose 已加 `pyroscope` 服务（`:4040`）。
- pprof 仍在 `-pprof :6060` 常开。
