# ACCEPTANCE.md — Phase 1.5 真实运行结果（3 regimes × 3 configs × 5 reps）

> 本文件记录一次**真实运行**的 acceptance（不是示例）。数据在 `./acceptance-data/`：
> 运行 `bin/ops -data-dir=acceptance-data`（连同 `-loadtest-bin` / `-server-bin` / `-repo-dir`）
> 后，Evidence 页会自动从这些实验推导 claim 状态。想复现请看 [EXPERIMENTS.md §14](./EXPERIMENTS.md)。

## 0. 环境与复现信息

```text
机器      : 2 vCPU / 2 GB RAM / Linux amd64（VM-0-13-ubuntu）
架构      : monolith，受控 server（-mq=none 本机广播），loadtest 与 server 同机
Go        : go1.26.0 linux/amd64（实验运行时逐 run 采集 environment）
工作负载  : 150 conns @1.5 msg/s/conn（workload_overrides），每 run warmup=2s measure=5s
配置      : server batch_timeout ∈ {5ms, 20ms, 50ms}（requires_restart，每 config 重启受控 server）
repetitions: 5  ×  3 configs × 3 regimes = 45 runs（顺序执行，~8.7 min）
commit    : 运行跨 2e80866 / 0d6c410 / 6231c5ff（run 各自记录实际 commit；本报告代码基线 6231c5ff），工作区 dirty
日期      : 2026-08-20
```

复现 sweep 请求：

```bash
curl -s -X POST localhost:17900/api/sweeps -H 'Content-Type: application/json' -d '{
  "name":"acceptance-3x3x5","architecture":"monolith",
  "regimes":["low-fanout","hot-room","skewed-hot-room"],
  "params":[{"name":"batch_timeout","values":["5ms","20ms","50ms"]}],
  "workload_overrides":{"connections":"150","message_rate":"1.5"},
  "repetitions":5,"warmup":"2s","duration":"5s","target":"ws://127.0.0.1:18181"}'
```

## 1. 结果矩阵（每格 = Experiment aggregate，median/mean/CV/95% bootstrap CI）

| Regime | Config | Throughput (msg/s) mean | CV | P99 median (µs) | P99 CV | Delivery rate mean | Server CPU % mean |
|---|---|---|---|---|---|---|---|
| Low Fanout | bt=5ms | 115.9 | 0.000 | **12,255** | 0.132 | 1.0000 | 3.5 |
| Low Fanout | bt=20ms | 115.8 | 0.001 | 23,807 | 0.085 | 1.0000 | 2.6 |
| Low Fanout | bt=50ms | 115.7 | 0.003 | 55,359 | 0.111 | 1.0000 | 2.2 |
| Hot Room | bt=5ms | 11,204 | 0.000 | **32,719** | 0.128 | **0.9399** | 4.2 |
| Hot Room | bt=20ms | 11,200 | 0.001 | 40,415 | 0.066 | 0.8505 | 3.2 |
| Hot Room | bt=50ms | 11,193 | 0.001 | 71,039 | 0.105 | 0.9796 | 2.9 |
| Skewed Hot Room | bt=5ms | 1,194 | 0.001 | 20,079 | 0.557 | 0.9981 | 3.7 |
| Skewed Hot Room | bt=20ms | 1,192 | 0.003 | **27,279** | 0.023 | 1.0000 | 2.8 |
| Skewed Hot Room | bt=50ms | 1,193 | 0.001 | 51,807 | 0.072 | 0.9886 | 2.4 |

P99 的 95% bootstrap CI（示例，µs）：
`low-fanout/bt=5ms: [11,090, 13,601]` · `hot-room/bt=5ms: [31,685, 38,690]` · `skewed/bt=20ms: [27,093, 28,028]`

## 2. 回答验收的科学问题（全部由本轮数据支撑）

1. **每个 Experiment 的运行方差**：吞吐 CV 全部 ≤0.003（极稳）；**P99 尾部延迟 CV 0.023–0.557**，其中 skewed-hot-room/bt=5ms 的 CV=0.557 是真实的高变异性。
2. **benchmark 是否稳定**：吞吐与投递率重复性极稳（CV≈0.001–0.01）；P99 在中/大 batch_timeout 下稳定（CV<0.15），小 batch_timeout+高扇出下变异性大。结论：**主指标稳定，尾部延迟在部分配置下有高方差**。
3. **每个 regime 的 best static config**（约束：P99≤50ms、delivery≥99.9%）：
   - Low Fanout → **bt=5ms**（P99 12.3ms、delivery 100%）
   - Hot Room → **NO FEASIBLE CONFIGURATION**（bt=5ms 吞吐最优但 delivery 93.99% < 99.9%；其余也不达标）
   - Skewed Hot Room → **bt=20ms**（P99 27.3ms、CV 0.023、delivery 100%，比 bt=5ms 的 0.557 方差稳定得多）
4. **相同 config 随 workload 的变化**：bt=50ms 时低扇出 P99 55ms，热房 71ms —— 同一配置在高扇出下尾部延迟 +28%。
5. **是否存在始终占优的 config**：**否** —— `STATIC OPTIMUM SHIFTS ACROSS WORKLOAD REGIMES`（low-fanout→bt=5ms、skewed→bt=20ms）。
6. **最优 static configuration 是否随 workload 改变**：**是**（第 5 点）。
7. **optimum shift 差异有多大**：P99 中位数差异 ~15ms（12.3 vs 27.3ms）；主配置切换 batch_timeout 5ms↔20ms。
8. **是否有足够证据支持 Adaptive Control**：**NOT YET SUPPORTED**（详见第 4 节 gate 判定）。

> 诚实边界：2 vCPU 同机压测，P99 绝对数值偏保守；且少数 run 在运行期间仓库有代码提交
> （run 各自记录 commit）。结论的**结构**（跨 regime 最优不同、热房投递劣化、方差差异）是可复现阶段观察。

## 3. 每个 regim 的稳定度

| Regime | 吞吐 CV | P99 CV（最优 config） | 判定 |
|---|---|---|---|
| Low Fanout (bt=5ms) | 0.000 | 0.132 | throughput 完美、尾延迟可重复 |
| Hot Room (bt=5ms) | 0.000 | 0.128 | 吞吐稳定；投递率波动 |
| Skewed Hot Room (bt=20ms) | 0.003 | 0.023 | 最优配置下两者都稳 |

## 4. Adaptive-Control Research Gate（离线判定，非 controller）

```text
Condition A · ≥2 regime 的最优 static config 不同        : TRUE （low-fanout→bt=5ms, skewed→bt=20ms）
Condition B · best 相比固定 default 有实际 improvement   : FALSE（本验收未 sweep "default"，无法证 >10% 改善）
Condition C · benchmark 方差足够低                      : FALSE（skewed/bt=5ms P99 CV=0.557；需更多 repetition 或更稳配置）
Condition D · 存在可安全调节的系统参数                   : TRUE （batch_timeout 可经受控 server 调节）

=> ADAPTIVE CONTROL RESEARCH: NOT YET JUSTIFIED
```

证据链在 `acceptance-data/sweeps/sweep-*.json` 的 `report.adaptive_gate.evidence`。

## 5. Evidence 自动推导（来自 acceptance-data）

```text
VERIFIED  claim-repeatability-observed            存在多 repetition 实验且聚合稳定
VERIFIED  claim-hot-room-higher-tail-latency      hot-room P99 median 40.4ms > low-fanout 23.8ms
VERIFIED  claim-static-optimum-shifts             STATIC OPTIMUM SHIFTS ACROSS REGIMES
VERIFIED  claim-delivery-accounting-supported     实验真跑出 delivery_rate/missing（热房 85–98%）
UNKNOWN   claim-one-config-dominates-all          没有任何单一 config 统治所有 regime
```

再次强调：Gate 是 **NOT YET JUSTIFIED** —— 现证据支持"配置随 workload 变化"这一观察，
但尚不支持"值得做一个 controller"。Phase 2 需要：更多 repetition 压低方差 + 扫出
相对 default 的确定性改善。
