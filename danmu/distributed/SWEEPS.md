# SWEEPS.md — 参数扫描 / 跨 workload regime 分析（Phase 1.5）

> Sweep 页（`#/sweeps`）把"配置空间"一次性跑出来：
> **规格 → 重复运行 → 统计聚合 → 参数扫描 → 跨 regime 对比 → 可复现证据**。

## 1. Sweep 是什么

```text
Sweep
 ├── Experiment config-A × regime-1
 ├── Experiment config-B × regime-1
 ├── Experiment config-C × regime-1
 ├── Experiment config-A × regime-2
 └── ...
```

MVP 支持 **deterministic Cartesian product**：

```yaml
batch_size:    [100, 500, 1000, 2000]
batch_timeout: [5ms, 20ms]
```

→ 8 个 configs，每个 regime 都跑一遍；每个 config×regime 是一个 Experiment（含 repetitions）。

## 2. 安全上限（§16）

- `max_configs = 32`、`max_total_runs = 120`（默认）。超出 → 422 validation error。
- 创建时前端/后端都先展示：`8 configurations × 5 repetitions = 40 runs` 再运行。
- Sweep 顺序执行（不并行 benchmark）。

## 3. 哪些参数可以 swept

只扫描**当前代码真实存在、且安全暴露**的参数（`ops/sweep.go` 白名单）：

| 参数 | 属于 | 是否需重启 server |
|---|---|---|
| `batch_size` | server（worker 批量聚合条数） | 是 |
| `batch_timeout` | server（worker 批量聚合超时） | 是 |
| `workers` | server（worker 池大小） | 是 |
| `connections` / `rooms` / `message_rate` / `distribution` / `zipf_s` | workload（loadtest） | 否 |

**诚实建模**：batch_size / batch_timeout / workers 在代码里是 **startup config**，改它们必须重启
server 进程 → `system_config.requires_restart = true`。Sweep 遇到系统参数时，由 ops 用
**安全固定 argv**（`ServerProcessManager`，与 loadtest 同款子进程模式）为每个 config 拉起一个
受控 monolith server（`-mq=none` 本机广播），跑完释放。绝不引入任意 shell command / 任意进程管理器。

如果你没有配置 `-server-bin`，系统参数 sweep 会明确失败（而不是偷偷按默认跑）。

## 4. Sweep 结果矩阵（§18）

每一个格子来自 **Experiment aggregate**（不是单次 run）：

```text
Config        Throughput     P99        Delivery     CPU
A             34.2k          25ms       99.99%       62%
B             38.1k          31ms       99.99%       71%
C             40.0k          66ms       99.40%       91%
```

可点击进入 Sweep → Experiment → Runs 全链路 traceable。

## 5. Best Static Configuration（§19, §20）

**不是 "highest throughput wins"。** 约束化排名：

```text
maximize throughput (或 p99 / delivery_rate)
subject to:
  P99 <= 50ms（可配）
  delivery_rate >= 99.9%（可配）
  CPU <= Z%（可配）
```

- 满足约束的 config 里选主目标最优 → **best observed configuration**。
- 没有任何 config 满足约束 → **NO FEASIBLE CONFIGURATION**（绝不偷降约束）。
- 不实现 `J = throughput - λ1·latency ...` 式的权重 reward（单位不同、权重任意）作为默认结论。

## 6. Cross-Regime 分析与 Static Dominance（§21, §22）

同一个配置在多个 workload regime 下运行，得到：

```text
LowFanout       → Config A
HotRoom         → Config C
SkewedHotRoom   → Config B
```

- 若不同 regime 的 best 不同 → **STATIC OPTIMUM SHIFTS ACROSS WORKLOAD REGIMES**。
- 若同一个配置一直最好 → **NO EVIDENCE OF REGIME-DEPENDENT STATIC OPTIMUM**。
- Configuration A **支配** B：A 在所有目标/约束上不劣于 B，且至少一个更优。

结果可能否定未来研究假设（比如"根本没有需要自适应调整的东西"）——这是正常结果，如实输出。

## 7. Adaptive-Control Gate（§35，离线判定，**不是 controller**）

未来 Phase 2 是否值得做，由如下条件判定（每一条都显示证据）：

| 条件 | 含义 |
|---|---|
| A | ≥2 个 workload regime 的 best static config 不相同 |
| B | best config 相比固定 default 有实际可测 improvement（>10%） |
| C | benchmark 方差足够低（aggregate 成功 reps ≥2 且 stability 可用） |
| D | 存在至少一个可安全调节的系统参数 |

全部满足 → `ADAPTIVE CONTROL RESEARCH: GO`；否则 `NOT YET JUSTIFIED`。
这是实验结论，不是自动产品决策。

> **为什么还没有 adaptive controller：** 只有实验先证明"配置需要随 workload 变化"
> 这件事是**真**的（且能被低噪声基准看见），才值得做 Phase 2。Phase 1.5 的全部工作
> 就是建立这个 strong static baseline 与 Go/No-Go 证据。

## 8. API（§30）

```text
POST /api/sweeps                    创建（400/422 validation）
GET  /api/sweeps                    历史
GET  /api/sweeps/{id}               详情 + progress
POST /api/sweeps/{id}/start         开始/恢复（409 冲突）
POST /api/sweeps/{id}/stop          停止（已完成的 config×regime 单元保留）
GET  /api/sweeps/{id}/report        结果矩阵 + cross-regime + adaptive gate
GET  /api/regime-analysis           基于已完成实验的确定性 cross-regime 视图
GET  /api/regimes                   4 个 workload regime 的默认 workload
```

## 9. 测试

sweep 逻辑是纯函数 + 顺序执行器，均有单测：

- Cartesian product（8×2=16 configs）、sweep 上限（32/120）、未知 regime/参数 422
- 执行与 stop（stopped 后已完成单元保留）、resume/recovery（重启后仍能读到 report + gate）
- best static config、约束违反（NO FEASIBLE）、cross-regime winner、dominance、adaptive gate

## 10. 复现 acceptance

见 `EXPERIMENTS.md` §「Phase 1.5 acceptance」：3 regimes × 3 configs × 5 reps 的真实运行结果、
commit、环境、workload、配置、repetitions、median、CI、CV，以及最终 Adaptive-Control Gate。
