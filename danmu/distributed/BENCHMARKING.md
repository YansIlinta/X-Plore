# BENCHMARKING.md — 基准正确性语义（Phase 1.5）

> Realtime Systems Lab 的统计与测量语义，是"可复现基准"的契约。
> 这一页回答一个最重要的问题：**测量到的数字到底意味着什么？**

## 1. Run / Experiment / Sweep（三个概念必须分清）

```text
Run        = 一次真实执行（一个 workload 跑一次）
Experiment = 一个需要重复执行的规格（spec 不可变）+ 它的全部 Run
Sweep      = 多个 Experiment（deterministic Cartesian product）
```

- 一个 Run 有自己独立的 started/finished、environment、measurement window、result、资源采样、错误。
- 一个 Experiment 把它们顺序执行，**不并行**（避免资源争用与不可归属的干扰）。
- `repetitions` 默认 3，允许 1–20。`5 runs 中 4 成功 1 失败` → 实验状态 **PARTIAL**，绝不伪装成全成功。

## 2. Warm-up 与 Measurement Window（§8）

```text
connect → warm-up（发送流量，不计入统计）→ measurement starts → collect → measurement ends → drain
```

- warm-up 期间**照常打流量**（让系统充分预热：连接、批量、GC 都进入稳态），
  但在 `measureStart` 那一刻对测量基线**整体归零**（计数器基线 + 替换 e2e 直方图 + 推进 epoch）。
- 只有 measurement 窗口内的数据才进 latency / throughput / delivery 聚合。
- Run 记录 `measurement_start` / `measurement_end`（真实观测窗口）；UI 与文档都可据此复核。
- 速率 = 测量窗内的 total ÷ 测量窗真实耗时，**不是**"墙钟总时长"。
- 这不是"sleep + 截掉随机数据"：归零是定义明确的测量边界，不是事后随机截断。

## 3. 统计聚合（§9）

对 Experiment 的成功 repetitions，每个真正测到的数值指标计算：

| 量 | 含义 |
|---|---|
| count | 参与聚合的成功 run 数（成功 run 全部计入，无论该指标是否测到） |
| samples | 其中**真正测到**该指标的 run 数（展示为 `samples/total_rep`，如 3/5） |
| mean / median / min / max | 对实测值求 |
| stddev | 样本标准差（ddof=1） |
| CV | 变异系数 stddev/mean；`[10,10,10]` → CV=0（完美稳定） |
| 95% CI | 均值的百分位 bootstrap 区间（B=1000，可注入 seed 保证确定性） |

**铁律：只聚合实测值。** 某指标在一次 run 里没测到（N/A）→ 不进聚合、不算 0；
`null ≠ 0`。某指标在所有成功 run 都 N/A → 该指标整体 Measured=false，相关字段全 null。
置信区间：`n<3` 时明确标记 `insufficient_samples`，**绝不制造假区间**。

## 4. 投递核算（§12）

- 目标不是"发了多少"（sent），而是**按连接的实际投递**（observed）与缺口（missing）。
- loadtest `-delivery-check` 模式对每条连接做 **room seq 连续性跟踪**：
  期望 nextSeq = lastSeq+1；seq 跳跃 → 缺口（missing）；`seq ≤ lastSeq`（重放）不计缺口。
- `expected = observed + missing`；`delivery_rate = observed / expected`。
- **缺失的连接投递次数 = missing_deliveries**，它等价于真实的 drops（不再恒为 null）。
- 关键：**sent ≠ delivered**。投递 ker accounted 不把"发送数"当"投递数"。
- 该模式只在 loadtest（基准客户端）启用，不改变任何正常业务消息语义。
- 计算正确性有纯单测证明（缺口/重放/跨帧/epoch 重置）。若某场景无法可靠核算 → 保持 N/A，不硬造。

## 5. 服务器资源测量（§11）

- 实验期间按有界间隔（默认 1s）采样**目标 server 自身**的 `/api/v1/stats`：
  goroutines / heap / RSS / CPU% / GC cycles / GC pause。
- CPU% = 进程累计 CPU 时间增量 ÷ 墙钟增量 ÷ 核数；RSS 读目标进程 `/proc/self`（不越权、不读全系统冒充）。
- 采样器随实验结束退出（go test -race 断言无 goroutine leak）；拿不到 → null。
- 每个 run 保存 mean/peak 与有界 samples（≤240 点）供画趋势；不无限存高频时序。

## 6. Workload Skew（§13）

- 房间热度分布：`uniform`（conn % rooms）、`hot_room`（80% 进最热房）、`zipf`（s 参数）。
- 相同 `seed` 产出相同分配（确定性、可复现）。
- 每次 run 记录**实际分配**的 diagnostics：
  `largest_room_share`、`top_10_percent_room_share`、`mean/max/min_room_size`。
  这样 "Hot Room" 不只是名字，而是可以被数据证明的负载形态。

## 7. 解释结果时的纪律（不做的事）

- 不写 "optimal"，除非搜索整个配置空间——只写 **best observed configuration**。
- 不写 "statistically significant"，除非实现了正式统计检验——MVP 用确定性规则给出
  likely improvement / likely regression / no clear difference / high variance。
- 不做 LLM 摘要：所有结论由约束化排名 / 占优分析规则生成。
- 单次实验的数字可以很低可信度（如 2 vCPU 同机压测）；repetition 的 CV / 置信区间让不可信可见。
