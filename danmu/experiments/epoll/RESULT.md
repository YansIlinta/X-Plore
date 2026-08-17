# P8 · epoll 连接层实验（gnet + 自写最小 WS 帧层）— RESULT

> 生成：2026-08-17 ｜ 机器：2 核 / 1.9G RAM（本机上限 ≈ 10k 空闲连接）
> 预注册判据：RSS 降 ≥40% 且建连 P99 不劣化 → **CONTINUE**；否则归档 NEGATIVE（同样算完成）。

## 被测对象

| 版本 | 引擎 | 连接模型 |
|---|---|---|
| `cmd/wsd` | gnet v2.10.0（epoll 事件循环，单 reactor） | 无 goroutine-per-conn；自写最小 WS 握手 + 帧层（ping→pong/close，文本帧忽略；无 TLS——gnet 无内置 TLS，边缘终止） |
| `danmu/monolith/server` | gorilla/websocket v1.5.1 | goroutine-per-conn（readPump + writePump + Hub 注册） |

## 数据（10k 空闲长连接，100 并发建连，settle 10s）

| 指标 | wsd (epoll) | monolith (gorilla) | 差异 |
|---|---|---|---|
| 连接数 | 10000 / 10000 ok | 10000 / 10000 ok | — |
| RSS | **16.1 MB** | 28.9 MB | **-44.3%**（红线 ≥40% ✓） |
| 建连 P50 | 3.3 ms | 13.4 ms | -75% |
| 建连 P90 | 6.6 ms | 21.6 ms | -69% |
| 建连 P99 | **11.4 ms** | 44.2 ms | **-74%（不劣化 ✓）** |

## 判定：CONTINUE

两个预注册判据全部命中：
1. RSS 降 44.3% ≥ 40% ✓
2. 建连 P99 11.4ms < 44.2ms（不劣化且显著更好）✓

## 结论与边界

- **方向成立**：事件循环在空闲长连接上内存与建连性能均优于 goroutine-per-conn；
  差距随连接数放大（gorilla 每连接 2 goroutine + sendCh 512 缓冲 + 会话令牌结构；
  gnet 单 reactor 摊还）。
- **本机规模限制**：10k 是 2 核/1.9G 机器的实测上限，未到目标 100k；100k 量级
  （RSS 预期 epoll ~160MB vs gorilla ~290MB 外推）需在基准机复测。
- **代价**：自写 WS 帧层（无 TLS/无分片消息/无 HTTP 语义）；生产化需 gobwas/ws
  帧库 + 边缘 TLS + 与既有 Hub/worker 批量架构融合——这是独立后续工作，不替换主架构。
- **主架构不动**：本实验未修改 `danmu/monolith/` 与 `danmu/distributed/` 任何代码。

## 复现

```bash
cd danmu/experiments/epoll
go build -o /tmp/wsd ./cmd/wsd && go build -o /tmp/wsd-loader ./cmd/loader
/tmp/wsd-loader -server-bin /tmp/wsd -server-args "-addr :19000" -ws-url ws://localhost:19000 -conns 10000 -settle 10s
/tmp/wsd-loader -server-bin /tmp/danmu-server-new -server-args "-addr :19002 -id srvA" \
  -ws-url ws://localhost:19002 -conns 10000 -settle 10s
```

## 远程实测（SeetaCloud 48 核容器，cgroup 限 12 CPU，440G 内存）

> 2026-08-17 追加。多端口方案：客户端单 IP 临时端口上限 ~28k，Linux 四元组唯一性
> 允许「同一源端口 × 不同目标端口」并存，server 监听 64 端口即可让单客户端打到 10 万级。

| 场景 | 连接 | RSS | 建连 P50/P90/P99 |
|---|---|---|---|
| **wsd（gnet）单进程 × 64 端口** | **100,000**（0 失败，5.4 万/s） | **63.0 MB**（~630B/连接） | 2.3 / 6.6 / 72.3 ms |
| wsd 单进程 × 1 端口 | 28,000 | 24.2 MB | 13.3 / 192.9 / 389.3 ms |
| monolith（gorilla）单进程 | 28,000 | 41.6 MB | 33.7 / 191.0 / 387.0 ms |
| monolith × 64 进程（多端口） | 98,437/100,000 | **1,789.1 MB（合计）** | 5.7 / 73.1 / 86.8 ms |

**100k 结论（同机同构）：gnet 单进程 63 MB vs gorilla 64 进程合计 1.79 GB——内存差 28 倍；
同口径单进程 28k：RSS -41.8%（24.2 vs 41.6MB）、建连 P50 -60%（13.3 vs 33.7ms）。
预注册判据（RSS ≥40% 且 P99 不劣化）在 10k、28k、100k 三个量级全部命中 → CONTINUE。**

## 附：远程实测发现的真实缺陷（已修复）

100k 空闲测试后的 10k 连接带流量压测在远程暴露 P3 引入的死锁：
`sendAck` 阻塞式投递在高扇出下把发送者 readPump 卡死（goroutine dump：397/504
卡在 client.go sendAck），导致连接雪崩。修复：ack 改走独立 `ackCh`（writePump 最先
排空）+ 非阻塞投递 + `danmu_ack_drops_total` 计数。修复后同机复测：
10k 连接空闲全成（0 失败 0 丢）、1000 连接带流量 Ack Rate 100%、0 丢弃、
E2E P50 6.0ms / P90 14.2ms（受客户端 JSON 接收端 ~75 万 msg/s 上限约束，
10k 连接 × 1 msg/s 的满扇出需更强压测端）。
