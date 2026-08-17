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
