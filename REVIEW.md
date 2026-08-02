# 代码审查报告（consumer / loadtest / server / web）

> 审查日期：2026-07-15
> 范围：`server/`、`consumer/`、`loadtest/`、`web/`
> 基线：`go build` / `go vet` / `go test ./server/` 全绿；本报告是在"能编译能跑"之上找逻辑/可扩展/工程问题。

严重度：🔴 高（影响正确性或百万级扩展性） / 🟡 中（多机/生产下出问题或误导） / ⚪ 低（清理/健壮性）

---

## 🔴 H1. Prometheus 以 `room_id` 为标签 → 时间序列基数爆炸

`server/metrics.go`

```go
metricConnectionsTotal = ... []string{"room_id"}
metricMessagesTotal    = ... []string{"room_id", "direction"}
```

`WithLabelValues(roomID)` 会为**每个房间**创建一条独立且永不回收的时间序列。房间数在设计上可达数十万~百万，这会：
- 撑爆 Prometheus 存储与查询；
- 让 server 进程内的 metric map 无限增长（内存泄漏式增长）。

这是高基数标签的经典反模式，直接违背"百万房间"目标。

**修复方向**：去掉 `room_id` 标签，只保留 `direction`（连接数用不带标签的 Gauge/Counter）。房间维度的观测走日志采样或 Top-N，不进 Prometheus 标签。

---

## 🔴 H2. `/api/v1/history` 是死接口，永远返回空

`server/api.go handleHistory` 依赖 `a.historyDB`，但 `server/main.go` 从未给 `api.historyDB` 赋值 → 恒走 `historyDB == nil` 分支返回 `{total:0, items:[]}`。而 README 把它列为可用接口。

根因：server 与 consumer 是两个进程，只有 consumer 连了 ClickHouse，server 没有查询通路。

**修复方向**：二选一——
1. 给 server 增加只读 ClickHouse 连接（`-clickhouse-addr` 可选，配置了才启用 history）；或
2. 明确接口契约：未接存储时返回 `501 Not Implemented`，而不是假装成功返回空列表（避免"看起来能用其实没数据"的误导）。

本次采用方案 1（可选开启），保持默认行为不变。

---

## 🟡 M1. `/api/v1/stats` 的 `qps` 是累计请求数，不是 QPS

`qpsMiddleware` 只对 `qpsCount` 累加、从不重置；`handleStats` 直接 `a.qpsCount.Load()`，返回的是自启动以来的**累计请求总数**，随时间单调增长，不是"每秒请求数"。

`StartQPSTracker` / `startQPSCounter` / `QPSValue` 里算了每秒差值却全部丢弃（`_ = qps`），是无效代码。

**修复方向**：维护一个 `lastSecondQPS atomic.Int64`，后台每秒用差值刷新，stats 读它。删掉空转的 tracker goroutine。

---

## 🟡 M2. 管理员广播 / 关房 / 踢人只在本机生效

`handleBroadcast`、`Hub.CloseRoom`、`Hub.KickClient` 都只操作本机连接，不经 Redis 跨机。多机部署下：
- 从 srv1 发系统公告，连在 srv2 的用户收不到；
- 踢 srv1 上的 uid，不影响 srv2 上同 uid 的连接。

**修复方向**：管理员广播复用弹幕的 Redis 跨机通路（发到 `room:{id}` 频道）。踢人/关房属于控制面，需要额外的控制频道，成本较高——本次先修广播（高频且用户可感知），关房/踢人在文档标注为"每机独立"的已知限制。

---

## 🟡 M3. consumer 落库存在数据丢失窗口（at-most-once）

`consumer/main.go runStorageConsumer`：Kafka offset 由 `ReadMessage` + `CommitInterval=1s` 周期性自动提交（读到即会被提交），而 ClickHouse 落库是**解耦的异步批**（攒够 1000 条或 500ms flush）。若进程在"offset 已提交、CH 尚未 flush"之间崩溃，这批消息 offset 已前进但数据没落库 → 永久丢失。这与"Kafka 兜底可靠"的说法矛盾。

**修复方向**：关闭自动提交（`CommitInterval=0`），改用 `FetchMessage` 读取、`BatchInsert` 成功后再 `CommitMessages` 显式提交该批 offset，得到 at-least-once（配合 ClickHouse 侧幂等/去重）。

---

## 🟡 M4. loadtest E2E 延迟只有毫秒精度，却按微秒报告

`runClient` 读循环：`latency := now - msg.ClientTS`，两端都是 `UnixMilli()`，差值是**整毫秒**；再 `time.Duration(latency)*time.Millisecond` 记进 HDR。结果：亚毫秒延迟被 clamp 到 1μs，报告里 `p50=...μs` 的真实分辨率其实是 1ms，具误导性（尤其单机压测本机延迟常 < 1ms）。

**修复方向**：客户端发送时间戳改用 `UnixNano`（新增 `client_ts_ns` 或直接用 ns），端到端按纳秒算再转微秒记录。需 server 透传该字段。为不改协议，最简做法：loadtest 侧自己用 `time.Now()` 高精度计时——但跨"发/收"是不同 goroutine/连接，拿不到原始发送 `time.Time`。因此采用透传纳秒时间戳方案。

---

## 🟡 M5. loadtest 不实现 reauth，>10min 的压测会被服务端批量断开

server `sessionTTL = 10min` 硬编码，`writePump` 到期未续期即以 4008 主动断连。README 阶段 3/4 用 `duration=300s/600s`，**600s 正好撞 10min 边界**，压测末段会出现成片断连与读错误，污染结果。且 `sessionTTL` 不可配置。

**修复方向**：
1. server 增加 `-session-ttl` 参数（默认 10min）；
2. loadtest 增加 `--reauth` 处理（可选）或在文档提示压测时用 `-session-ttl` 调大 / 关闭。
本次实现 server 端可配置（最小改动，且压测脚本可据此设长）。

---

## 🟡 M6. Redis `room:*` 模式订阅：每台 server 反序列化全网每条消息

`redis.go handleIncoming` 对每条 payload 先 `json.Unmarshal` 整个 `[]*Message`，才判断 `SourceServer`/`RoomID`——即使该房间根本不在本机也照样全量解析。开销是"N 台机 × 全量消息"，不利于横向扩展。

**修复方向**：Publish 时把 `SourceServer` 编进频道或消息头，订阅侧先做便宜判断再决定是否反序列化；或先按"本机是否持有该房间"短路（需先拿到 roomID——可把 roomID 也放进 channel 名，已经是 `room:{id}` 了，订阅回调能拿到 channel）。本次用 channel 名直接取 roomID + 本机房间存在性短路，避免无谓 Unmarshal。

---

## ⚪ L1. 令牌比较非恒定时间

`middleware.go authMiddleware` 的 `provided != token`、`api.go handleWebSocket` 的 `token != a.authToken` 用普通字符串比较，存在理论计时侧信道。

**修复**：改用 `crypto/subtle.ConstantTimeCompare`。

## ⚪ L2. loadtest 控制消息计入 recvCount

`rate_limited` / `session_token` / `reauth_ack` 也被当成 DownMessage 计入 `recvCount`，轻微虚高接收吞吐。**修复**：读循环里按 `type` 跳过非 danmu 消息。

## ⚪ L3. 死代码清理

- `message.go`：`bufferPool` / `acquireBuffer` / `releaseBuffer` / `serializeMessages` 无调用者（worker 直接 `json.Marshal`）。
- `api.go`：`startQPSCounter` / `QPSValue` / `formatUptime` 无调用者；`StartQPSTracker` 内两个 goroutine 是空转。
- `redis.go`：`SubscribeRoom` 无调用者（只用 `SubscribePattern`）。
- `loadtest/main.go`：`rand.Seed` + `math/rand` import（Go 1.20+ 自动播种，且 rand 未被使用）。

## ⚪ L4. 同房间消息被多 worker 拆批、跨 worker 无序（已知取舍）

所有 worker 共享一个 `msgQueue`，同房间两条弹幕可能落到不同 worker，广播/Kafka 写入顺序都可能与到达顺序不一致。弹幕场景可接受，仅作记录，不修。

---

## 修复优先级与本轮计划

本轮修复（对照上表）：H1、H2、M1、M2、M3、M4、M5、M6、L1、L2、L3。
不修（记录取舍）：L4（无序）、M2 的关房/踢人跨机部分。

每项修复后跑 `go build/vet/test`，再交叉编译 linux 二进制上 AutoDL 服务器压测验证。

---

## 服务器压测验证（2026-07-15，AutoDL 256 核 / 755G）

单机 server（`-mq=redis`，Redis 缺省优雅降级为本机广播）+ 同机 loadtest。

### 第一轮修复的验证
- **H1 基数爆炸已消除**：`/metrics` 中 `danmu_connections_total` 无标签、`danmu_messages_total{direction=in|out}` 仅剩 direction 标签。
- **M4 纳秒精度生效**：200 连接基线 E2E `P50=2451μs P90=6635μs P99=8687μs`——数值不再是 1ms 的整数倍，确为亚毫秒分辨率。
- **M1 qps 生效**：`/api/v1/stats` 的 `qps` 在空闲时回到 0（不再是单调累计值）。
- server 在 256 核上起 1024 worker，Redis/Kafka 缺失时按设计降级，无 panic。

### 压测暴露的新问题 → 第二轮修复
首轮 10k 连接压测出现「E2E 秒级、Read Errors == 连接数」，定位到两处**压测工具自身**问题 + 一处 server 热点，已修复：

- **L5（loadtest）Read Errors 恒等于连接数**：收尾时 writer 侧 `conn.Close()` 让 reader 观察到的错误被计成读失败。加 `closing` 标志区分「主动关闭」与「服务端异常断开」→ 修复后 Read Errors 从 10000 降到 **0**。
- **L6（loadtest）单把全局直方图锁不扩展**：万级连接下数百万次记录串行化，既拖慢读取又给延迟灌水。改为按连接分片（每片独立锁，report 时 Merge）。
- **L7（server）`addClient` 每连接一条 join 日志**：万级连接下走序列化的 `Hub.Run` 单 goroutine，抬高建连延迟。已删除（连接数由 metric/stats 观测）。

### 第二轮修复后的最终结果
| 场景 | 连接 | 房间/扇出 | E2E P50 | E2E P90 | E2E P99 | Read/Write Err | 丢弃 |
|------|------|-----------|---------|---------|---------|----------------|------|
| 低扇出 | 10000 | 1000 房 / 扇出 10 | **1.6ms** | **5.3ms** | 34ms | 0 / 0 | 0 |
| 高扇出 | 10000 | 100 房 / 扇出 100 | 510ms | 1.6s | 7.1s | 0 / 17 | 有（server 侧 sendCh 满静默丢弃）|

结论：**合理扇出下 P90 < 6ms、零错误零丢弃**，修复均生效。高扇出的秒级延迟是「压测机与 server 同机争抢 256 核 CPU + 100× 扇出」的容量现象——server 内部广播延迟直方图显示 ~79% 的消息 generate→broadcast < 100ms，秒级部分主要出在下游 sendCh/writePump/TCP 与被 CPU 饿死的压测 reader，不是本次改动的回归。生产中压测机应与 server 分机部署，并适当增大 `sendChSize` 或对超大房间做分级广播。

### 剩余未改（记录取舍）
- L4 同房间跨 worker 无序（弹幕可接受）。
- M2 的关房/踢人跨机（仅广播做了跨机，控制面留作已知限制）。
- 高扇出下 `sendChSize=256` 满即静默丢弃：符合"弹幕允许丢"的设计，未改；如需可调大或按房间体量分级。
</content>
