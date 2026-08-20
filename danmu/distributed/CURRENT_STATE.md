# CURRENT STATE — Repository Audit（源码级核实）

> 依据执行 Prompt 工作原则第 0 节，从源码核实（非 README 转述），仅记录事实。

## Verified — 已从源码证实

### 当前架构：真实数据路径
```text
Client --WS--> Comet --gRPC OnMessage--> Logic --Kafka--> Job --gRPC PushRoom-->(所有) Comet --BroadcastToRoom--> 房间内 Client
```
- `comet/main.go handleWebSocket`:鉴权(secret/JWT)→ `NewClient` → `hub.AddClient` → `ReadPump/WritePump`。上行弹幕经 `hub.Uplink` 回调 → `uplinkCh` → `OnMessage` gRPC(etcd 发现 logic + round_robin)。
- `logic/main.go OnMessage`:过滤 → 生成 `msg_id` → `Async:true` kafka writer 入队（`RequiredAcks=RequireOne`）→ 返回 `msg_id`。
- `job/main.go`:消费 Kafka(key=roomID) → 10ms flush 聚合 → `pool.pushRoom` **对 etcd 发现的所有 comet 并发 PushRoom**(2s 超时)。
- `comet.PushRoom`:调 `hub.BroadcastToRoom(req.RoomId, req.Payload)`；本机无该房间 → `delivered=0`。

### 连接模型
- `core/hub.go`:Hub 持 `256` 分片，每片 `map[roomID]map[uid]*Client` —— **room-centric，uid→单连接**。
- `AddClient`:同 uid 同 room **顶号**，`old.Close(4009, "session replaced by new connection")`；顶号不算新增(`connCount` 不变)。
- 无 `ConnectionID` / `DeviceID` / `Channel` 概念；`session replacement` 是底层数据结构限制而非显式策略。
- 生命周期:`ReadPump defer` → `hub.RemoveClient(c)` + `conn.Close()` + `cancel()`（幂等：Remove 只删「条目存在且 existing==c」）。
- slow consumer:`Client.TrySend` 非阻塞，`sendCh`(256) 满即丢并 `MetricDropped`——这是既有 backpressure 行为。

### Routing（Job 侧）
- `job.pushRoom` **向 etcd 发现的全部 comet 扇出**，靠 comet 本地 `delivered==0` 过滤 → **RPC/message ≈ Gateway count**。
- 无 room→comet / uid→comet 路由；曲线讨巧：PushRoom 带 metadata trace。**无 Redis route store。**
- etcd 现状：仅 service discovery（comet/logic/job-http 注册），无用户级 presence。

### 消息语义
- `msg_id`:logic 生成（`id-seq` 原子计数，进程启动时间播种）。**无 client_message_id 幂等键**（UpMessage 有 MsgID 字段但分布式侧不消费）。
- Kafka writer `Async:true`:OnMessage 返回=“交给 writer 入队”，**非 broker 确认**；写失败走 ErrorLogger 丢弃──“成功”后消息仍可能丢。
- 分布式侧 `Message.Seq` **恒 0**（代码注释明言“重连补发未实现前仅保留字段做 wire 兼容”）。**无 device ack / 无离线补发**。
- 单体侧已有:房间级 `roomSeqs` 单调序号、`RoomHist` 热历史补发(`ReplayFrom(afterSeq)`)、`MsgIDSet` 房间级幂等、`ackCh` 消息级 ack、`reconnect/reauth` 协议。

### 持久化
- Redis:仅**单体**用 Pub/Sub 跨机广播（RedisHub）。分布式路由/状态无 Redis。
- Kafka:logic 生产(削峰/扇出总线) → job 消费(10ms flush) → 落库 consumer 写 **ClickHouse**。
- ClickHouse 不承担事务性消息状态，仅 analytics。

### Evidence
- 已有严格引擎:`VERIFIED / PARTIAL / CODE VERIFIED / TARGET / UNKNOWN`，只认实验存储，不读文档、无 LLM（`ops/evidence.go`）。10k 连接 claim 默认 TARGET。
- 基线:两 module `go build / go vet / go test ./...` 全绿（distributed 含 embed etcd 测试 17.7s）。

## Partially implemented / 偏差
- `PushRoom` 带 `danmu:room:<id>` 兼容层概念，但底层仍是 `map[room]map[uid]`，无真实 Channel/Subscription 抽象。
- 多设备：**不存在**。分片结构强制 uid→单连接。
- 路由感知投递：Job 无目标感知，RPC 放大=gateway 数。

## Missing（对照执行 Prompt 的 v1 目标）
- ConnectionID / DeviceID / SessionIndex / SubscriptionIndex / multi-device 策略。
- Route Store（user/device/channel → gateway，TTL）；targeted push。
- client_message_id 幂等、conversation sequence、durable ACK、offline sync、client ACK。
- 分布式下 EPHEMERAL/RELIABLE 语义区分（目前全是 best-effort broadcast）。

## Architectural constraints（重构必须守住）
1. 不推倒现有系统：`Room Broadcast` 是既有可用能力，作为回归基线保留。
2. goim 分层(comet/logic/job)与 etcd 服务发现保持不变——Phase 3 才引入 Redis route。
3. `core` 是 comet/logic/job 共享的连接/消息包，改它必须以「API 兼容 + 属性递增」方式接入，避免大爆炸。
4. Job 扇出逻辑不动（Phase 3 再换 Route Store）；本项目只改连接平面。
5. 观测指标/Stats JSON 结构尽量兼容（api.go 增量加字段）。
6. 证据纪律：任何性能改动不得声称未验证数字。

## Main migration risks
- `core_test.go TestHubCounterConsistency` 断言“顶号不改计数”——multi-device 默认语义下必须改写为“新连接=新增计数”，需保留 CloseRoom/KickClient 计数一致性回归部分。
- 顶号被依赖处:`comet.handleWebSocket` 顶号由 Hub 内部驱动，重构后默认改 multi-device 不再顶号——需确认 goim/client 冒烟不受影响（loadtest 按 uid 顶号场景若依赖则需改为 SingleDevice policy）。
- Redis/Channel 抽象若提前引入会牵动 Kafka 链路，Phase 3 再评估。
