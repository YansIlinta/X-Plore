# goim 式微服务化设计（Comet / Logic / Job）

> 目标：把原单体弹幕 server（`../monolith/server/`）按 [Bilibili goim](https://github.com/Terry-Mao/goim) 的思路拆成三层无状态服务，
> 用本仓库自研的 `minirpc`（带 registry 服务发现 + 一致性哈希 lb + 熔断）做服务间通信，
> **跨机广播从 Redis Pub/Sub 换成 goim 的 `Kafka → Job → Comet 定向推送`**。
> 原单体 `../monolith/server/` 保留作为「单体 vs 微服务」对比基线，不动。

## 组件映射

| goim | 本项目 | 职责 |
|------|--------|------|
| Comet | `comet/` | 维持 WebSocket 长连接、房间-连接分片管理、会话续期、限流；暴露 `Comet.PushRoom` 供 Job 回推；上行弹幕经 RPC 转发给 Logic；启动时注册到 registry |
| Logic | `logic/` | 无状态逻辑层：敏感词过滤、生成全局 `msg_id`、把消息 produce 到 Kafka |
| Job | `job/` | 消费 Kafka，从 registry 发现所有 Comet，按房间把消息 `Comet.PushRoom` 定向推给每个 Comet（Comet 本地无该房间即丢弃） |
| discovery(eureka) | `minirpc/registry` | HTTP + 内存 map + TTL 租约的注册中心 |
| Kafka | Kafka(KRaft) | 削峰 + 扇出总线 |
| （落库） | `../monolith/consumer/` | 复用原有：消费 Kafka 落库 ClickHouse |

## 数据流

```
                         ┌────────────┐  register/heartbeat  ┌─────────┐
                         │  registry  │◄─────────────────────│ comet-i │
                         └─────▲──────┘                       └─────────┘
                    discover comets │
 client ──WS──► comet ──RPC:Logic.OnMessage──► logic ──produce──► Kafka(danmu-broadcast)
   ▲                                                                       │
   │ RPC:Comet.PushRoom(roomID,payload)                                    │ consume
   └───────────────────────────── job ◄──────────────────────────────────┘
                          (对每个 comet 定向 PushRoom；无该房间则丢弃)
```

一条弹幕完整链路：`client → comet(收) → Logic.OnMessage(过滤/msg_id) → Kafka → job(消费) → 对所有 comet Comet.PushRoom → comet 本机 BroadcastToRoom → 房间内所有 client`。
- 发送者自己的弹幕也走这条回路（不在 comet 本地 echo），由前端 `msg_id` 去重保证不重复；这与 goim 的房间消息「Job 推给所有 Comet」一致。
- 落库：`../monolith/consumer/` 独立消费同一个 Kafka topic 写 ClickHouse（与广播解耦）。

## 为什么这样拆（对照 goim 与前沿）

- **跨机扇出 Redis Pub/Sub → Kafka+Job**：Redis Cluster Pub/Sub 每条 publish 广播到每个节点，吞吐随集群规模负向扩展；goim 用 Kafka 削峰 + Job 定向 gRPC 推送，Comet 只收自己该收的。本项目用 minirpc RPC 替代 gRPC。
- **无状态可独立扩缩**：Comet 按连接数扩、Logic 按 CPU 扩、Job 按 Kafka 分区扩，互不耦合。
- **服务发现**：Comet 注册 `comet` 服务；Job 轮询 registry 的 `/services?service=comet` 拿全量 Comet 地址做扇出；Logic 用 `comet` 无关，Comet 侧用 `Discovery.GetKeyed(roomID)` 把同房间上行路由到固定 Logic 实例（一致性哈希，便于 Logic 做每房间限流/聚合）。

## 通信契约（minirpc，方法签名 `func(*Args,*Reply) error`）

**Logic 服务**（Comet → Logic）
```
Logic.OnMessage(args{RoomID,UID,Content,ClientTS,ClientTSNano,SourceComet}, reply{MsgID,Filtered})
```
Logic：敏感词过滤 → 分配 `msg_id` → produce 到 Kafka（key=RoomID）→ 返回 msg_id。

**Comet 服务**（Job → Comet）
```
Comet.PushRoom(args{RoomID, Payload []byte}, reply{Delivered int})
```
Comet：`BroadcastToRoom(RoomID, Payload)`，返回本机投递数（无该房间则 0）。
Payload 已是发给客户端的最终 JSON（`[]Message` 数组），Comet 不再序列化。

## 部署

```
registry(:7350) ── logic(:7400 rpc) ── job ── comet-1(:8080 ws, :7500 rpc) / comet-2(:8081 ws, :7501 rpc)
                                              └ Kafka(:9092) / ClickHouse(:9000, 落库 consumer)
```
- 本地/无中间件：comet 可脱离 logic/job 单独起（`-standalone` 退化为本机广播）便于冒烟。
- 生产：registry 换 etcd/consul；minirpc 换 gRPC；Comet 连接层可换 epoll 事件循环上百万单机（见 REVIEW.md 演进）。

## 复用与改动边界

- 新增：`core/`（连接/房间/消息/限流/敏感词/令牌/指标共享包）、`comet/`、`logic/`、`job/`、`cmd/registry/`。
- `core` 相对原单体应用了 REVIEW round-2 的 **D1**：去掉 `register/unregister` 单 goroutine 串行化，直接调用分片安全的 `AddClient/RemoveClient`；readPump 通过注入的 `Uplink` 回调把上行交给 comet（而非硬编码 msgQueue）。
- 不动：`../monolith/server/`（单体基线）、`../monolith/consumer/`、`../monolith/loadtest/`、`web/`。loadtest/web 对 comet 的 WS 协议完全兼容，可直接压测 comet。

## 运行

**单体基线（对比用，无需中间件）**：见 `README.md`。

**goim 单机冒烟（standalone comet，无 logic/job/kafka）**：
```bash
go build -o bin/comet ./comet/
DANMU_AUTH_TOKEN=danmu-secret-token bin/comet -ws-addr=:8080   # standalone 本机过滤+广播
./bin/loadtest --server=ws://localhost:8080 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
```

**goim 全链路（需 Kafka）**：
```bash
# 需先有一个 Kafka（localhost:9092）；一键起 registry+logic+job+2×comet：
bash scripts/run-goim-local.sh
# 压测两台 comet：
./bin/loadtest --server=ws://localhost:8080,ws://localhost:8081 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
bash scripts/run-goim-local.sh stop
```

**容器全链路（含 Kafka/ClickHouse/nginx）**：
```bash
docker compose -f docker-compose.goim.yml up -d --build
```

## 验证状态（2026-07-16）

- ✅ 全部服务 `go build`/`go vet`/`gofmt` 通过；`core` 单测（AC 过滤、令牌签发校验、令牌桶）通过。
- ✅ **standalone comet + loadtest**（本地）：200 连接零错误，E2E P50 0.6ms / P90 1.1ms / P99 1.7ms——core 重构 + D1（去单点注册）+ uplink 队列验证通过。
- ✅ **goim 链路集成测试**（`cmd/chaintest`，本地起 registry+logic+comet）：
  - registry 服务发现（comet 注册可被发现）；
  - `Logic.OnMessage` gRPC 可达；
  - **`Job→Comet.PushRoom→WS 客户端`收到广播**（`delivered=1`，最关键的新扇出路径打通）；
  - 上行 `readPump→uplink`（comet `messages_in` 递增）。
- ⏳ **未在本会话实测**：`logic→Kafka→job 消费` 这一段（标准 kafka-go，本会话无 Kafka broker、且 AutoDL 测试机 SSH 中断）。用上面的全链路命令在有 Kafka 时可跑通。
