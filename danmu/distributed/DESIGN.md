# goim 式微服务化设计（Comet / Logic / Job）

> 目标：把原单体弹幕 server（`../monolith/server/`）按 [Bilibili goim](https://github.com/Terry-Mao/goim) 的思路拆成三层无状态服务，
> 服务间通信用 **gRPC**（契约见 `pb/danmu.proto`），服务发现用 **etcd**（租约 + keepalive + 前缀 Get/Watch，
> 取代原自研的 `registry/`：HTTP + 内存 + TTL 租约），
> comet 选 logic 实例用 **etcd 官方 naming/resolver + gRPC round_robin**（取代原自研的 `lb/`：一致性哈希环 + 虚拟节点），
> **跨机广播从 Redis Pub/Sub 换成 goim 的 `Kafka → Job → Comet 定向推送`**。
> 原单体 `../monolith/server/` 保留作为「单体 vs 微服务」对比基线，不动。

## 组件映射

| goim | 本项目 | 职责 |
|------|--------|------|
| Comet | `comet/` | 维持 WebSocket 长连接、房间-连接分片管理、会话续期、限流；暴露 `Comet.PushRoom` 供 Job 回推；上行弹幕经 gRPC 转发给 Logic；启动时向 etcd 注册 |
| Logic | `logic/` | 无状态逻辑层：敏感词过滤、生成全局 `msg_id`、把消息 produce 到 Kafka；向 etcd 注册 |
| Job | `job/` | 消费 Kafka，watch etcd 发现所有 Comet，按房间把消息 `Comet.PushRoom` 定向推给每个 Comet（Comet 本地无该房间即丢弃） |
| discovery(eureka) | etcd | 注册中心：key `danmu/services/<service>/<addr>`，value 为 grpc naming.Update 兼容 JSON，10s 租约 + keepalive；崩溃后租约过期自动清理（无注销接口） |
| （etcd resolver + round_robin） | `etcdreg/` + gRPC | comet→logic 用 etcd 官方 naming/resolver watch 地址集 + round_robin 负载均衡；`etcdreg/` 是各服务共用的注册/发现封装 |
| Kafka | Kafka(KRaft) | 削峰 + 扇出总线 |
| （落库） | `../monolith/consumer/` | 复用原有：消费 Kafka 落库 ClickHouse |

## 数据流

```
                         ┌─────────┐  register(lease+keepalive)  ┌─────────┐
                         │  etcd   │◄───────────────────────────│ comet-i │
                         └────▲────┘                             └─────────┘
                    watch comets │
 client ──WS──► comet ──RPC:Logic.OnMessage──► logic ──produce──► Kafka(danmu-broadcast)
   ▲                                                                       │
   │ RPC:Comet.PushRoom(roomID,payload)                                    │ consume
   └───────────────────────────── job ◄──────────────────────────────────┘
                          (对每个 comet 定向 PushRoom；无该房间则丢弃)
```

一条弹幕完整链路：`client → comet(收) → Logic.OnMessage(过滤/msg_id) → Kafka → job(消费) → 对所有 comet Comet.PushRoom → comet 本机 BroadcastToRoom → 房间内所有 client`。
- 发送者自己的弹幕也走这条回路（不在 comet 本地 echo），由前端 `msg_id` 去重保证不重复；这与 goim 的房间消息「Job 推给所有 Comet」一致。
- 落库：`../monolith/consumer/` 独立消费同一个 Kafka topic 写 ClickHouse（与广播解耦）。
- comet→logic 的上行按 **round_robin** 分发：logic 完全无状态，同房间粘性不是正确性要求（原自研一致性哈希已删除）；
  实例增减由 etcd watch 实时生效，扩容零迁移成本。

## 为什么这样拆（对照 goim 与前沿）

- **跨机扇出 Redis Pub/Sub → Kafka+Job**：Redis Cluster Pub/Sub 每条 publish 广播到每个节点，吞吐随集群规模负向扩展；goim 用 Kafka 削峰 + Job 定向 gRPC 推送，Comet 只收自己该收的。本项目同样用 gRPC。
- **自研 registry/lb → etcd + gRPC 标准负载均衡**：原自研注册中心（HTTP+内存+TTL）与一致性哈希环在本项目只是 etcd/生产组件的替身；换成 etcd 后，注册=租约 keepalive（崩溃自动清理）、发现=前缀 watch（即时性优于轮询），comet→logic 直接用 etcd 官方 naming/resolver + round_robin，自研路由代码清零。
- **无状态可独立扩缩**：Comet 按连接数扩、Logic 按 CPU 扩、Job 按 Kafka 分区扩，互不耦合。
- **服务发现**：Comet/Logic/Job 把地址注册进 etcd 前缀 `danmu/services/`；Job watch `danmu/services/comet/` 拿全量 Comet 地址做扇出；comet 通过 `etcd:///danmu/services/logic` resolver 发现 Logic 实例集合。

## 通信契约（gRPC，定义见 `pb/danmu.proto`）

**LogicService**（Comet → Logic）
```
OnMessage(req{room_id,uid,content,client_ts,client_ts_nano,source_comet,offset_ms}) → resp{msg_id,filtered}
```
Logic：敏感词过滤 → 分配 `msg_id` → produce 到 Kafka（key=RoomID）→ 返回 msg_id。

**CometService**（Job → Comet）
```
PushRoom(req{room_id, payload bytes}) → resp{delivered int32}
```
Comet：`BroadcastToRoom(RoomID, Payload)`，返回本机投递数（无该房间则 0）。
Payload 已是发给客户端的最终 JSON（`[]Message` 数组），Comet 不再序列化。

## 部署

```
etcd(:2379) ── logic(:7400 rpc) ── job ── comet-1(:8080 ws, :7500 rpc) / comet-2(:8081 ws, :7501 rpc)
                                              └ Kafka(:9092) / ClickHouse(:9000, 落库 consumer)
```
- 本地/无中间件：comet 不传 `-etcd` 即 standalone，退化为本机过滤+广播，便于冒烟。
- K8s：`k8s/` 提供整套清单（`kubectl apply -k k8s/`）——etcd 3 节点 StatefulSet、KRaft Kafka、
  ClickHouse、comet/logic（Deployment + HPA）、job/consumer/ops、nginx。注册地址用 pod IP
  （downward API `$(POD_IP)`），服务发现语义与裸机一致；细节见 `k8s/README.md`。
- 生产：etcd 3 节点 + TLS；Comet 连接层可换 epoll 事件循环上百万单机（见 REVIEW.md 演进）。

## 复用与改动边界

- 新增：`core/`（连接/房间/消息/限流/敏感词/令牌/指标共享包）、`comet/`、`logic/`、`job/`、`etcdreg/`（etcd 注册/发现约定封装）。
- 删除：自研 `registry/`、`lb/`、`cmd/registry/`（注册中心不再自建，etcd 是外部标准组件）。
- `core` 相对原单体应用了 REVIEW round-2 的 **D1**：去掉 `register/unregister` 单 goroutine 串行化，直接调用分片安全的 `AddClient/RemoveClient`；readPump 通过注入的 `Uplink` 回调把上行交给 comet（而非硬编码 msgQueue）。
- 不动：`../monolith/server/`（单体基线）、`../monolith/consumer/`、`../monolith/loadtest/`、`web/`。loadtest/web 对 comet 的 WS 协议完全兼容，可直接压测 comet。

## 运行

**单体基线（对比用，无需中间件）**：见 `README.md`。

**goim 单机冒烟（standalone comet，无 etcd/logic/job/kafka）**：
```bash
go build -o bin/comet ./comet/
DANMU_AUTH_TOKEN=danmu-secret-token bin/comet -ws-addr=:8080   # 不传 -etcd = standalone 本机过滤+广播
./bin/loadtest --server=ws://localhost:8080 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
```

**goim 全链路（需 Kafka + etcd）**：
```bash
# 需先有 Kafka（localhost:9092）与 etcd（localhost:2379）；一键起 etcd+logic+job+2×comet：
bash scripts/run-goim-local.sh
# 压测两台 comet：
./bin/loadtest --server=ws://localhost:8080,ws://localhost:8081 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
bash scripts/run-goim-local.sh stop
```

**容器全链路（含 Kafka/ClickHouse/etcd/nginx）**：
```bash
docker compose -f docker-compose.goim.yml up -d --build
```

## 验证状态（2026-08-16，etcd 替换后复验）

- ✅ 全部服务 `go build`/`go vet`/`gofmt` 通过。
- ✅ `etcdreg` 单测（进程内 embed etcd）：注册/发现/排序、ctx 取消主动 Revoke、Watch 感知增删、
  注册 value 与官方 `naming/endpoints` 读取兼容。
- ✅ `comet/logicpool_test.go`：embed etcd + 两个 mock logic 实例，resolver 发现 + round_robin 真实分摊 20 个并发上行（两实例均命中、总数守恒）。
- ✅ **链路集成测试**（`cmd/chaintest`，本会话用 etcd v3.5.21 真机二进制 + logic + comet 复验）：
  - etcd 服务发现（comet 注册可被发现）；
  - `Logic.OnMessage` gRPC 可达；
  - **`Job→Comet.PushRoom→WS 客户端`收到广播**（`delivered=1`）；
  - 上行 `readPump→uplink`（comet `messages_in` 递增）。
- ✅ **job 的 etcd watch 实测**：第 2 台 comet 启动后，job `comets[]` 由 1 变 2（无需重启、无需轮询）。
- ✅ **ops 走 etcd 实测**：2 台 comet 时 services/topology 正确去重（顺带修复原 `probeServices` 多实例组件重复分组的既有 bug，含回归测试）、etcd 节点健康、overview 状态正确。
- ✅ **standalone comet 复验**（无 etcd）：loadtest 200 连接零错误，E2E P50 0.3ms / P99 0.6ms。
- ⏳ **未实测**：`logic→Kafka→job 消费` 这一段（标准 kafka-go，本会话无 Kafka broker）。用上面的全链路命令在有 Kafka 时可跑通。
