# 百万 QPS 直播弹幕系统 · 分布式版（goim 式）

把单体 `../monolith/server` 按 [Bilibili goim](https://github.com/Terry-Mao/goim) 的思路拆成三层无状态服务，
跨机广播从 Redis Pub/Sub 换成 **Kafka → Job → Comet 定向推送**。
设计动机、分层职责与取舍见 [DESIGN.md](./DESIGN.md)；与单体版的对照见 [`../PROJECT.md`](../PROJECT.md)。

module：`github.com/YansIlinta/danmu-distributed`

## 组成

| 目录 | 角色 |
|------|------|
| `comet/` | WebSocket 接入层，只管长连接；对外 gRPC `PushRoom`，启动时向 registry 注册 |
| `logic/` | 业务层，gRPC `OnMessage`：敏感词过滤 + 生成 `msg_id` + 投 Kafka |
| `job/` | 消费 Kafka → 从 registry 发现 comet → 定向 `PushRoom`（替代 Redis Pub/Sub） |
| `core/` | 三层共享的连接层（Hub 分片锁、Client、限流、过滤、鉴权、指标） |
| `minirpc/` | 自研 RPC 框架（registry 服务发现 + 一致性哈希 lb + 熔断），以 `replace` 内嵌 |
| `cmd/registry` | minirpc registry 服务端，comet/logic 的服务注册与发现 |
| `cmd/chaintest` | 全链路集成测试：registry 发现 → PushRoom → WS 下行 → 上行 |
| `pb/` | gRPC 契约（`danmu.proto` 及生成代码） |
| `web/` | 弹幕联调页；comet 用 `http.Dir("web")` 提供，**必须在本目录下启动 comet 才能找到** |

## 起链路

```bash
# 方式一：本地进程（需本机 Kafka，默认 localhost:9092）
cd danmu/distributed
bash scripts/run-goim-local.sh          # registry + logic + job + 2×comet
bash scripts/run-goim-local.sh stop

# 方式二：docker compose 全链路（含 Kafka / ClickHouse / nginx）
cd danmu/distributed
docker compose -f docker-compose.goim.yml up -d --build
```

WS 入口：`ws://localhost:8080`、`ws://localhost:8081`；nginx 汇聚在 `:8088`。

## 压测

`loadtest` 是架构无关的公共工具，源码在 `../monolith/loadtest`，不在本 module 内重复一份：

```bash
(cd ../monolith && go build -o ../distributed/bin/loadtest ./loadtest/)
./bin/loadtest --server=ws://localhost:8080,ws://localhost:8081 \
               --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
```

同理，Kafka→ClickHouse 落库的 `consumer` 也在 `../monolith`，
`docker-compose.goim.yml` 里的 `consumer` 服务直接以 `../monolith` 为 build context 构建。

## 重新生成 pb

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       pb/danmu.proto
```
