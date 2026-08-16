# 百万 QPS 直播弹幕系统 · 分布式版（goim 式）

把单体 `../monolith/server` 按 [Bilibili goim](https://github.com/Terry-Mao/goim) 的思路拆成三层无状态服务，
跨机广播从 Redis Pub/Sub 换成 **Kafka → Job → Comet 定向推送**。
设计动机、分层职责与取舍见 [DESIGN.md](./DESIGN.md)；与单体版的对照见 [`../PROJECT.md`](../PROJECT.md)。

module：`github.com/YansIlinta/danmu-distributed`

## 组成

| 目录 | 角色 |
|------|------|
| `comet/` | WebSocket 接入层，只管长连接；对外 gRPC `PushRoom`，启动时向 etcd 注册 |
| `logic/` | 业务层，gRPC `OnMessage`：敏感词过滤 + 生成 `msg_id` + 投 Kafka |
| `job/` | 消费 Kafka → 从 etcd 发现 comet → 定向 `PushRoom`（替代 Redis Pub/Sub） |
| `core/` | 三层共享的连接层（Hub 分片锁、Client、限流、过滤、鉴权、指标） |
| `etcdreg/` | 本项目在 etcd 上的注册/发现约定（key 规范 + 租约续期 + List/Watch），各服务共用 |
| `k8s/` | Kubernetes 部署基线（kustomize：base/ + overlays/etcd-tls），含 NetworkPolicy/PDB/HPA/Ingress，见 `k8s/README.md` |
| `helm/` | 同一部署的 Helm chart（values 参数化），见 `helm/danmu/README.md` |
| `ops/` + `cmd/ops` | Ops Console 后端：旁路观测聚合，不参与消息链路 |
| `etcd` | 服务发现与注册中心（标准组件，不再是自研 registry） |
| `cmd/chaintest` | 全链路集成测试：etcd 发现 → PushRoom → WS 下行 → 上行 |
| `pb/` | gRPC 契约（`danmu.proto` 及生成代码） |
| `web/` | 弹幕联调页；comet 用 `http.Dir("web")` 提供，**必须在本目录下启动 comet 才能找到** |

## 起链路

```bash
# 方式一：本地进程（需本机 Kafka，默认 localhost:9092）
cd danmu/distributed
bash scripts/run-goim-local.sh          # etcd + logic + job + 2×comet
bash scripts/run-goim-local.sh stop

# 方式二：docker compose 全链路（含 Kafka / ClickHouse / nginx）
cd danmu/distributed
docker compose -f docker-compose.goim.yml up -d --build

# 方式三：Kubernetes（etcd 3 节点 / Kafka / ClickHouse / comet·logic×2+HPA / job / consumer / ops / nginx）
cd danmu/distributed
docker build -f Dockerfile.goim                  -t danmu-distributed:latest .
docker build -f ../monolith/Dockerfile.consumer ../monolith -t danmu-consumer:latest .
kubectl apply -k k8s/          # 部署与验证细节见 k8s/README.md
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

## 观测端点（Ops Console 数据面）

各服务内置最小观测面（`/health` + `/api/v1/stats`，纯读、原子计数，不参与消息链路）：

| 服务 | HTTP 观测地址（默认） | 注册服务名 | 说明 |
|------|----------------------|-----------|------|
| comet | `-advertise-http` 或由 advertise 主机 + ws 端口推导 | `comet-http` | 另有 `/metrics`（Prom）、`/api/v1/rooms`（Bearer 鉴权） |
| logic | `-http-addr=:7410`（`-advertise-http` 推导注册地址） | `logic-http` | onmessage/filtered/kafka 错误计数 |
| job | `-http-addr=:7420`（`-advertise-http` 推导注册地址） | `job-http` | 消费/扇出/投递计数、当前 comet 列表 |
| etcd | `:2379` | — | key 前缀 `danmu/services/`；`etcdctl get --prefix danmu/services/` 查看 |

Ops Console 后端（`cmd/ops`，默认 `:7900`）经 etcd 的 `*-http` 服务发现各实例并聚合。裸机跑：

```bash
go run ./cmd/ops -etcd=localhost:2379 -kafka=localhost:9092
```

compose 已内置该服务，`up` 之后直接访问宿主 `:7900`：

```bash
docker compose -f docker-compose.goim.yml up -d --build
curl -s localhost:7900/api/overview
```

宿主还额外映射了 logic `:7410`、job `:7420`，可直接 `curl localhost:7410/api/v1/stats` 单独看某个服务。

> ⚠️ 容器内 **job 必须显式传 `-advertise-http=job:7420`**：job 没有 `-advertise`，缺省推导会得到 `localhost:7420`，注册进 etcd 后 ops 会拨到自己身上。comet/logic 有 `-advertise`，缺省推导本身正确，compose 里仍写全以防日后改 advertise 时静默失效。

comet 到 logic 的上行路由经 **etcd resolver + gRPC round_robin** 分发：logic 无状态，扩缩容不需要重映射任何路由表；实例增减由 etcd watch 实时生效（替换了原自研的一致性哈希按房间粘性路由）。

API：`/api/overview`、`/api/services`、`/api/topology`、`/api/events`、`/api/traces`、`/api/health`。`-mock` 显式启用假数据模式（响应带 `"mock":true`），默认只呈现真实数据，拿不到的一律 `null`。

## 消息 trace

回答「这条弹幕卡在哪一段」。五个环节各记一条 span，ops 按 msg_id 拼成链路：

```
comet.uplink → logic.produce → job.consume → job.push → comet.deliver
```

```bash
curl -s localhost:7900/api/traces | jq '.traces[0]'
curl -s -H "Authorization: Bearer $DANMU_AUTH_TOKEN" localhost:8080/api/v1/traces  # 单机原始 span
```

每条链路带 `complete` 与 `missing_hops`：缺哪段就说明消息停在那之前。

**采样**：`-trace-sample=N` 表示 1/N 采样（0=关闭，默认 100）。**comet 与 logic 的取值必须一致** —— 两者各自对 msg_id 做同一套确定性哈希来判定，值不同就会各采各的，链路永远拼不齐。job 不做判定，它读 logic 写在 Kafka header 里的结果。

传播方式刻意避开了协议改动：logic 把命中采样的 msg_id 写进 **Kafka header**，job 再经 **gRPC metadata** 传给 comet。`pb/danmu.proto` 不用动，落库 consumer 也不受影响（它忽略 header）。下游因此无需为了取 msg_id 而反序列化每条消息 —— 未采样消息在热路径上零额外成本。

**已知局限**：
- msg_id 由 logic 生成，所以链路只能从「logic 成功返回」起记。**上行 RPC 失败的消息不会留下任何 trace**（它压根没有 msg_id），表现为「查无此消息」而不是「链路缺后几段」。
- 各服务的 span 缓冲有界（`-trace-buffer`，默认 512），ops 侧保留最近 200 条链路。溢出量在 `/api/traces` 的 `sources` 里透传，别把残缺链路当完整结论。
- 跨节点耗时依赖各机器时钟同步，未做时钟偏差校正。
- `-mock` 模式不产生 trace。
