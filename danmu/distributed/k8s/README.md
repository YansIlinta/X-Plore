# danmu-distributed K8s 部署基线

与 `docker-compose.goim.yml` 等价的一整套 Kubernetes 清单，命名空间 `danmu`：

| 路径 | 内容 |
|------|------|
| `kustomization.yaml` | 顶层入口（`resources: [base]`），`kubectl apply -k k8s/` 一次部署 |
| `base/00-namespace.yaml` | 命名空间 `danmu` |
| `base/10-config.yaml` | ConfigMap `danmu-env`：非敏感项（`WS_ALLOWED_ORIGINS`） |
| `base/10-secret.yaml` | Secret `danmu-secret`：`DANMU_AUTH_TOKEN` / `JWT_SECRET`（基线为开发默认值，生产覆盖见下） |
| `base/20-etcd.yaml` | etcd **3 节点** StatefulSet + headless `etcd`（peer/per-pod 直连）+ `etcd-client`（应用入口）+ PDB（minAvailable 2，保多数派） |
| `base/21-kafka.yaml` | Kafka KRaft 单节点 StatefulSet（与 compose 同镜像同配置，10 分区 auto-create） |
| `base/22-clickhouse.yaml` | ClickHouse 单节点 StatefulSet（落库目标） |
| `base/30-comet.yaml` | comet Deployment（2 副本）+ Service（WS 入口，ClientIP 粘性）+ HPA（CPU 70%，2–20）+ PDB（minAvailable 1） |
| `base/31-logic.yaml` | logic Deployment（2 副本）+ HPA（CPU 70%，2–20）+ PDB（minAvailable 1），无 Service（etcd 发现直连 pod IP） |
| `base/32-job.yaml` | job Deployment（**固定 1 副本**：同步提交 at-most-once，扩副本无益） |
| `base/33-consumer.yaml` | 落库 consumer Deployment（镜像来自 `../monolith`） |
| `base/34-ops.yaml` | ops Deployment + Service（`/api/*` 观测面 + 内嵌 UI） |
| `base/35-nginx.yaml` | nginx Deployment + Service（WS/HTTP 统一入口，对应 compose 的 `:8088`） |
| `base/36-ingress.yaml` | **可选** Ingress（需集群已有 Ingress Controller），默认不随 kustomize 部署 |
| `base/40-networkpolicy.yaml` | 入向白名单 ×8（含 etcd 发现直连的 pod 间流量），见下「网络策略」 |
| `overlays/etcd-tls/` | **可选** overlay：etcd 客户端面 https + 四服务 TLS 客户端（配 `tls/gen-certs.sh`） |
| `tls/` | 证书生成脚本与说明（`certs/` 已 gitignore） |
| `../helm/danmu/` | 同一部署的 **Helm chart**（values 参数化，见 `../helm/danmu/README.md`） |

## K8s 化要点（与裸机/compose 的差异）

- **注册地址 = pod IP**：comet/logic/job 的 `-advertise(-http)` 用 downward API 的
  `$(POD_IP)`（`base/30/31/32-*.yaml`）。pod 重启后 IP 变化，etcd 旧 key 靠 10s 租约过期自动
  清理，无需任何人工干预。服务发现语义与裸机完全一致：job watch etcd 拿 comet 列表、
  comet 经 etcd resolver + round_robin 找 logic，因此 **comet/logic/job 之间没有 Service 互联**。
- **etcd 3 节点**（对应 DESIGN.md「生产：etcd 换 3 节点集群」）：StatefulSet 固定
  `initial-cluster`，客户端统一走 `etcd-client:2379`，etcd 内部经 per-pod FQDN
  （`etcd-0.etcd`）重定向。生产建议再叠 TLS 与独立存储类。
- **WS 粘性**：裸机靠 nginx `hash consistent` 分到固定 comet；K8s 里 nginx upstream 只有一个
  ClusterIP，粘性由 comet Service 的 `sessionAffinity: ClientIP`（1h）承担。若集群有
  Ingress Controller，也可用其 cookie affinity 替换本层 nginx。
- **HPA**：comet 按连接数扩（本基线用 CPU 近似）、logic 按 CPU 扩。job/consumer/ops 固定 1 副本。

## 部署

```bash
# 0) 构建镜像（需推到集群可拉取的 registry；单机 kind/minikube 可用本地 docker 镜像）
cd danmu/distributed
docker build -f Dockerfile.goim          -t danmu-distributed:latest .
docker build -f ../monolith/Dockerfile.consumer ../monolith -t danmu-consumer:latest .

# 0.5) 生产环境覆盖基线 Secret（本基线是开发默认值）
kubectl -n danmu create secret generic danmu-secret \
  --from-literal=DANMU_AUTH_TOKEN=<强随机> --from-literal=JWT_SECRET=<强随机> \
  --dry-run=client -o yaml | kubectl apply -f -

# 1) 部署全链路
kubectl apply -k k8s/

# 1.5) 集群有 Ingress Controller 时再补外部入口（域名按需改）
kubectl apply -f k8s/base/36-ingress.yaml

# 2) 看状态（etcd/kafka/clickhouse 起得慢，属正常）
kubectl -n danmu get pods -o wide
kubectl -n danmu logs deployment/comet --tail=20
```

## 验证

```bash
# Ops Console：先 port-forward 再开浏览器（http://localhost:7900）
kubectl -n danmu port-forward svc/ops 7900:7900

# 链路自检（chaintest 需打到 comet 的 WS/RPC 与 logic RPC；三者都注册在 etcd 里，
# chaintest 第一步的 etcd 发现用 pod 内网络，集群外跑请分别 port-forward）：
kubectl -n danmu port-forward svc/comet 8080:8080
kubectl -n danmu port-forward svc/etcd-client 2379:2379
# WS 冒烟：
curl -s localhost:8080/health
```

压测（loadtest 是架构无关公共工具，构建一次即可）：

```bash
(cd ../monolith && go build -o ../distributed/bin/loadtest ./loadtest/)
kubectl -n danmu port-forward svc/comet 8080:8080 &
./bin/loadtest --server=ws://localhost:8080 --conns=2000 --rooms=50 --rate=2 \
               --duration=30s --token=danmu-secret-token
```

> port-forward 只打到单个 pod，压测「单 comet 容量」；压「多 comet 扇出」需经 nginx
> 入口（`kubectl -n danmu port-forward svc/nginx 8080:80`），但单条 port-forward 仍是
> 单 nginx pod 转发到 ClusterIP，粘性行为与真实多副本略有差异——多副本行为建议用
> NodePort/LoadBalancer 或 Ingress 暴露后测。

## 扩缩容

```bash
kubectl -n danmu scale deployment comet --replicas=4   # 手动；HPA 会自动参与
kubectl -n danmu get hpa                                # CPU 利用率与目标副本数
kubectl -n danmu logs deployment/job | grep 'comet added'   # job 眼里的 comet 增删
```

comet/logic 扩缩容后：job 的 comet 列表由 etcd watch 秒级生效；comet 到 logic 的
round_robin 地址集由 resolver 自动更新——**没有任何一步需要重启或改配置**。

## 网络策略

`base/40-networkpolicy.yaml` 给九个服务面配了入向白名单（默认随 `kubectl apply -k k8s/` 部署）：
etcd/Kafka/ClickHouse 只对各自消费者开放；comet→logic :7400、job→comet :7500 这些
**etcd 发现后按 pod IP 直连的流量**已显式放行（区别于 compose「服务间全靠 Service」的思维）。
kubelet 探针与 port-forward 不经 NetworkPolicy，不会被误伤；关闭方式：

```bash
kubectl -n danmu delete networkpolicy --all   # 或从 kustomization 移除该文件
```

## etcd TLS（可选 overlay）

`overlays/etcd-tls/` 把 etcd 客户端面换成 https，四个业务服务经 `env DANMU_ETCD_*` 挂证书——
**业务代码无需改 flag**（`etcdreg.NewClient` 在 `DANMU_ETCD_CA` 非空时自动启用 TLS，同一镜像
既能跑明文也能跑 TLS，单测见 `etcdreg/client_test.go`）。启用步骤见 `tls/README.md`：

```bash
bash k8s/tls/gen-certs.sh                                # 生成自签证书
kubectl -n danmu create secret generic danmu-etcd-tls ... # 见脚本末尾提示
kubectl apply -k k8s/overlays/etcd-tls
```

## Helm

同一部署的 chart 化版本在 `../helm/danmu/`（`helm install danmu ./helm/danmu -n danmu --create-namespace`），
参数化面：镜像 tag、comet/logic 副本、HPA、auth、ingress 开关；与 kustomize 基线的差异见
`../helm/danmu/README.md`。

## 维护安全

```bash
# 驱逐节点/滚动维护时 PDB 生效（etcd 保 2 个、comet/logic 各保 1 个）
kubectl -n danmu drain <node> --ignore-daemonsets --delete-emptydir-data
kubectl -n danmu get pdb
```

## 已知限制（与裸机一致的存量问题）

- job **固定 1 副本**（见 `32-job.yaml` 注释）；扩展方式是加 Kafka 分区再扩副本。
- consumer 无 HTTP 观测面 → 无探针；活性靠 ops 里 `danmu-storage` 的 Kafka lag 推断。
- ops 镜像内无 loadtest 二进制 → Load Test 页显示不可用，压测走集群外。
- token/JWT 在 Secret 里但值是开发默认值；生产按上面 0.5 步覆盖，并给 etcd 上 TLS。
- 各 `/health` 是静态探活，不检查依赖（同裸机）。
