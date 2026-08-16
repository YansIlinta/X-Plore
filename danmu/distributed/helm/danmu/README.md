# danmu-distributed Helm Chart

`k8s/` 目录 kustomize 基线的 Helm 等价物，资源模型、参数、验证方法与裸机/kustomize 版一致
（详见 `k8s/README.md` 的「K8s 化要点」）。

## 安装

```bash
cd danmu/distributed
# 先构建镜像（同 k8s/README.md）：
docker build -f Dockerfile.goim                  -t danmu-distributed:latest .
docker build -f ../monolith/Dockerfile.consumer ../monolith -t danmu-consumer:latest .

helm install danmu ./helm/danmu -n danmu --create-namespace

# 生产覆盖敏感值：
helm install danmu ./helm/danmu -n danmu --create-namespace \
  --set auth.token=<强随机> --set auth.jwtSecret=<强随机>
```

## 常用操作

```bash
helm template danmu ./helm/danmu -n danmu > /tmp/rendered.yaml   # 离线预览渲染结果
helm upgrade danmu ./helm/danmu -n danmu --set comet.replicas=4  # 改副本数
helm uninstall danmu -n danmu
```

## 与 kustomize 基线的差异

- chart 内资源**不带 namespace**，统一由 `helm -n` 控制（kustomize 基线硬编码 `danmu`）。
- 参数化面：镜像 tag、comet/logic 副本数、HPA、auth、ingress 开关与域名。
- etcd 副本数可参数化（initial-cluster 由 `replicas` 推导），但**首装后改副本数需先
  `etcdctl member add/remove`**，不能直接 scale。
- NetworkPolicy 默认开启（`networkPolicy.enabled=false` 关闭）；Ingress 默认关闭。
- 未内置 etcd TLS（对应 `k8s/overlays/etcd-tls`）：需要时在 values 里加 env/挂载，
  或直接用 kustomize overlay。
