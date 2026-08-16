# etcd 客户端面 TLS 化（可选 overlay）

在明文基线（`k8s/`）之上，把 etcd 客户端端口换成 https，四个业务服务（comet/logic/job/ops）
的 etcd 客户端经 `env DANMU_ETCD_*` 挂证书。业务代码无需改 flag：`etcdreg.NewClient` 在
`DANMU_ETCD_CA` 非空时自动启用 TLS（同一镜像既能跑明文也能跑 TLS）。

## 步骤

```bash
cd danmu/distributed

# 1) 生成证书（自签 CA + 3 个 etcd 服务端证书 + 1 个客户端证书）→ k8s/tls/certs/
bash k8s/tls/gen-certs.sh

# 2) 把证书放进 Secret（key 名要与 pod/挂载约定一致，见 gen-certs.sh 末尾的提示）
kubectl -n danmu create secret generic danmu-etcd-tls \
  --from-file=ca.crt=k8s/tls/certs/ca.crt \
  --from-file=server-0.crt=k8s/tls/certs/server-0.crt --from-file=server-0.key=k8s/tls/certs/server-0.key \
  --from-file=server-1.crt=k8s/tls/certs/server-1.crt --from-file=server-1.key=k8s/tls/certs/server-1.key \
  --from-file=server-2.crt=k8s/tls/certs/server-2.crt --from-file=server-2.key=k8s/tls/certs/server-2.key \
  --from-file=client.crt=k8s/tls/certs/client.crt --from-file=client.key=k8s/tls/certs/client.key

# 3) 用 TLS overlay 部署（复用 ../.. 明文基线的一切，仅覆盖需要 https 的部分）
kubectl apply -k k8s/overlays/etcd-tls

# 验证：etcd 客户端口已走 https，且各服务仍能发现/注册
kubectl -n danmu get pods -o wide
kubectl -n danmu logs statefulset/etcd   # 看到 "serving client requests on https://..."
kubectl -n danmu port-forward svc/ops 7900:7900   # Ops Console 拓扑正常即服务发现 OK
```

## 说明与边界

- **peer 面仍是明文**：本 overlay 只做 etcd↔业务服务的客户端认证与加密；etcd 三节点间的
  peer 流量在集群内网，未加密。要全加密，按同样套路给 `PeerTLSInfo`（peer 证书 + `--peer-cert-file`
  等）即可。
- **证书轮换**：证书有效期 3650 天；到期前重新生成并 `kubectl create secret ... -o yaml | kubectl apply`，
  再 `kubectl -n danmu rollout restart statefulset/etcd deployment/comet deployment/logic deployment/job deployment/ops`。
- **服务端未开启 client-cert-auth**：`client.crt/key` 现在只是转发兼容（未来开双向认证时无需再发证书）。
- 生产建议：换 cert-manager 签发或接企业 CA，不落地自签私钥。
