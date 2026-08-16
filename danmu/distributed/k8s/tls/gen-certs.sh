#!/usr/bin/env bash
# 生成 etcd 集群 TLS 证书（自签 CA，openssl）：
#   ca.crt / ca.key                  —— 服务端与客户端共同信任的 CA
#   server-{0,1,2}.crt / .key        —— 各 etcd pod 的服务端证书（SAN 覆盖 pod FQDN）
#   client.crt / client.key          —— 业务服务的客户端证书（服务端开启 client-cert-auth 时才被校验）
#
# 输出到本目录 certs/（已 gitignore）。与 k8s/overlays/etcd-tls 配套使用，步骤见 README.md。
# 生产建议换成 cert-manager 或企业 CA 签发。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs

CNF_COMMON='
distinguished_name = dn
[dn]
[v3]
subjectAltName = @alt
[alt]
DNS.1 = etcd
DNS.2 = etcd-client
DNS.3 = etcd-client.danmu.svc
DNS.4 = etcd-client.danmu.svc.cluster.local
IP.1 = 127.0.0.1
'

# 1) CA
if [[ ! -f certs/ca.crt ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=danmu-etcd-ca" -keyout certs/ca.key -out certs/ca.crt 2>/dev/null
fi

# 2) 每个 etcd pod 一张服务端证书；SAN 必须同时覆盖 per-pod FQDN 与统一入口 etcd-client
#    （客户端按 dial 的主机名校验证书，二者都会被用上）
for i in 0 1 2; do
  {
    echo "$CNF_COMMON"
    echo "DNS.5 = etcd-$i.etcd"
    echo "DNS.6 = etcd-$i.etcd.danmu.svc"
    echo "DNS.7 = etcd-$i.etcd.danmu.svc.cluster.local"
  } > certs/server-$i.cnf
  openssl req -newkey rsa:2048 -nodes -subj "/CN=etcd-$i" \
    -keyout certs/server-$i.key -out certs/server-$i.csr 2>/dev/null
  openssl x509 -req -days 3650 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
    -extensions v3 -extfile certs/server-$i.cnf \
    -in certs/server-$i.csr -out certs/server-$i.crt 2>/dev/null
  rm -f certs/server-$i.csr certs/server-$i.cnf
done

# 3) 业务服务客户端证书
openssl req -newkey rsa:2048 -nodes -subj "/CN=danmu-client" \
  -keyout certs/client.key -out certs/client.csr 2>/dev/null
openssl x509 -req -days 3650 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -in certs/client.csr -out certs/client.crt 2>/dev/null
rm -f certs/client.csr

echo "生成完毕（certs/）：ca.{crt,key}、server-{0,1,2}.{crt,key}、client.{crt,key}"
echo "下一步（见 k8s/tls/README.md）："
echo "  kubectl -n danmu create secret generic danmu-etcd-tls \\"
echo "    --from-file=ca.crt=certs/ca.crt \\"
echo "    --from-file=server-0.crt=certs/server-0.crt --from-file=server-0.key=certs/server-0.key \\"
echo "    --from-file=server-1.crt=certs/server-1.crt --from-file=server-1.key=certs/server-1.key \\"
echo "    --from-file=server-2.crt=certs/server-2.crt --from-file=server-2.key=certs/server-2.key \\"
echo "    --from-file=client.crt=certs/client.crt --from-file=client.key=certs/client.key"
echo "  kubectl apply -k k8s/overlays/etcd-tls"
