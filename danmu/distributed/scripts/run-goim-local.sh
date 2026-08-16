#!/usr/bin/env bash
# 本地起完整 goim 链路：etcd + logic + job + 2×comet。
# 前置：一个可用的 Kafka（默认 localhost:9092）、一个 etcd（默认 localhost:2379）。
#       本机有 etcd 二进制就用二进制，否则有 docker 就用 docker 拉起；
#       两者都没有则报错退出（Kafka 同理假定已就绪）。落库可另起 consumer + ClickHouse。
#
# 用法：
#   在 danmu/distributed/ 下执行：
#   bash scripts/run-goim-local.sh          # 启动全链路
#   压测：  loadtest 是架构无关的公共工具，在 ../monolith 里构建后使用：
#           (cd ../monolith && go build -o ../distributed/bin/loadtest ./loadtest/)
#           ./bin/loadtest --server=ws://localhost:8080,ws://localhost:8081 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
#   浏览器：http://localhost:8080  （必须在 danmu/distributed/ 下运行 comet 才能找到 web/）
#   停止：  bash scripts/run-goim-local.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."

KAFKA=${KAFKA:-localhost:9092}
ETCD_ADDR=${ETCD_ADDR:-localhost:2379}
ETCD_PEER_ADDR=${ETCD_PEER_ADDR:-127.0.0.1:12380}
TOKEN=${DANMU_AUTH_TOKEN:-danmu-secret-token}
export DANMU_AUTH_TOKEN=$TOKEN
mkdir -p bin logs

if [[ "${1:-}" == "stop" ]]; then
  pkill -f 'bin/(logic|job|comet|ops)' 2>/dev/null || true
  pkill -f 'etcd --name danmu-etcd' 2>/dev/null || true
  docker stop danmu-etcd 2>/dev/null || true
  echo "stopped."
  exit 0
fi

echo "building..."
go build -o bin/logic ./logic/
go build -o bin/job   ./job/
go build -o bin/comet ./comet/
go build -o bin/ops   ./cmd/ops/

# --- etcd：本机二进制优先，docker 兜底 ---
if command -v etcd >/dev/null 2>&1; then
  echo "starting etcd (local binary)..."
  nohup etcd --name danmu-etcd --data-dir etcd-data \
    --listen-client-urls "http://$ETCD_ADDR" \
    --advertise-client-urls "http://$ETCD_ADDR" \
    --listen-peer-urls "http://$ETCD_PEER_ADDR" \
    --initial-advertise-peer-urls "http://$ETCD_PEER_ADDR" \
    --initial-cluster "danmu-etcd=http://$ETCD_PEER_ADDR" \
    > logs/etcd.log 2>&1 &
elif command -v docker >/dev/null 2>&1; then
  echo "starting etcd (docker)..."
  docker run -d --rm --name danmu-etcd --network host \
    quay.io/coreos/etcd:v3.5.21 \
    /usr/local/bin/etcd --name danmu-etcd \
    --listen-client-urls "http://$ETCD_ADDR" \
    --advertise-client-urls "http://$ETCD_ADDR" \
    --listen-peer-urls "http://$ETCD_PEER_ADDR" \
    --initial-advertise-peer-urls "http://$ETCD_PEER_ADDR" \
    --initial-cluster "danmu-etcd=http://$ETCD_PEER_ADDR" \
    > logs/etcd.log 2>&1
else
  echo "FATAL: 未找到 etcd 二进制或 docker。安装 etcd（如 apt install etcd-server）后重试，"
  echo "或自行启动一个监听 $ETCD_ADDR 的 etcd 再跑本脚本。"
  exit 1
fi
sleep 1

echo "starting logic x1..."
bin/logic -addr=:7400 -id=logic1 -etcd=$ETCD_ADDR -advertise=localhost:7400 -kafka=$KAFKA > logs/logic1.log 2>&1 &

echo "starting job..."
bin/job -kafka=$KAFKA -etcd=$ETCD_ADDR > logs/job.log 2>&1 &

echo "starting comet x2..."
bin/comet -ws-addr=:8080 -rpc-addr=:7500 -advertise=localhost:7500 -id=comet1 -etcd=$ETCD_ADDR -pprof=:6060 > logs/comet1.log 2>&1 &
bin/comet -ws-addr=:8081 -rpc-addr=:7501 -advertise=localhost:7501 -id=comet2 -etcd=$ETCD_ADDR -pprof=:6061 > logs/comet2.log 2>&1 &

echo "starting ops console..."
bin/ops -addr=:7900 -etcd=$ETCD_ADDR -kafka=$KAFKA > logs/ops.log 2>&1 &

sleep 4
echo "--- etcd view ---"
if command -v etcdctl >/dev/null 2>&1; then
  etcdctl --endpoints="$ETCD_ADDR" get --prefix danmu/services/
else
  echo "(无 etcdctl，跳过；可自行执行：etcdctl --endpoints=$ETCD_ADDR get --prefix danmu/services/)"
fi
echo
echo "链路已启动。WS 入口：ws://localhost:8080 与 ws://localhost:8081"
echo "Ops Console：http://localhost:7900"
echo "日志在 logs/，停止：bash scripts/run-goim-local.sh stop"
