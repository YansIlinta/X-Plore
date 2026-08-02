#!/usr/bin/env bash
# 本地起完整 goim 链路：registry + logic + job + 2×comet。
# 前置：一个可用的 Kafka（默认 localhost:9092）。落库可另起 consumer + ClickHouse。
#
# 用法：
#   bash scripts/run-goim-local.sh          # 启动全链路
#   压测：  ./bin/loadtest --server=ws://localhost:8080,ws://localhost:8081 --conns=2000 --rooms=50 --rate=2 --duration=30s --token=danmu-secret-token
#   浏览器：http://localhost:8080  （在仓库根目录运行 comet 才能找到 web/）
#   停止：  bash scripts/run-goim-local.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."

KAFKA=${KAFKA:-localhost:9092}
REG=http://localhost:7350
TOKEN=${DANMU_AUTH_TOKEN:-danmu-secret-token}
export DANMU_AUTH_TOKEN=$TOKEN
mkdir -p bin logs

if [[ "${1:-}" == "stop" ]]; then
  pkill -f 'bin/(registry|logic|job|comet)' 2>/dev/null || true
  echo "stopped."
  exit 0
fi

echo "building..."
go build -o bin/registry ./cmd/registry/
go build -o bin/logic    ./logic/
go build -o bin/job      ./job/
go build -o bin/comet    ./comet/

echo "starting registry..."
bin/registry -addr=:7350 > logs/registry.log 2>&1 &
sleep 1

echo "starting logic x1..."
bin/logic -addr=:7400 -id=logic1 -registry=$REG -advertise=localhost:7400 -kafka=$KAFKA > logs/logic1.log 2>&1 &

echo "starting job..."
bin/job -kafka=$KAFKA -registry=$REG > logs/job.log 2>&1 &

echo "starting comet x2..."
bin/comet -ws-addr=:8080 -rpc-addr=:7500 -advertise=localhost:7500 -id=comet1 -registry=$REG -pprof=:6060 > logs/comet1.log 2>&1 &
bin/comet -ws-addr=:8081 -rpc-addr=:7501 -advertise=localhost:7501 -id=comet2 -registry=$REG -pprof=:6061 > logs/comet2.log 2>&1 &

sleep 4
echo "--- registry view ---"
echo "comet: $(curl -s "$REG/services?service=comet")"
echo "logic: $(curl -s "$REG/services?service=logic")"
echo
echo "链路已启动。WS 入口：ws://localhost:8080 与 ws://localhost:8081"
echo "日志在 logs/，停止：bash scripts/run-goim-local.sh stop"
