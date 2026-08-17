#!/bin/bash
# deploy-demo-remote.sh —— 远端演示环境一键部署
#
# 用途：在 SeetaCloud（或其他 Linux 容器）上拉起完整弹幕演示：
#   Redis 7（本仓库自编译静态依赖） + 2×danmu-server（sharded Pub/Sub 跨机）+ 新版 UI
#
# 用法（在"有公网可达端口"的机器上）：
#   1. 把本地产物传到远端（在 X-Plore 仓库根目录执行）：
#      scp -P <ssh端口> danmu/monolith/web/index.html \
#          /tmp/danmu-server-new /tmp/redis-server /tmp/redis-cli \
#          root@<host>:/root/demo-in/
#      （或直接把本脚本与产物放同一目录后整体 scp）
#   2. 远端执行：
#      bash deploy-demo-remote.sh <server二进制> <redis-server> <redis-cli> <index.html>
#
# 产物准备（本仓库）：
#   go build -o /tmp/danmu-server-new ./danmu/monolith/server
#   Redis 7 需自编译或从发行版获取：redis-server / redis-cli
#
# 部署内容：
#   /root/demo/bin/{danmu-server, redis-server, redis-cli}
#   /root/demo/web/index.html
#   服务：redis :6379（后台）；danmu srvA :18080；danmu srvB :18081
#   健康检查 + 跨机冒烟（用 danmu-loadtest 双机验证）
#
# 访问：
#   容器一般只暴露 SSH 端口，演示端口需 SSH 隧道：
#     ssh -L 8080:localhost:18080 -p <ssh端口> root@<host>
#   然后浏览器打开 http://localhost:8080（srvA 的页面）
#   或者用 srvB 隧道: -L 8081:localhost:18081
set -euo pipefail

# 可通过环境变量覆盖部署路径（非 root 环境测试 / 自定义目录）
DEMO_ROOT="${DEMO_ROOT:-/root/demo}"
DEMO_IN="${DEMO_IN:-/root/demo-in}"

BIN_SERVER="${1:-$DEMO_IN/danmu-server-new}"
BIN_REDIS="${2:-$DEMO_IN/redis-server}"
BIN_CLI="${3:-$DEMO_IN/redis-cli}"
PAGE="${4:-$DEMO_IN/index.html}"

for f in "$BIN_SERVER" "$BIN_REDIS" "$BIN_CLI" "$PAGE"; do
  [ -f "$f" ] || { echo "缺少文件: $f"; exit 1; }
done

mkdir -p "$DEMO_ROOT/bin" "$DEMO_ROOT/web"
cp -f "$BIN_SERVER" "$DEMO_ROOT/bin/danmu-server"
cp -f "$BIN_REDIS"  "$DEMO_ROOT/bin/redis-server"
cp -f "$BIN_CLI"    "$DEMO_ROOT/bin/redis-cli"
cp -f "$PAGE"       "$DEMO_ROOT/web/index.html"
chmod +x "$DEMO_ROOT"/bin/*

# 清掉旧实例
for p in $(ss -tlnp 2>/dev/null | grep -E ':18080|:18081|:6379' | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill "$p" 2>/dev/null || true
done
sleep 1

ulimit -n 1048576 || true

echo "[1/4] 启动 Redis 7"
nohup "$DEMO_ROOT/bin/redis-server" --port 6379 --save '' --appendonly no \
  > "$DEMO_ROOT/redis.log" 2>&1 &
sleep 1
"$DEMO_ROOT/bin/redis-cli" -p 6379 ping | grep -q PONG || { echo "Redis 启动失败"; exit 1; }

echo "[2/4] 启动 danmu srvA :18080 / srvB :18081（sharded Pub/Sub 跨机）"
cd "$DEMO_ROOT"
nohup "$DEMO_ROOT/bin/danmu-server" -addr :18080 -id srvA -mq both -redis localhost:6379 -redis-sharded \
  -pprof :16070 > "$DEMO_ROOT/srvA.log" 2>&1 &
nohup "$DEMO_ROOT/bin/danmu-server" -addr :18081 -id srvB -mq both -redis localhost:6379 -redis-sharded \
  -pprof :16071 > "$DEMO_ROOT/srvB.log" 2>&1 &
sleep 2

echo "[3/4] 健康检查"
for port in 18080 18081; do
  curl -sf "http://localhost:$port/health" >/dev/null && echo "  srv$([ $port = 18080 ] && echo A || echo B) :$port OK" || { echo "  srv :$port FAILED"; exit 1; }
done
curl -sf http://localhost:18080/ | grep -q 'X-Plore' && echo "  UI 页面 OK" || echo "  UI 页面警告：未匹配标题"

echo "[4/4] 跨机冒烟（可选，需 danmu-loadtest）"
if [ -f "$DEMO_IN/danmu-loadtest" ]; then
  cp -f "$DEMO_IN/danmu-loadtest" "$DEMO_ROOT/bin/" 2>/dev/null || true
  "$DEMO_ROOT/bin/danmu-loadtest" -server ws://localhost:18080,ws://localhost:18081 \
    -conns 100 -rooms 2 -rate 1 -duration 10s -ramp 1s 2>&1 | grep -E 'Total Sent|Total Received|Dropped|Connect Failed' || true
fi

echo
echo "部署完成。访问方式（容器只暴露 SSH 端口时）：
  ssh -L 8080:localhost:18080 -p <ssh端口> root@<host>   # 浏览器打开 http://localhost:8080
  （srvB: -L 8081:localhost:18081）
日志：$DEMO_ROOT/srvA.log $DEMO_ROOT/srvB.log $DEMO_ROOT/redis.log"
