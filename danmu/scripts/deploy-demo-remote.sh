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

echo "[1/4] 启动 Redis 7（LANG=C 规避容器缺 locale 导致 setlocale 卡启动的问题）"
nohup env LANG=C LC_ALL=C "$DEMO_ROOT/bin/redis-server" --port 6379 --save '' --appendonly no \
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

echo "[5/5] supervisord 托管（崩溃自愈 + AutoDL 开机自启）"
SUP_BIN=""
for c in /root/miniconda3/bin/supervisord /usr/bin/supervisord; do
  [ -x "$c" ] && SUP_BIN="$c" && break
done
if [ -n "$SUP_BIN" ]; then
  SUPCTL="$(dirname "$SUP_BIN")/supervisorctl"
  cat > "$DEMO_ROOT/supervisord.conf" <<CONF
[unix_http_server]
file=$DEMO_ROOT/supervisor.sock

[supervisorctl]
serverurl=unix://$DEMO_ROOT/supervisor.sock

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisord]
logfile=$DEMO_ROOT/logs/supervisord.log
logfile_maxbytes=10MB
pidfile=$DEMO_ROOT/supervisord.pid
nodaemon=false
minfds=65535

[program:redis]
command=$DEMO_ROOT/bin/redis-server --port 6379 --save '' --appendonly no
environment=LANG=C,LC_ALL=C
autorestart=true
startsecs=2
stdout_logfile=$DEMO_ROOT/logs/redis.out.log
stderr_logfile=$DEMO_ROOT/logs/redis.err.log

[program:srvA]
command=$DEMO_ROOT/bin/danmu-server -addr :18080 -id srvA -mq both -redis localhost:6379 -redis-sharded -pprof :16070
directory=$DEMO_ROOT
autorestart=true
startsecs=2
stdout_logfile=$DEMO_ROOT/logs/srvA.out.log
stderr_logfile=$DEMO_ROOT/logs/srvA.err.log

[program:srvB]
command=$DEMO_ROOT/bin/danmu-server -addr :18081 -id srvB -mq both -redis localhost:6379 -redis-sharded -pprof :16071
directory=$DEMO_ROOT
autorestart=true
startsecs=2
stdout_logfile=$DEMO_ROOT/logs/srvB.out.log
stderr_logfile=$DEMO_ROOT/logs/srvB.err.log
CONF
  cat > "$DEMO_ROOT/start-all.sh" <<EOF
#!/bin/bash
ulimit -n 1048576
mkdir -p "$DEMO_ROOT/logs"
if [ -f "$DEMO_ROOT/supervisord.pid" ] && kill -0 "\$(cat "$DEMO_ROOT/supervisord.pid")" 2>/dev/null; then
  echo "supervisord already running (pid \$(cat "$DEMO_ROOT/supervisord.pid"))"; exit 0
fi
"$SUP_BIN" -c "$DEMO_ROOT/supervisord.conf" > /dev/null 2>&1 &
sleep 2
echo "supervisord started"
EOF
  chmod +x "$DEMO_ROOT/start-all.sh"
  # AutoDL 开机自启（若目录存在）
  if [ -d /root/autodl-tmp ]; then
    cat > /root/autodl-tmp/autodl.sh <<EOF
#!/bin/bash
ulimit -n 1048576
mkdir -p "$DEMO_ROOT/logs"
"$SUP_BIN" -c "$DEMO_ROOT/supervisord.conf" > /dev/null 2>&1 &
EOF
    chmod +x /root/autodl-tmp/autodl.sh
    echo "  AutoDL 开机自启已配置 (/root/autodl-tmp/autodl.sh)"
  fi
  # 从 nohup 裸进程切换到 supervisord
  for pid in $(ls /proc | grep -E '^[0-9]+$'); do
    CMD=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null)
    case "$CMD" in *danmu-serve[r]*|*redis-serve[r]*) kill "$pid" 2>/dev/null;; esac
  done
  sleep 1
  bash "$DEMO_ROOT/start-all.sh"
  sleep 3
  "$SUPCTL" -c "$DEMO_ROOT/supervisord.conf" status || true
  echo "  托管完成：supervisord=$SUP_BIN（自愈+自启）"
else
  echo "  未发现 supervisord，保持 nohup 裸进程模式（无自愈/自启）"
fi

echo
echo "部署完成。访问方式（容器只暴露 SSH 端口时）：
  ssh -L 8080:localhost:18080 -L 8081:localhost:18081 -p <ssh端口> root@<host>
  # 浏览器打开 http://localhost:8080（demo） / http://localhost:8080/live.html（实时回传监控）
日志：$DEMO_ROOT/logs/（supervisord 托管）；管理：supervisorctl -c $DEMO_ROOT/supervisord.conf status"
