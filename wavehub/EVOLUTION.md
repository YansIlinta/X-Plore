# X-Plore 视频平台演进（点播可运营 MVP）

> 决策日期：2026-07-17  
> 状态：**MVP 代码主线完成**（M1–M6 + 前端体验）；真机转码依赖 Docker/ffmpeg。演示见 [DEMO.md](../docs/DEMO.md)。  
> **V2（类 B 站升级）已完成第一轮**：前端产品级重做 + search(:8005)/social(:8004) 微服务 + 热门/相关推荐，见 [ROADMAP-V2.md](./ROADMAP-V2.md)。

## 产品决策

| 项 | 选择 |
|----|------|
| MVP | **点播**：上传 → FFmpeg HLS → 播放页 + 弹幕绑定稿件 |
| 目标 | 长期可运营（网关 / etcd / 观测 / 前端工程化） |
| 前端 | React + TypeScript + Vite + hls.js |
| 业务底座 | `micro`（Kratos + gRPC） |
| 弹幕连接层 | 现有 goim：`comet` / `logic` / `job`（独立进程） |
| 房间约定 | `room_id = strconv.FormatUint(video_id, 10)` |

## 与现有代码的关系

> 路径均相对仓库根 `X-Plore/`。

| 路径 | 角色 |
|------|------|
| `danmu/monolith/server/` | 弹幕**单体基线**，压测对照，保留 |
| `danmu/distributed/{comet,logic,job,core,minirpc}/` | 弹幕微服务，继续作为实时通道 |
| `wavehub/monolith/` | 音乐站 Gin 单体，学习对照，不作为视频主线 |
| `wavehub/micro/` | **视频平台业务主线**（user + video + media） |
| `danmu/*/web/index.html` | 弹幕联调页，保留 |
| `wavehub/web-app/` | **产品前端**（React） |
| `wavehub/deploy/platform/` | 平台依赖 compose（PG/Redis/MinIO/Kafka/CH/etcd） |

## 端口约定

| 服务 | HTTP | gRPC |
|------|------|------|
| user | 8001 | 9001 |
| track（音乐旁路） | 8002 | 9002 |
| **video** | **8003** | **9003** |
| **social(V2)** | **8004** | **9004** |
| **search(V2)** | **8005** | **9005** |
| media worker | — | 调 video/track |
| comet WS | 8080 | （PushRoom RPC 见 goim 配置） |
| **gateway** | **8088** | 统一 `/v1/*` + `/ws` |

## 环境变量（video 常用）

```bash
PG_DSN=host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable
REDIS_ADDR=localhost:6379
USER_GRPC_ADDR=localhost:9001
VIDEO_GRPC_ADDR=localhost:9003
JWT_SECRET=dev-only-change-me
MINIO_ADDR=localhost:9000
MINIO_ACCESS_KEY=wavehub
MINIO_SECRET_KEY=wavehub123
MINIO_BUCKET=xplore-video
HTTP_ADDR=:8003
GRPC_ADDR=:9003
DANMU_WS_URL=ws://localhost:8080/ws
```

## 里程碑

- [x] Phase 0 / M0 文档与骨架：compose、video proto/服务、React 壳、`dev-platform.ps1`
- [x] M1 代码就绪：HLS 公共 URL（`MINIO_PUBLIC_BASE`）、MinIO CORS 初始化、`scripts/smoke-vod.ps1`
- [x] M2 代码就绪：comet WS **JWT | DANMU_AUTH_TOKEN 双模**；Watch 页优先业务 JWT
- [x] M3 代码就绪：Go gateway `:8088`、nginx 样例、`dev-platform.ps1 gateway|build`、前端 `VITE_API_BASE` / `VITE_DANMU_WS`
- [ ] M0 环境：本机 Docker Desktop + ffmpeg（阻塞真机闭环）
- [ ] M1 验收：smoke-vod 跑通 ready + 浏览器播放
- [ ] M3 验收：经 gateway 完成登录/投稿/弹幕
- [x] M4 代码就绪：
  - `GET/POST /v1/videos/{id}/danmu`（PG `video_danmus`，按 `offset_ms`）
  - WS 上行/下行透传 `offset_ms`（comet standalone / logic Kafka）
  - Watch：历史拉取 + 实时叠加 + msg 去重 + 进度窗口侧栏
- [ ] M4 验收：有 PG 时发弹幕刷新仍在；播放进度附近展示
- [x] M5 代码就绪：评论 / 点赞 / 收藏（PG 同库 `video_comments|likes|favorites`）
  - `GET/POST /v1/videos/{id}/comments`
  - `POST /v1/videos/{id}/like|favorite`
  - `GET /v1/videos/{id}/stats`；详情带 like/comment/favorite 计数
  - Watch 页互动条 + 评论列表
- [x] M6 代码/文档就绪（第一刀）：
  - gateway：`X-Request-Id`、安全头、IP 限流、`CORS_ORIGINS`、`/ready` `/metrics`
  - comet：`WS_ALLOWED_ORIGINS` 白名单
  - `deploy/platform/.env.example` + `PRODUCTION.md` 上线清单
  - 后置：OTel 全链路、etcd 发现、mTLS、CDN 自动化
- [x] 前端体验：Watch 飘字弹幕层 + 发现页分区/时长/播放量；可关飘字

## 本地快速路径

> Windows 可用：`.\scripts\dev-platform.ps1 help` / `deps` / `frontend`

```bash
# 0. 前置
# - Docker Desktop（起 PG/Redis/MinIO/…）
# - ffmpeg + ffprobe 在 PATH（media 转 HLS）
# - Go 1.22+ / Node 20+

# 1. 依赖
cd deploy/platform && docker compose up -d

# 2. 业务（三个终端，在 wavehub/micro 下）
# Windows:
#   go run ./app/user/cmd
#   go run ./app/video/cmd
#   set ENABLE_TRACK_WORKER=false && go run ./app/media/cmd
make run-user
make run-video
ENABLE_TRACK_WORKER=false make run-media   # 需本机 ffmpeg

# 3. 网关（可选但推荐）
cd micro && go run ./app/gateway/cmd
# 或: .\scripts\dev-platform.ps1 gateway
# http://localhost:8088/health

# 4. 前端（npm 官方源不稳时用 npmmirror）
cd web-app
npm install --registry=https://registry.npmmirror.com
npm run dev
# http://localhost:5173
# 默认 Vite 代理拆到 8001/8003；经 gateway：
#   $env:USE_GATEWAY='1'; .\scripts\dev-platform.ps1 frontend
```

### curl 冒烟（业务已启动时）

```bash
# 注册
curl -s -X POST http://localhost:8001/v1/register \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"alice\",\"password\":\"pass1234\"}"
# → {"token":"...","userId":1}

# 创建稿件（替换 TOKEN）
curl -s -X POST http://localhost:8003/v1/videos \
  -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d "{\"title\":\"demo\",\"description\":\"hi\",\"category\":\"tech\"}"
# → id + uploadUrl；浏览器或 curl -T file.mp4 "uploadUrl"
# 再 POST /v1/videos/{id}/complete ，等 media worker 转码后 GET /v1/videos/{id} 取 playlistUrl
```

### 本机环境备注（2026-07 检查）

- 当前开发机 **未装 Docker / ffmpeg** 时，compose 与 HLS 转码无法在本机完成；代码与二进制已就绪，装好依赖后按上序启动即可。
- 前端 `react-router-dom` + `hls.js` 已写入 `package.json`，请用 **npmmirror** 安装以防 ECONNRESET。
