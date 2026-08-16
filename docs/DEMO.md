# X-Plore 演示指南（弹幕 + 点播平台）

> 一条叙事讲清仓库里**两条主线**，以及如何 15 分钟演示「类 B 站点播 MVP」。  
> 技术演进细节见 [EVOLUTION.md](../wavehub/EVOLUTION.md)；弹幕压测见 [PROJECT.md](../danmu/PROJECT.md) / [README.md](../README.md)。

---

## 1. 一句话

**X-Plore** =  
① **百万级 WebSocket 弹幕**（单体 + goim 微服务，可压测）  
② **点播业务闭环**（Kratos 用户/稿件/转码 + React 播放/弹幕/互动）

---

## 2. 架构（演示时画这个）

```text
                    React web-app :5173
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
        gateway :8088              MinIO 预签名
     /v1/*  /ws  /metrics          上传/HLS 播放
         │
    ┌────┼──────┬──────┬─────────┐
    ▼    ▼      ▼      ▼         ▼
  user video social search     comet
  :8001 :8003 :8004  :8005    :8080 WS
         │            │
         ▼            │ room_id = video_id
    media worker      │
    (FFmpeg→HLS)     │
         │            │
    PG / Redis / MinIO / (Kafka 弹幕可选)
```

**纪律（面试加分）**

- 大文件不经 Go：预签名直传 MinIO  
- 长连接独立进程（comet），业务用 Kratos  
- 弹幕 `room_id` ≡ 字符串 `video_id`  
- WS 鉴权双模：业务 JWT + 压测 secret  

---

## 3. 前置检查

```powershell
cd D:\X-Plore\wavehub
.\scripts\check-platform.ps1
.\scripts\dev-platform.ps1 deps
```

| 依赖 | 用途 | 没有时 |
|------|------|--------|
| Docker | PG/Redis/MinIO/… | 业务起不来 |
| ffmpeg | HLS 转码 | 可登录/发弹幕，不能出片 |
| Go / Node | 编译运行 | 必需 |

---

## 4. 启动顺序（全功能）

### 4.1 基础设施

```powershell
cd D:\X-Plore\wavehub\deploy\platform
docker compose up -d
```

### 4.2 后端（多个终端）

```powershell
cd D:\X-Plore\wavehub\micro
$env:JWT_SECRET='dev-only-change-me'

go run ./app/user/cmd          # :8001
go run ./app/video/cmd         # :8003
go run ./app/social/cmd        # :8004 关注/粉丝(V2)
go run ./app/search/cmd        # :8005 搜索/联想(V2)
$env:ENABLE_TRACK_WORKER='false'; go run ./app/media/cmd

cd D:\X-Plore\danmu\distributed
$env:JWT_SECRET='dev-only-change-me'
go run ./comet -ws-addr=:8080  # standalone 即可演示实时弹幕

cd D:\X-Plore\wavehub\micro
go run ./app/gateway/cmd       # :8088
```

或：

```powershell
cd D:\X-Plore\wavehub
.\scripts\dev-platform.ps1 build
.\scripts\dev-platform.ps1 gateway
```

### 4.3 前端

```powershell
cd D:\X-Plore\wavehub
# 直连拆分端口（Vite 代理）
.\scripts\dev-platform.ps1 frontend

# 或走网关
$env:USE_GATEWAY='1'
.\scripts\dev-platform.ps1 frontend
```

浏览器：http://localhost:5173  

---

## 5. 演示剧本（约 10～15 分钟）

### A. 产品路径（点播）

1. **注册 / 登录** → 说明统一 JWT  
2. **投稿** → 选短 mp4 → 看「我的」状态 `processing → ready`  
3. **播放页** → HLS 播放、飘字弹幕开关、侧栏历史  
4. **点赞 / 收藏 / 评论** → M5 互动  
5. （可选）打开 Network：上传是 **PUT 到 MinIO**，不是打业务 API body  

### B. 工程路径（弹幕 / 网关）

1. `curl http://localhost:8088/health` / `/metrics`  
2. 说明 gateway 限流、`X-Request-Id`、CORS  
3. 弹幕：comet 分片 Hub、standalone vs goim（Kafka→Job→PushRoom）  
4. 压测（可选）：`loadtest` + PROJECT.md 压测表  

### C. curl 冒烟（无 UI）

```powershell
cd D:\X-Plore\wavehub
.\scripts\smoke-vod.ps1 -SampleMp4 C:\path\to\short.mp4
```

---

## 6. 验收清单（MVP）

- [ ] 注册登录成功  
- [ ] 投稿后可 ready（需 ffmpeg）  
- [ ] 播放页能播 HLS  
- [ ] 登录后 JWT 连 WS 发弹幕  
- [ ] 刷新后历史弹幕仍在（PG）  
- [ ] 点赞/评论生效  
- [ ] gateway `/metrics` 有计数  

---

## 7. 文档索引

| 文档 | 内容 |
|------|------|
| **DEMO.md（本文）** | 演示顺序 |
| [EVOLUTION.md](../wavehub/EVOLUTION.md) | 点播演进决策与里程碑 |
| [PROJECT.md](../danmu/PROJECT.md) | 弹幕系统统合 |
| [wavehub/deploy/platform/PRODUCTION.md](../wavehub/deploy/platform/PRODUCTION.md) | 上线清单 |
| [wavehub/deploy/platform/.env.example](../wavehub/deploy/platform/.env.example) | 环境变量 |
| [wavehub/micro/MICROSERVICES.md](../wavehub/micro/MICROSERVICES.md) | Kratos 学习笔记 |
| [danmu/distributed/DESIGN.md](../danmu/distributed/DESIGN.md) | goim 式微服务化设计 |
| [danmu/monolith/REVIEW.md](../danmu/monolith/REVIEW.md) | 单体版代码审查报告 |

---

## 8. 已知限制（诚实讲）

- 本机若无 Docker/ffmpeg，只能演示前端壳 + 已启动的服务  
- HLS 默认公共读 `MINIO_PUBLIC_BASE`；生产应 CDN + 私有原片  
- 关注 / 推荐 / 搜索 / 直播推流未做  
- OTel、etcd、mTLS 仍在后置  

---

*演示目标：听的人 5 分钟内明白「你做了什么、为什么拆、边界在哪」。*
