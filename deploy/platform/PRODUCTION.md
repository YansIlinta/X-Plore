# X-Plore 生产硬化清单（M6）

## 1. 密钥与配置

- [ ] 所有服务使用**同一** `JWT_SECRET`（长度 ≥ 32，随机）
- [ ] 修改 `DANMU_AUTH_TOKEN`；生产可关闭 secret 仅允许 JWT（后续开关）
- [ ] 数据库 / Redis / MinIO 密码不使用默认 `wavehub`
- [ ] 密钥仅来自环境变量或密钥管理，**禁止**进 Git
- [ ] 参考 `.env.example`，本地用 `.env`（已建议 gitignore）

## 2. 网络与 TLS

- [ ] 对外只暴露 **Gateway**（或 CDN/LB → Gateway），user/video/comet gRPC 内网
- [ ] HTTPS 终结在 Nginx / Caddy / 云 LB；Gateway 前配置 TLS
- [ ] `CORS_ORIGINS` 设为前端真实域名（不要 `*`）
- [ ] `WS_ALLOWED_ORIGINS` 与前端域名一致
- [ ] MinIO 桶：原片私有；`videos/*/hls/*` 可 CDN 公共读或签名 URL

## 3. 可观测

| 端点 | 说明 |
|------|------|
| `GET :8088/health` | 存活 |
| `GET :8088/ready` | 就绪 |
| `GET :8088/metrics` | gateway 请求/限流计数（Prometheus 文本） |
| comet `/metrics` | 连接/消息（透传 `/api` 或直连） |
| 日志 | gateway 带 `X-Request-Id`；按 rid 串联排障 |

建议：Prometheus scrape gateway + comet；日志收集 JSON/结构化可后置。

## 4. 限流与容量

- Gateway `RATE_LIMIT_PER_SEC`（默认 100/IP/s）按带宽调整
- media worker 并发保持低（转码吃 CPU）
- 弹幕仍允许丢消息（`sendCh` 满丢弃）

## 5. 数据

- [ ] PostgreSQL 定期备份
- [ ] MinIO/S3 生命周期（原片/冷存）
- [ ] 点播弹幕表 `video_danmus` 可按 video_id 归档
- [ ] ClickHouse（若启用直播历史）设 TTL

## 6. 发布检查

```bash
# 构建
cd wavehub-micro && go build -o bin/user ./app/user/cmd
go build -o bin/video ./app/video/cmd
go build -o bin/media ./app/media/cmd
go build -o bin/gateway ./app/gateway/cmd
cd .. && go build -o wavehub-micro/bin/comet ./comet

# 冒烟
curl -s http://localhost:8088/health
curl -s http://localhost:8088/metrics
# smoke-vod（需依赖与 ffmpeg）
./scripts/smoke-vod.ps1 -SampleMp4 short.mp4
```

## 7. 明确后置（未在本仓做完）

- OpenTelemetry 全链路 trace
- etcd 服务发现替换静态地址
- mTLS 服务间认证
- CDN 自动刷新与多清晰度 ABR
- 内容审核 / 风控 / 验证码

## 8. 推荐生产拓扑

```text
Internet → CDN (HLS) + LB(TLS) → gateway:8088
                                    ├ user
                                    ├ video
                                    ├ comet (WS)
                                    └ (gRPC 内网)
media worker ← Redis asynq ← video
MinIO/S3 ← 预签名上传 / HLS 读
PostgreSQL + Redis
```
