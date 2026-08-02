# X-Plore V2:类 B 站视频平台升级

> 决策日期:2026-07-17。在点播 MVP(M1–M6,见 [EVOLUTION.md](./EVOLUTION.md))之上,
> 前端全面重做为产品级 UI,后端新增 search / social 微服务并补推荐接口。

## 目标

| 维度 | V1(MVP) | V2 |
|------|----------|----|
| 前端 | 5 页流程壳(~900 行) | 类 B 站产品级:首页信息流、播放页三栏、搜索页、个人空间、统一设计系统 |
| 微服务 | user / video / media + gateway + comet | + **search**(:8005)+ **social**(:8004)+ video 补 hot/related |
| 面试叙事 | "闭环能跑" | "像一个真平台" + 服务拆分边界的取舍 |

## 新增服务与 API 合约

### social(:8004 HTTP / :9004 gRPC)

关注关系与 UP 主公开信息。表 `user_follows(follower_id, followee_id)` 唯一约束。

| 接口 | 鉴权 | 说明 |
|------|------|------|
| `POST /v1/users/{id}/follow` | JWT | toggle 关注,返回 `{following, followerCount}` |
| `GET /v1/users/{id}/profile` | 可选 JWT | 用户名(gRPC 调 user)+ 关注/粉丝数 + 当前用户是否已关注 |
| `GET /v1/users/{id}/followings` `followers` | 无 | 分页列表 |

### search(:8005 HTTP / :9005 gRPC)

| 接口 | 说明 |
|------|------|
| `GET /v1/search/videos?q=&page=&size=&category=` | 标题/简介检索,仅 `status=ready` |
| `GET /v1/search/suggest?q=` | 搜索框联想(标题前缀,limit 10) |

实现:`Searcher` 接口 + PG ILIKE 默认实现(pg_trgm 索引可选);演进位:ES 实现 + outbox 同步,不改接口。
取舍(面试点):搜索直读 videos 表(共库读旁路),写路径不受影响;拆库时换 ES 实现即可。

### video 服务补充

| 接口 | 说明 |
|------|------|
| `GET /v1/videos/hot?limit=` | `play_count + 3*like` 加权,首页推荐位 |
| `GET /v1/videos/{id}/related?limit=` | 同分区按热度,排除自身,播放页侧栏 |

### gateway 路由新增

```text
/v1/search/*  -> search :8005
/v1/users/*   -> social :8004   (register/login 仍归 user :8001)
/v1/videos/hot、/{id}/related -> video :8003(前缀已覆盖)
```

## 前端重做(web-app)

设计系统:粉白主色(#fb7299 系)+ 暗色支持、design tokens(CSS 变量)、统一卡片/按钮/骨架屏。

| 页面 | 内容 |
|------|------|
| 全局框架 | 顶栏(logo/搜索框+联想/投稿/头像菜单)+ 分区导航 |
| Home | 推荐位(hot)+ 视频卡片网格(封面/时长角标/播放量/UP/发布时间)+ 分区 tab + 分页加载 + 骨架屏 |
| Watch | 三栏:自定义播放器控制条(播放/进度/音量/倍速/网页全屏/全屏 + 弹幕开关/透明度/发送框)· UP 卡片+关注 · 互动条 · 评论区 · 相关视频侧栏 |
| Search | 结果列表页 + 空态;顶栏搜索框全站可用 |
| Space `/space/:uid` | UP 信息(粉丝/关注数+关注按钮)+ 投稿列表 |
| Upload / Mine / Login | 视觉对齐设计系统;Upload 保留预签名直传+进度 |

## 里程碑(2026-07-17 完成第一轮)

- [x] V2-B1 social 服务(proto → buf 生成 → 实现 → gateway 路由):8004/:9004
- [x] V2-B2 search 服务:8005/:9005;修复 gorm Order(Expr) 静默忽略、PG DISTINCT+表达式排序冲突
- [x] V2-B3 video:ListVideos 加 user_id/sort=hot、/v1/videos/{id}/related、列表补封面+UP 名、IncrPlay 落 PG
- [x] V2-F1 设计系统(index.css tokens 亮/暗 + App.css)+ TopNav(搜索联想/用户菜单)
- [x] V2-F2 Home:热门加权区 + 分区 tab + 卡片流 + 加载更多 + 骨架屏/空态
- [x] V2-F3 Watch:自定义播放器(进度/缓冲/倍速/音量/弹幕开关/双全屏)+ UP 关注卡 + 相关推荐 + 评论重做
- [x] V2-F4 Search / Space(/space/:uid)/ Upload / Mine / Login / 404 重做
- [x] V2-V go build+vet 全过;前端 tsc+vite build+oxlint 全过;gateway 新路由冒烟通过
- [x] 真机闭环验收(2026-07-17):装好 Docker Desktop 29.6.1(WSL2)+ ffmpeg 6.0(D:\tools\ffmpeg,已入 PATH);
  注册→投稿→预签名直传→FFmpeg HLS 转码 ready→m3u8/分片可播→弹幕存取→评论/点赞/收藏→
  search/suggest/hot/follow/profile 全部经 gateway :8088 冒烟通过。
  注意:MinIO 控制台宿主端口已改 9101(9001 与 user gRPC 冲突);中文 JSON 用 curl 时需 UTF-8 文件体(Git Bash GBK 坑)。

## 附:pkg 变更

- JWT 中间件自 `app/video/internal/middleware` 提升为 `pkg/authmw`,video/social 共用。

## 工具链备注

本机无 protoc,使用 `buf`(已 `go install`,`~/go/bin/buf.exe`)+ 既有 protoc-gen-go / go-grpc / go-http 插件生成;
`wavehub-micro/buf.gen.yaml` + `buf.yaml` 已配置,生成命令:`cd wavehub-micro && buf generate`。
