# WaveHub 微服务版：架构与选型

> 前置阅读：`../wavehub/ARCHITECTURE.md`（单体版）。两套代码实现**同一个业务**，
> 最好的学习方式是对比着读：同一个"上传→转码→可视化"流程，单体和微服务各怎么写。

---

## 0. 为什么是 Kratos

**候选：Kratos / go-zero / kitex / 裸 gRPC**（对比详见上一轮讨论，这里记录最终理由）

1. **B 站开源**，你在做音乐版 B 站，用它既对题又是简历上的好故事；
2. **proto-first**：一份 proto 同时生成 gRPC 服务端/客户端和 HTTP 路由，前端要 REST、内部要 gRPC，一份契约两头吃 —— 这是本项目结构的支点；
3. Kratos 本质是"工具箱"而非"全家桶"：transport、middleware、registry 都是可拆的库，**不绑架你的项目结构**，学到的是通用微服务概念而不是某框架的黑话。

**代价（诚实说）**：比 go-zero 概念多、生成器弱（go-zero 的 goctl 一条命令出整个服务）。所以本骨架做了两个刻意简化：

| 官方 `kratos new` 模板 | 本骨架 | 为什么 |
|---|---|---|
| wire 依赖注入（生成 wire_gen.go） | main.go 手工组装 | 先看清每根线怎么接，wire 只是自动化这个过程，熟练后再上 |
| proto 定义配置文件 + config 组件 | 环境变量 | 少一层生成物；配置中心是量大后的事 |

---

## 1. 服务怎么拆的（以及为什么这么拆）

```
浏览器/前端 ──── 预签名 URL 直传/直播放 ────► MinIO (音频文件)
   │ HTTP :8001               │ HTTP :8002        ▲ 下载
   ▼                          ▼                   │
 user 服务               track 服务 ──── gRPC :9001 ───► user (查作者)
 (注册/登录/JWT签发)      (作品CRUD/鉴权/签发直传URL)
                              │ asynq 投递(经 Redis)
                              ▼
                         media worker ── gRPC :9002 ──► track (回写结果)
                         (FFmpeg 转码/提峰值，无对外端口)
```

注意音频文件的完整路径：**上传和播放都不经过任何 Go 服务**（浏览器拿预签名 URL 和 MinIO 直接交互），Go 服务只搬元数据。这是文件类业务省带宽的标准架构。

**拆分依据不是"名词"而是"变化速率和资源模型"：**

- **user**：所有服务都依赖的身份源，最稳定，单独部署后别的服务随便重启都不影响登录态；
- **track**：业务迭代最频繁的地方（评论、点赞、收藏都会长在这里），要能天天发版；
- **media**：**唯一 CPU 密集**的服务。这是全项目最正当的拆分理由——转码忙的时候单独给它加机器，而不用把 API 一起扩容。它甚至没有对外端口，纯粹从队列领活。

**三条纪律（微服务的本质，比框架重要）：**

1. **数据归属**：`users` 表只有 user 服务能碰，`tracks` 表只有 track 服务能碰。media 转码完成后必须通过 `ReportProcessed` 这个 gRPC 回写，而不是直接连数据库改 —— 谁破坏这条，系统就退化成"分布式单体"（微服务的运维成本 + 单体的耦合，两头的坏处全占）。
2. **同步 vs 异步**：查作者信息用同步 gRPC（马上要用结果）；转码用异步队列（耗时几十秒，调用方不该等）。判断标准：**调用方是否需要立刻拿到结果**。
3. **对外 HTTP、对内 gRPC**：浏览器只认 HTTP/JSON；服务之间用 gRPC（强类型契约、二进制编码、连接复用）。同一份 service 代码两个口都挂，是 Kratos proto-first 的直接收益。

---

## 2. 每个服务内部：service / biz / data 三层

对应关系（和单体对照着记）：

| 单体 wavehub | 微服务每个 app | 职责 |
|---|---|---|
| handler | **service** | 协议适配：proto 消息 ⇄ 业务参数，必须薄 |
| service | **biz** | 业务规则全在这，**只依赖自己定义的 Repo 接口** |
| model + db 调用 | **data** | 实现 biz 的接口；表结构是本层私有的 |

biz 定义接口、data 实现接口（依赖倒置）带来的直接好处：给 biz 写单元测试时 mock 一个 Repo 就行，不用起数据库 —— 这是 Kratos 分层相对随手写的最大纪律价值。

错误处理统一用 `kratos/v2/errors`：`errors.NotFound("TRACK_NOT_FOUND", ...)` 在 HTTP 侧自动变 404、gRPC 侧自动变对应 code，业务代码不用关心传输层。

## 3. 鉴权设计

- user 服务**签发** JWT（登录/注册返回 token）；
- track 服务**验证** JWT：自写 Kratos 中间件（`app/track/internal/middleware/jwt.go`，和 Gin 版对比着看），并用 `selector` 只挂在写接口上 —— 列表/详情匿名可看，和 B 站一致；
- 两个服务共享 `JWT_SECRET` 环境变量。**验证不需要调 user 服务**（JWT 自包含），这是无状态 token 相对 session 在微服务里的核心优势：省掉每个请求一次 RPC；
- track 的 gRPC 端口（内部接口）不做用户鉴权，生产上靠内网隔离/mTLS —— "边缘认证、内部信任"是常见起步模型。

## 4. 服务发现：现在是静态地址，这是刻意的

当前 track 找 user 是 `USER_GRPC_ADDR=localhost:9001` 写死。演进路径三选一，**都对，取决于部署环境**：

1. **静态配置**（现在）：服务少、单机部署时完全够用，零组件；
2. **etcd 注册中心**：服务多、多实例时上。Kratos 有现成插件（`contrib/registry/etcd`），改动只有两处——服务端 `kratos.Registrar(r)`，客户端 endpoint 换成 `discovery:///wavehub.user`；
3. **Kubernetes DNS**：如果最终上 K8s，K8s 的 Service 本身就是服务发现，注册中心可以不要。

学习顺序建议 1 → 2（体验注册/健康检查的概念）→ 了解 3。

---

## 5. 怎么跑起来

```bash
# 1. 基础设施(复用单体版的 compose，同一套 PG/Redis/MinIO)
cd ../wavehub && docker compose up -d

# 2. 三个终端分别启动(顺序：user → track → media)
make run-user    # HTTP :8001 / gRPC :9001
make run-track   # HTTP :8002 / gRPC :9002
make run-media   # 无端口，连 Redis 领任务

# 3. 冒烟测试：注册 → 建作品 → 直传音频 → 通知完成 → 等转码 → 看详情
curl -X POST localhost:8001/v1/register -d '{"username":"alice","password":"123456"}'
# 拿到 token：
curl -X POST localhost:8002/v1/tracks -H "Authorization: Bearer <token>" -d '{"title":"我的第一首歌"}'
# 返回 {id, upload_url}，把本地音频直接 PUT 给 MinIO(注意 URL 要加引号)：
curl -T song.mp3 "<upload_url>"
curl -X POST localhost:8002/v1/tracks/<id>/complete -H "Authorization: Bearer <token>" -d '{}'
# media worker 日志会打出"开始处理"；几秒后：
curl localhost:8002/v1/tracks/<id>   # status=ready，含 peaks(画波形) 和 stream_url(直接播放)
```

改了 proto 之后重新生成代码：`make api`（需要 protoc + 三个插件，已装在 `C:\Users\huang\go`）。

## 6. 亲身对比后你该得出的结论（也是面试答案）

跑通两套之后回头看：同一个功能，微服务版**文件数 ×3、启动步骤 ×3、排错要跨三个进程的日志**，换来的是独立部署/独立扩容/故障隔离。什么时候值得？——**当团队和流量的规模让"改一处发全站"变得不可接受时**。能亲口说出这个代价对比，比背十篇微服务文章有用。

## 7. 下一步（按顺序）

1. ~~MinIO 预签名直传~~ ✅ 已完成：`CreateTrack` 返回直传 URL，`CompleteUpload` 校验文件存在+作品归属，
   media 从 MinIO 拉文件处理，详情页返回预签名播放地址（存储抽象见 `app/track/internal/biz` 的 `ObjectStorage` 接口）
2. etcd 服务发现（见第 4 节）
3. 列表页作者信息的批量获取：user 服务加 `BatchGetUsers` rpc，消灭 N+1 调用
4. 链路追踪：Kratos 内置 OpenTelemetry 中间件，跨三个服务追一个请求，微服务排错的必修课
5. 用 wire 替换 main.go 的手工组装，再回看官方 `kratos new` 模板就全能看懂了
