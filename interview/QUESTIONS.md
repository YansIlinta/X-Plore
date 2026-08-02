# X-Plore 面试题（题目 · 代码 · 简历锚点）

> **本文件只有题**，答案见 [ANSWERS.md](./ANSWERS.md)。  
> 格式：`题号 | 网上常见问法标签 | 题干 | 代码/文档 | 简历条目`  
> 仓库根：`D:\X-Plore`

---

## A. 项目总览与 STAR（Q01–Q10）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q01 | 介绍项目 | 用 60 秒介绍 X-Plore 弹幕系统：场景、指标、架构一句话 | `INTERVIEW.md`、`PROJECT.md` | 项目描述 |
| Q02 | 为什么做 | 为什么既做单体又做 goim 微服务？面试官听什么 | `PROJECT.md` §2 | 双架构对比 |
| Q03 | 你的角色 | 从 0 到 1 你独立做了哪些、哪些复用生态 | 目录 `server/` `comet/` `minirpc/` | 主要职责 |
| Q04 | 最大难点 | 挑一个最难问题讲 STAR（建议：高扇出/锁/双路径） | `REVIEW.md` | 建连/广播/演进 |
| Q05 | 量化成果 | 说出你压测的**环境与数字**，以及数字的可信条件 | `PROJECT.md` §5、`loadtest/` | 成果 P50/P90 |
| Q06 | 为什么可丢 | 弹幕为什么允许丢消息？和 IM 有何不同 | `core/client.go` sendCh | 设计取舍 |
| Q07 | 和 B 站关系 | 和 goim/B 站的关系：借鉴什么、自研什么 | `DESIGN-goim.md` | goim 拆分 |
| Q08 | 边界诚实 | 你项目**还不能**宣称什么？（避免吹嘘） | `PROJECT.md` §4.7 | 已知限制 |
| Q09 | 点播关系 | 弹幕主线与点播平台如何复用？room 怎么绑 | `EVOLUTION.md`、`DEMO.md` | 其他项目·弹幕复用 |
| Q10 | 技术选型 | 为什么 Go + WebSocket 而不是 SSE/长轮询 | `server/` `comet/` | 技术栈 |

---

## B. WebSocket 与连接模型（Q11–Q20）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q11 | 连接模型 | 一条连接几个 goroutine？职责划分？ | `core/client.go` ReadPump/WritePump | 连接模型 |
| Q12 | 背压 | sendCh 满了怎么办？为什么不阻塞？ | `core/client.go` TrySend | 慢客户端保护 |
| Q13 | 心跳 | 如何检测死连接？会话过期怎么断？ | `core/client.go` WritePump、session | 会话令牌 |
| Q14 | 鉴权 | WS 握手如何鉴权？JWT 与 secret 双模？ | `comet/main.go` handleWebSocket | 安全 |
| Q15 | Origin | 生产 CheckOrigin 应如何配置？ | `comet/main.go` makeCheckOrigin | 生产硬化 |
| Q16 | 升级 | HTTP 如何升级到 WebSocket？Nginx 要注意什么 | `nginx.conf`、gateway `/ws` | Nginx |
| Q17 | 粘连 | 为什么要对连接做一致性哈希？CDN 后 ip_hash 问题 | `nginx.conf` X-Forwarded-For | 一致性哈希 |
| Q18 | 百万连接 | 单机百万连接瓶颈在哪（fd/内存/goroutine）？你们做到哪 | `PROJECT.md` 边界 | 成果与边界 |
| Q19 | 房间进出 | 用户进房/退房如何注册到 Hub？并发安全吗？ | `core/hub.go` AddClient/RemoveClient | 分片 Hub |
| Q20 | 广播路径 | 一条弹幕从读到写到多个连接的完整路径（单体） | `server/worker.go` `hub` | 批量广播 |

---

## C. Go 并发与正确性（Q21–Q30）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q21 | 分片锁 | 为什么 256 分片？如何选分片键？ | `core/hub.go` numShards | 256 分片 |
| Q22 | 全局锁危害 | 全局 RWMutex 在万级连接下会发生什么？ | `REVIEW.md`、hub 演进 | 锁竞争 STAR |
| Q23 | Close 竞态 | Client.Close 有哪些并发坑？如何修？ | `core/client.go`、`REVIEW.md` | Close 竞态 |
| Q24 | channel 关闭 | 谁关闭 sendCh？双重 close 如何避免？ | `core/client.go` | 正确性 |
| Q25 | 令牌桶 | 无锁令牌桶如何实现？ABA 是什么？ | `core/ratelimit.go` | 令牌桶 ABA |
| Q26 | CAS | 为什么用单次 CAS 打包 tokens+时间？ | `core/ratelimit.go` packState | 令牌桶 |
| Q27 | 对象池 | Message 是否用了 sync.Pool？何时该用 | `core/message.go` | 工程优化 |
| Q28 | context | 优雅关闭如何用 context 传播？ | `comet/main.go` signal | 优雅关闭 |
| Q29 | 乱序 | 同房间弹幕为什么可能乱序？可接受吗？ | worker 多协程 | 已知取舍 |
| Q30 | 数据竞争 | 如何用 race detector 验证？你修过哪些 | `go test -race`、REVIEW | 审查 |

---

## D. 削峰、批量与实时路径（Q31–Q36）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q31 | 削峰 | 上行队列多大？满了怎么办？ | `comet` uplinkCh 100K | Worker 池 |
| Q32 | 批量 | 按条数与时间窗批量的利弊？窗口如何选？ | `server/worker.go` | 10ms 窗口 |
| Q33 | syscall | 为什么批量能降延迟的 P90？ | 广播扇出 | 写放大 STAR |
| Q34 | 本地广播 | standalone comet 本地广播路径 | `comet` localBroadcast | goim 冒烟 |
| Q35 | 过滤 | 敏感词在哪一层做？AC 自动机复杂度 | `core/filter.go` | 敏感词 |
| Q36 | msg_id | 全局 msg_id 谁生成？格式？ | `logic/main.go` nextMsgID | 去重 |

---

## E. Redis 与 Kafka（Q37–Q48）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q37 | 为何双中间件 | Redis 与 Kafka 分工？能只用一个吗？ | `PROJECT.md` §4.3 | 双路径 |
| Q38 | Pub/Sub 缺点 | Redis Cluster Pub/Sub 规模负向扩展原因 | `DESIGN-goim.md` | 演进动机 |
| Q39 | 全网反序列化 | 每机收到无关房间消息如何优化？ | `REVIEW.md` M6 | Redis 短路 |
| Q40 | Kafka 主题 | 弹幕 topic 如何分区？key 选 room 的好处 | `logic` Kafka key=room | 保序 |
| Q41 | 消费组 | storage 与 broadcast 消费组如何隔离 | `consumer/main.go` | 多消费组 |
| Q42 | 至少一次 | consumer 如何避免 offset 先提交丢数据？ | `consumer` CommitMessages | 丢数据窗口 |
| Q43 | 幂等 | at-least-once 如何配合去重？ | msg_id / CH | 可靠性 |
| Q44 | 为何不用 MQ 当广播 | 为什么不单用 Kafka 实时推所有在线用户 | 延迟与语义 | 追问速答 |
| Q45 | 削峰填谷 | Job 消费 Kafka 再推 Comet 解决什么问题 | `job/` | goim 扇出 |
| Q46 | 积压 | Kafka 积压时 Job 如何表现？背压策略 | job 消费 | 扩展 |
| Q47 | 顺序 | 同房间消息顺序保证到哪一层？ | Kafka partition | 乱序边界 |
| Q48 | 双写一致性 | Redis 实时与 Kafka 落库不一致时用户感知？ | 双路径 | 取舍 |

---

## F. ClickHouse 与历史（Q49–Q54）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q49 | 为何 CH | 为什么历史用 ClickHouse 不用 MySQL？ | `consumer/db.go` | MergeTree |
| Q50 | 表设计 | 分区键、排序键如何设计？查询模式？ | danmu_history ORDER BY | 按天分区 |
| Q51 | 批量写 | 为何 batch insert？小批量有何问题？ | BatchInsert | 吞吐 |
| Q52 | 历史 API | `/history` 曾是死接口，根因与修复？ | `server/history.go` REVIEW H2 | 审查 |
| Q53 | 点播历史 | 点播 offset_ms 历史为何可放 PG？ | `video_danmus` | 点播复用 |
| Q54 | 冷热 | 实时与历史查询如何拆分？ | Redis vs CH/PG | 架构 |

---

## G. goim 微服务拆分（Q55–Q66）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q55 | 拆分标准 | 为什么按 Comet/Logic/Job 拆？变化速率/资源模型 | `DESIGN-goim.md` | 微服务演进 |
| Q56 | 完整链路 | 口述一条弹幕 goim 全链路 | DESIGN 数据流 | 职责 |
| Q57 | PushRoom | Job 如何推？无房间为何 delivered=0 | `pb/danmu.proto` PushRoom | 定向推 |
| Q58 | 为何不本地 echo | 发送者消息也走回路的原因 | DESIGN-goim | 一致性 |
| Q59 | 无状态 | Logic 为什么无状态？如何扩 | `logic/main.go` | 水平扩展 |
| Q60 | 连接有状态 | Comet 状态是什么？故障如何迁移 | comet 连接 | 有状态服务 |
| Q61 | standalone | standalone 模式解决什么演示问题 | comet `-standalone` | 冒烟 |
| Q62 | 何时拆 | 什么规模才值得拆微服务？（反问） | 高扇出 P90 1.6s | 压测驱动 |
| Q63 | 数据归属 | 微服务数据归属纪律（结合 video/media） | MICROSERVICES.md | 点播服务拆分 |
| Q64 | 分布式单体 | 什么叫分布式单体？如何避免 | media 不写 tracks 表 | 纪律 |
| Q65 | 同步异步 | 查作者 gRPC vs 转码 asynq 如何选 | wavehub-micro | 同步异步 |
| Q66 | 对比 Gin | 单体 Gin WaveHub 与 Kratos 学习路径 | wavehub/ | 其他项目 |

---

## H. minirpc / 发现 / 熔断（Q67–Q74）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q67 | 为何自研 | 为什么自研 minirpc 而不是直接 gRPC？ | `minirpc/` | 自研 RPC |
| Q68 | 服务发现 | registry 如何实现？TTL 租约？ | `cmd/registry`、`minirpc/registry` | 发现 |
| Q69 | 一致性哈希 | Logic 路由如何按 room 哈希？ | `minirpc/lb`、logicpool | 一致性哈希 |
| Q70 | 熔断 | 熔断器状态机？失败阈值？ | `minirpc/breaker` | 熔断 |
| Q71 | 生产替换 | 生产用 etcd/K8s DNS 怎么演进 | DESIGN 边界 | 后置 |
| Q72 | gRPC vs HTTP | 对内 gRPC 对外 HTTP 的理由 | Kratos proto-first | 技术栈 |
| Q73 | Proto | OnMessage 请求字段有哪些？offset_ms 用途 | `pb/danmu.proto` | 协议 |
| Q74 | 负载均衡 | 客户端 LB 与 Nginx LB 层次差异 | nginx + minirpc lb | 多层 LB |

---

## I. 可观测、压测与缺陷（Q75–Q84）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q75 | 指标 | 暴露了哪些 Prometheus 指标？ | `core/metrics.go` | Prometheus |
| Q76 | 高基数 | 为何 room_id 不能做 label？ | REVIEW H1 | 高基数修复 |
| Q77 | pprof | 线上如何用 pprof 定位？ | pprof 端口 | 调优 |
| Q78 | 压测工具 | loadtest 如何测 E2E？HDR 是什么？ | `loadtest/` | HDR |
| Q79 | 纳秒 | 为何要从毫秒改成纳秒时间戳？ | REVIEW M4 | 延迟精度 |
| Q80 | 假错误 | 压测 Read Errors=连接数根因？ | REVIEW L5 | 工具缺陷 |
| Q81 | 直方图锁 | 全局直方图锁如何成为瓶颈？ | REVIEW L6 | 分片直方图 |
| Q82 | QPS 假象 | stats 的 qps 曾是累计值，如何修？ | REVIEW M1 | 正确性 |
| Q83 | 压测陷阱 | 压测机与 server 同机会怎样？ | PROJECT 注意 | 数字可信 |
| Q84 | 审查文化 | 你如何做代码审查清单？举 3 个高优先级 | REVIEW 全文 | 8 个缺陷 |

---

## J. 点播平台 / Kratos / 对象存储 / FFmpeg（Q85–Q95）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q85 | 服务拆分 | user/video/media 为何这样拆？ | `wavehub-micro/` | 5 服务（简历表述可对齐现状） |
| Q86 | 预签名 | 为什么预签名直传？防伪造 key 怎么做？ | video Create 服务端分配 key | 上传带宽 |
| Q87 | 状态机 | uploading→processing→ready/failed | video biz | 转码状态机 |
| Q88 | FFmpeg | media worker 如何调 FFmpeg？产物布局？ | `app/media/internal/hls` | FFmpeg HLS |
| Q89 | HLS 403 | 为何 m3u8 预签名后 .ts 会 403？如何解 | MINIO_PUBLIC_BASE | 播放优化 |
| Q90 | JWT 统一 | 业务 JWT 如何打通 comet？ | core/jwtauth、comet | 弹幕复用 |
| Q91 | offset_ms | 点播弹幕进度字段如何贯通？ | message offset_ms | 历史回放 |
| Q92 | 互动 | 点赞如何防重复？表唯一键？ | video_likes uk | 互动 |
| Q93 | 网关 | gateway 提供了哪些生产能力？ | `app/gateway` | gateway |
| Q94 | 限流 | 网关 IP 限流算法是什么？局限？ | gateway rateLimiter | 限流 |
| Q95 | 配置 | 生产 JWT_SECRET 默认值如何拦截？ | user main IsProd | 安全 |

---

## K. 安全、生产、开放题（Q96–Q100）

| # | 网上问法 | 题目 | 代码/文档 | 简历 |
|---|----------|------|-----------|------|
| Q96 | 鉴权 | 恒定时间比较密码/token 防什么？ | middleware subtle | 安全 |
| Q97 | 密钥 | 密钥如何管理？.env 为何 gitignore | `.env.example` PRODUCTION.md | 工程 |
| Q98 | 容量规划 | 给 10 万在线直播间，如何估算机器？ | 压测数据外推 | 分布式 |
| Q99 | 故障演练 | Comet 挂一台用户感知？如何降级 | 多 comet + Job | 可靠性 |
| Q100 | 开放设计 | 若要做「大房间 10 万观众」下一刀改什么？ | 分级广播/分层 | 演进 |

---

## 刷题建议

1. **第一轮**：Q01–Q10 + Q55–Q62（故事与架构）  
2. **第二轮**：Q11–Q36（连接与并发，最容易深挖）  
3. **第三轮**：Q37–Q54（中间件与存储）  
4. **第四轮**：Q67–Q84（RPC/压测/审查，差异化）  
5. **第五轮**：Q85–Q100（点播 + 开放）  

对照答案：`ANSWERS.md` 同号。
