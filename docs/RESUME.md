# 简历

## 个人信息

- 姓名：[待填写]
- 电话：[待填写]
- 邮箱：[待填写]
- GitHub：[待填写]
- 工作年限：[X] 年
- 学历：[学校 / 专业 / 时间]
- 求职意向：Go 后端开发工程师 / 分布式系统工程师 / 高并发服务端工程师

---

## 技术栈

- **语言**：Go（主要）、熟悉 Python / C++ / JavaScript
- **网络/协议**：TCP/IP、HTTP、WebSocket、RPC、Protobuf/gRPC
- **消息中间件**：Kafka、Redis Pub/Sub
- **数据库**：ClickHouse、MySQL、SQLite
- **容器/运维**：Docker、Docker Compose、Nginx、Prometheus、pprof
- **工程能力**：高并发设计、性能调优、单元测试、代码审查、优雅关闭

---

## 工作经历

### [公司名] · [职位]
*时间：20XX.XX - 至今*

- [待填写：负责的业务、技术栈、核心产出]
- [待填写：性能优化、架构改造、团队协作等]

### [公司名/学校项目] · [职位]
*时间：20XX.XX - 20XX.XX*

- [待填写]

---

## 项目经历

### X-Plore — 高并发直播弹幕系统（单体 + goim 微服务双架构）

**项目描述**：为探索百万级长连接、低延迟实时广播场景下的架构设计，独立实现的类 B 站直播弹幕系统。支持多房间、多机部署、跨机实时广播、消息持久化与历史回放；同仓提供「单体基线」与「goim 式微服务」两套可运行架构，以压测数据对比不同规模下的扩展方案。

**技术栈**：Go、Gorilla WebSocket、gRPC/Protobuf、Redis Pub/Sub、Kafka（KRaft）、ClickHouse、Nginx、Docker、Prometheus

**主要职责**（STAR：问题 → 行动 → 量化结果）：

- **建连/广播锁竞争**：万级并发下全局锁使建连与广播互相阻塞 → 将 room→clients 映射拆为 **256 分片 RWMutex**，配合 Client 独立读写泵、慢客户端 sendCh 满丢弃保护 → **10K 并发连接**下注册/广播无锁瓶颈，压测全程 **0 读写错误**。
- **广播写放大**：每条弹幕逐连接写出，syscall 随扇出线性放大 → 设计 **CPU×4 Worker 池 + 100K 缓冲队列**，按条数阈值 / **10ms 时间窗**聚合批量广播，Redis 改为按房间批量 Pub/Sub → 10K 连接 / 1000 房间场景 **E2E P50 1.6ms、P90 5.3ms**。
- **持久化不拖慢实时路径**：落库同步写会阻塞广播 → 实时走 Redis、持久化走 Kafka 异步双路径，存储由 SQLite 单写者升级为 **ClickHouse MergeTree（按天分区）**；双路径重复投递用**全局 msg_id + 滑动窗口去重**解决 → 广播热路径零同步 IO，历史可回放。
- **单体到微服务演进**：高扇出压测 P90 恶化至 **1.6s**，定位瓶颈为 Redis Pub/Sub 全网扩散与下游写路径 → 按 goim 拆分 comet（接入）/ logic（无状态逻辑）/ job（Kafka 消费→服务发现→**定向 PushRoom**），自研 minirpc（注册发现 + 一致性哈希 + 熔断）→ 逻辑层可水平扩展，跨机广播不再全网扇出，链路集成测试全绿。
- **压测与可观测性**：自研 loadtest（**HDR Histogram** 高精度 E2E 延迟），完成 1K～10K 连接多场景压测；Prometheus + pprof 审查全链路 → 定位并修复 **8 个编号缺陷**（Close 并发竞态、令牌桶拆分写 ABA、Prometheus 高基数标签、consumer 丢数据窗口等，见 REVIEW.md）。

**项目成果**：

- 低扇出（10K 连接 / 1000 房间）：**E2E P50 1.6ms / P90 5.3ms / 错误 0**；goim comet 单测 200 连接 **P50 0.6ms / P99 1.7ms**。
- 高扇出对比数据（P90 1.6s + 丢弃）成为架构演进依据——**用压测驱动重构**而非为微服务而微服务。
- 规模：核心 Go 约 **5.7K 行** + 单元/集成测试约 **1.2K 行**；两套架构均 Docker Compose 一键启动。

---

## 其他项目

### X-Plore 点播平台 — 类 B 站视频网站（Kratos 微服务）
*技术栈：Go/Kratos、gRPC/Protobuf(buf)、PostgreSQL、Redis、MinIO(S3)、FFmpeg、React/TypeScript*

- **服务拆分**：按业务域拆 user / video / social / search / media **5 个微服务** + 自研 Go gateway（IP 限流、CORS、X-Request-Id、/metrics）；proto-first 一份契约同时生成 gRPC 与 REST 接口 → 服务间强类型调用，前端零胶水对接。
- **上传带宽优化**：视频经业务服务器中转会产生双倍带宽与内存压力 → 改为**预签名 URL 浏览器直传 MinIO**，业务服务器只发凭证 → 视频字节流量为 0；转码走异步任务队列（FFmpeg→HLS，uploading→processing→ready 状态机），失败可重试可追溯。
- **弹幕复用**：点播弹幕房间与稿件 ID 1:1 绑定，复用弹幕系统 comet 长连接层 → 实时弹幕 + 按播放进度(offset_ms)历史回放，一套连接层服务两个产品。
- **可演进的搜索**：以 Searcher 接口隔离实现，PG ILIKE 读旁路起步、预留 ES 演进位 → 拆库/换引擎不改服务契约。
- **成果**：业务 Go 约 **3.3K 行** + React/TS 前端约 **3.5K 行**（信息流、自定义 HLS 播放器含倍速/弹幕层/双全屏、搜索联想、关注/个人空间）；注册→投稿→转码→播放→弹幕→互动→搜索→关注**全链路真机验证通过**。

---

## 自我评价

- 熟悉高并发服务端设计与调优，能独立完成从架构设计、编码实现到压测验证的完整链路。
- 注重代码正确性与工程规范，重视代码审查、单元测试与性能基线。
- 对分布式系统、消息队列、RPC、服务发现有一定实践经验。

---

## 附件/链接

- X-Plore 项目仓库：`D:/X-Plore`（或上传至 GitHub 后替换为公开链接）
- PROJECT.md / README.md / DESIGN-goim.md / REVIEW.md / DEMO.md / EVOLUTION.md
- **面试题库（约 100 题）**：`D:/X-Plore/interview/`  
  - 题目+代码锚点：`interview/QUESTIONS.md`  
  - 参考答案：`interview/ANSWERS.md`  
  - 口述稿：`INTERVIEW.md`  
  - 学习目录副本：`D:/huang/Documents/27studyz/X-Plore-interview/`

---

> **提示**：本简历为模板，请替换 `[待填写]` 部分。如需英文版、一页纸精简版，或针对具体岗位调整侧重点，告诉我即可。  
> 刷题时：**先 QUESTIONS 再 ANSWERS**，每题尽量指到仓库真实路径。
