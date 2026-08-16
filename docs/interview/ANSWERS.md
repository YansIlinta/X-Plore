# X-Plore 面试题参考答案

> 与 [QUESTIONS.md](./QUESTIONS.md) 同号。口述请用自己的话，并尽量指到仓库路径。  
> 更完整口述稿见 `../INTERVIEW.md`。

---

## A. Q01–Q10

**Q01**  
直播弹幕场景，目标百万级 WS 长连接、多房间实时广播。Go 实现：单体用分片 Hub + worker 批量 + Redis 实时 + Kafka→ClickHouse 持久化；演进 goim 式 Comet/Logic/Job。低扇出 10K 连接 E2E P90 ~5ms。点播侧另有 Kratos+MinIO+FFmpeg，房间 ID=稿件 ID 复用 comet。

**Q02**  
单体路径短，方便讲清长连接与压测定位；微服务对齐连接/逻辑/扇出不同扩展轴，解决 Redis Pub/Sub 规模问题。两套可跑、可对比，面试展示「压测驱动演进」而非为拆而拆。见 `PROJECT.md` §2。

**Q03**  
独立实现连接层、广播、中间件接入、goim 拆分、minirpc、loadtest、审查修复；中间件用 Redis/Kafka/CH/Nginx 生态。点播 user/video/media/gateway/React 同仓。

**Q04（示例 STAR：高扇出）**  
S：10K 连接高扇出 P90 到秒级且有丢弃。T：定位是否架构天花板。A：对照 Redis 全网扩散+写路径；修压测工具假错误；规划 Job 定向推。R：低扇出保持 ms 级；演进方案有数据支撑。详见 `REVIEW.md` + 压测表。

**Q05**  
环境如 AutoDL 大核；指标 E2E P50/P90、错误、丢弃；低扇出 10K/1000 房 P50 1.6ms P90 5.3ms 错误 0。前提：压测机与业务分机、注意工具精度（纳秒透传）。`danmu/monolith/loadtest/` + `PROJECT.md` §5。

**Q06**  
直播弹幕时效优先，慢客户端不能拖垮全房间；sendCh 满丢、限流丢是有意。IM 要可靠投递，语义不同。可靠诉求走 Kafka 落库与补偿，不进热路径强一致。

**Q07**  
借鉴 goim 三层与「Job 推 Comet」思想；自研 minirpc、ClickHouse 落库、保留单体基线、点播业务绑定。非官方 B 站代码 fork。

**Q08**  
未声称已上生产百万日活；控制面踢人/关房跨机不完整；registry 非 etcd；单机百万连接未做内核级优化；点播需 Docker+ffmpeg 真机转码。

**Q09**  
`room_id = strconv.FormatUint(video_id,10)`；播放页 JWT 连 comet；历史可用 PG `video_danmus`+offset_ms。`EVOLUTION.md`。

**Q10**  
WS 全双工低延迟适合推送；SSE 偏单向；长轮询延迟与连接开销差。Go 高并发与运维生态合适。

---

## B. Q11–Q20

**Q11**  
通常 readPump + writePump 两 goroutine：读解包上行，写唯一出口保证无并发写 conn。`danmu/distributed/core/client.go`。

**Q12**  
TrySend 非阻塞，满则丢并计数。弹幕允许丢；阻塞会反压到广播路径拖死房间。

**Q13**  
写循环心跳/ping；session 到期主动 close（如 4008）；reauth 续期。`SessionTTL` 可配置。

**Q14**  
query token：① 等于 `DANMU_AUTH_TOKEN`（压测）；② HS256 JWT，`uid` claim 强制覆盖。`danmu/distributed/comet/main.go` + `danmu/distributed/core/jwtauth.go`。

**Q15**  
生产 `WS_ALLOWED_ORIGINS` 白名单；空则开发全放。防 CSRF 跨站 WS。

**Q16**  
Upgrade 头；Nginx 需 HTTP/1.1、Upgrade、Connection、长 timeout。gateway 反代 `/ws` 同理。

**Q17**  
同一用户尽量落同机，利于会话与本地房间局部性。CDN 后 remote_addr 是节点 IP，`ip_hash` 失效，故用 `X-Forwarded-For` 一致性哈希。`nginx.conf`。

**Q18**  
瓶颈：fd、conn 内存、goroutine 栈、epoll。本项目验证万级并讨论演进，未宣称已内核调到百万单机。

**Q19**  
AddClient/RemoveClient 按 room 哈希分片加锁，直接调用、无单 goroutine 串行注册（REVIEW D1）。

**Q20**  
上行→msgQueue→worker 批量→本机 Hub.Broadcast + Redis Pub + Kafka → 他机 Sub 再广播。

---

## C. Q21–Q30

**Q21**  
256 平衡锁数量与 map 粒度；分片键 roomID hash（fnv）。`danmu/distributed/core/hub.go`。

**Q22**  
进房/广播/退房抢同一把锁，建连延迟尖刺、吞吐上不去。

**Q23**  
多处 Close/写 conn 竞态；需 once、状态位、仅 writePump 写。见 REVIEW。

**Q24**  
通常 writePump 退出路径关闭；用 sync.Once 或标志防 double close。

**Q25–Q26**  
令牌与时间打包进一个 uint64，单次 CAS 更新，避免拆字段 ABA。`danmu/distributed/core/ratelimit.go`。

**Q27**  
有 Acquire/Release Message 池；热路径频繁分配可降 GC，但要用对生命周期。

**Q28**  
signal→cancel context→停 accept、关连接、flush。

**Q29**  
多 worker 共享队列，同房消息可能不同 worker；弹幕可接受。点播历史按 offset 排序。

**Q30**  
`go test -race`；审查修复 Close、限流等。

---

## D. Q31–Q36

**Q31**  
上行 channel 大批量（如 100K）；满丢弃并 metrics，削峰。

**Q32**  
条数阈值或 ~10ms 窗：降 syscall、略增延迟。可配置。

**Q33**  
减少写系统调用与锁竞争次数，P90 常因尾部排队改善。

**Q34**  
无 logic 时本地过滤+msg_id+BroadcastToRoom，便于无中间件冒烟。

**Q35**  
Logic 或 standalone 本地 AC 过滤；多模式匹配高效。`danmu/distributed/core/filter.go`。

**Q36**  
Logic（或 standalone comet）生成 `instance-seq`；前端滑动窗口去重。

---

## E. Q37–Q48

**Q37**  
Redis：低延迟实时跨机；Kafka：持久、回放、多消费组。单用都不完整。

**补充 · 单体里 Redis 的意义（高频）**  
- **不是缓存/主库**，是多实例 server 之间的 **Pub/Sub 实时广播总线**。  
- 路径：本机 `BroadcastToRoom` + `Redis Pub room:{id}` → 其他 server Sub → 再本机广播；Kafka 另路落库。  
- **单机**可不启 Redis（降级本机广播）；**多机**没有就会出现「半个房间收不到」。  
- 局限：Cluster 下易全网扩散 → 才演进 goim（Kafka + Job 定向 PushRoom）。  
- 代码：`danmu/monolith/server/redis.go`；文档：`PROJECT.md` §3.1、§4.3。

**Q38**  
Cluster 下 Pub/Sub 消息扩散到各节点，加机器不线性提升广播能力。

**Q39**  
按 channel 含 room、本机无房间则跳过反序列化。REVIEW M6。

**Q40**  
key=roomID → 同房同分区，利于局部顺序。

**Q41**  
不同 GroupID 独立消费同 topic，落库与广播解耦。

**Q42**  
关自动提交；Fetch→落库成功→Commit。REVIEW M3。

**Q43**  
至少一次 + msg_id 去重/幂等表；CH 可接受极少重复。

**Q44**  
Kafka 非在线广播总线，延迟与 fanout 模型不适合纯实时弹幕。

**Q45**  
削峰 + 只推持有房间的 Comet，避免全网扩散。

**Q46**  
延迟升高、可水平加 Job；需监控 lag。

**Q47**  
分区内有序；跨 worker/跨路径仍可能乱序。

**Q48**  
用户先看实时；历史稍后一致；接受短暂差。

---

## F. Q49–Q54

**Q49**  
写多读少、时序、批量插入；MergeTree 适合分析型历史。MySQL 行存写放大。

**Q50**  
按天分区；ORDER BY (room_id, server_ts) 匹配按房时间查。

**Q51**  
小批量拖垮 merge；事务内攒批一次提交。

**Q52**  
server 未注入 historyDB；修：可选 CH 只读或 501。REVIEW H2。

**Q53**  
点播弹幕量级按稿件可控，PG 事务与业务同库简单；直播海量走 CH。

**Q54**  
热：内存连接+Redis；冷：CH/PG 查询。

---

## G. Q55–Q66

**Q55**  
Comet 吃连接与 fd；Logic 吃 CPU 过滤；Job 吃消费与 RPC 扇出——扩展轴不同。

**Q56**  
client→comet WS→Logic.OnMessage→Kafka→job→Comet.PushRoom→本机广播。

**Q57**  
gRPC/minirpc PushRoom(room, payload)；无房间 delivered=0。

**Q58**  
与全网统一路径一致，避免双写本地+回路重复逻辑；msg_id 去重。

**Q59**  
无连接状态，多副本无脑扩；按 room 哈希可亲和。

**Q60**  
连接与房间映射在 Comet；挂了用户重连其他实例（哈希可能变）。

**Q61**  
无 Kafka 也能演示 WS 广播与压测。

**Q62**  
团队与流量让「同进程扩缩」痛苦时；有数据证明瓶颈。本项目用高扇出数据驱动。

**Q63–Q64**  
videos 仅 video 服务写；media 只 gRPC ReportProcessed。跨库直写=分布式单体。

**Q65**  
要立即结果→同步 RPC；几十秒转码→队列异步。

**Q66**  
先 Gin 单体跑通业务，再 Kratos 拆，对比成本。

---

## H. Q67–Q74

**Q67**  
学习发现/LB/熔断完整链路；体量可控。生产可换标准 gRPC。

**Q68**  
HTTP 注册+心跳 TTL；过期剔除。`danmu/distributed/minirpc/registry`。

**Q69**  
roomID 哈希选 Logic 实例，便于房间级限流。`logicpool`。

**Q70**  
失败计数→open→半开探测→关闭；保护下游。`danmu/distributed/minirpc/breaker`。

**Q71**  
Kratos etcd Registrar 或 K8s Service DNS；Job 改发现源。

**Q72**  
浏览器 REST；内部二进制契约、多语言、流式。proto-first 一份双生成。

**Q73**  
room/uid/content/ts/source/offset_ms；点播进度。

**Q74**  
Nginx：用户接入粘连；minirpc：服务间选实例。

---

## I. Q75–Q84

**Q75**  
连接数、消息 in/out、广播延迟、队列长度等。`danmu/distributed/core/metrics.go`。

**Q76**  
房间基数爆炸撑爆 TSDB 与进程 map。

**Q77**  
独立 pprof 端口；看 CPU/heap/block 定位热点。

**Q78**  
多连接发收；透传时间戳；HDR 分位报告。

**Q79**  
毫秒分辨率把亚毫秒 clamp 成误导。

**Q80**  
主动 close 被计成读错误；closing 标志区分。

**Q81**  
万级连接争用一把锁；改分片直方图再 merge。

**Q82**  
维护每秒差值而非累计。

**Q83**  
CPU 互抢导致延迟虚高；应分机。

**Q84**  
按正确性/扩展/可观测列优先级；例：H1 高基数、M3 offset、Close 竞态。

---

## J. Q85–Q95

**Q85**  
user 身份稳定；video 业务迭代快；media CPU 密集独立扩。现状代码：user/video/media+gateway（简历若写 5 服务需与仓一致：social/search 可作规划或已扩展）。

**Q86**  
避免业务机扛双倍带宽；key 服务端分配 `videos/{id}/original` 防覆盖。

**Q87**  
Create uploading→Complete processing→Report ready/failed。

**Q88**  
下载原片→ffmpeg HLS→上传 hls/ 与 cover→gRPC 回写。

**Q89**  
签名仅覆盖 m3u8 对象；相对路径 ts 无签名。解：HLS 前缀公共读或播放代理。`MINIO_PUBLIC_BASE`。

**Q90**  
同一 JWT_SECRET；comet VerifyBusinessJWT。

**Q91**  
上行 JSON offset_ms→Message→PG 历史；播放页按进度窗口展示。

**Q92**  
(user_id, video_id) 唯一索引，Toggle 删/插。

**Q93**  
路由、CORS、限流、Request-Id、metrics、安全头。

**Q94**  
固定窗口每 IP 每秒 N 次；非令牌桶，突发不友好。

**Q95**  
`APP_ENV=prod` 且默认 secret 则 Fatal。

---

## K. Q96–Q100

**Q96**  
防计时侧信道猜 token。

**Q97**  
环境变量/密钥管理；.env 不进 Git；见 PRODUCTION.md。

**Q98**  
用单机压测连接密度与扇出曲线外推；预留 CPU 给广播与 GC；分 Comet 池。

**Q99**  
该机用户断线重连；Job 推其他 Comet；会话重新建立。

**Q100**  
分级广播（房间分片/代理节点）、降级采样弹幕、热点房间独立集群、写扩散改订阅树等——结合数据再拆。

---

## 口述万能句

1. **指标**：连接数、E2E 延迟分位、错误/丢弃，而不是「绝不丢」。  
2. **取舍**：延迟 vs 可靠 vs 成本，三点不能同时最优。  
3. **证据**：压测数字 + REVIEW 缺陷编号 + 代码路径。  
4. **边界**：生产还差 etcd/多机房/内核调优——主动说加分。  

祝面试顺利。刷题请回到 [QUESTIONS.md](./QUESTIONS.md)。
