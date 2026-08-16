// comet 是 goim 式架构的连接层：维持 WebSocket 长连接与房间-连接映射，
// 上行弹幕经 gRPC 转发给 Logic（一致性哈希按 roomID 路由），并暴露 Comet.PushRoom
// 供 Job 把消息回推后本机广播。启动时向 registry 注册 "comet" 服务供 Job 发现。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
	"github.com/YansIlinta/danmu-distributed/registry"
)

// uplinkMsg 一条待转发给 Logic 的上行弹幕。
type uplinkMsg struct {
	uid, roomID, content string
	clientTS, clientTSN  int64
	offsetMS             int64
}

func maskSecret(s string) string {
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}

// makeCheckOrigin: 空=开发允许全部；逗号分隔白名单用于生产。
func makeCheckOrigin(raw string) func(*http.Request) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "*" {
		return func(*http.Request) bool { return true }
	}
	allow := make(map[string]bool)
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			allow[p] = true
		}
	}
	return func(r *http.Request) bool {
		o := r.Header.Get("Origin")
		if o == "" {
			return true // 非浏览器客户端
		}
		return allow[o]
	}
}

type comet struct {
	pb.UnimplementedCometServiceServer
	id         string
	hub        *core.Hub
	logics     *logicPool
	authToken  string
	jwtSecret  string // 业务 JWT 校验密钥（JWT_SECRET）；空则仅 secret 鉴权
	upgrader   websocket.Upgrader
	startTime  time.Time
	uplinkCh   chan uplinkMsg
	standalone bool
	ctx        context.Context // 进程生命周期 ctx：worker 随它退出

	// standalone 本地模式用：无 logic 时 comet 自己过滤+生成 msg_id+本机广播
	filter   *core.SensitiveFilter
	msgIDSeq atomic.Uint64

	qpsCount      atomic.Int64
	lastSecondQPS atomic.Int64
	droppedUplink atomic.Int64

	tracer *core.TraceRecorder
}

// PushRoom 是 Job 调用的 gRPC：把一批消息广播给本机该房间的连接。
func (c *comet) PushRoom(ctx context.Context, req *pb.PushRoomReq) (*pb.PushRoomResp, error) {
	delivered := c.hub.BroadcastToRoom(req.RoomId, req.Payload)
	if delivered > 0 {
		core.MetricMsgOut(delivered)
	}
	// job 把本批命中采样的 msg_id 放在 metadata 里，这里直接记投递结果，
	// 不必解析 payload。delivered=0 是有意义的信息：本机没有该房间的连接。
	if c.tracer.Enabled() {
		if ids := traceIDsFromCtx(ctx); len(ids) > 0 {
			now := time.Now().UnixNano()
			detail := "delivered=" + strconv.Itoa(delivered)
			for _, id := range ids {
				c.tracer.Record(id, core.HopCometDeliver, req.RoomId, detail, now)
			}
		}
	}
	return &pb.PushRoomResp{Delivered: int32(delivered)}, nil
}

// traceIDsFromCtx 从 gRPC metadata 取本批采样的 msg_id 列表。
func traceIDsFromCtx(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	vals := md.Get(core.TraceMetadataKey)
	if len(vals) == 0 || vals[0] == "" {
		return nil
	}
	return strings.Split(vals[0], ",")
}

// onUplink 是注入给 hub 的回调：readPump 收到弹幕后非阻塞入队，满则丢弃计数。
func (c *comet) onUplink(uid, roomID, content string, clientTS, clientTSN, offsetMS int64) {
	select {
	case c.uplinkCh <- uplinkMsg{uid, roomID, content, clientTS, clientTSN, offsetMS}:
	default:
		c.droppedUplink.Add(1) // 上行队列满，丢弃（削峰保护）
	}
}

// uplinkWorker 消费上行队列：分布式→转发 Logic；standalone→本机过滤+广播。
// ctx 取消时退出（优雅关闭不留残留 goroutine）。
func (c *comet) uplinkWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case m := <-c.uplinkCh:
			c.handleUplink(m)
		}
	}
}

func (c *comet) handleUplink(m uplinkMsg) {
	if c.standalone {
		c.localBroadcast(m)
		return
	}
	client, ok := c.logics.forRoom(m.roomID)
	if !ok {
		return // 无可用 logic，丢弃（弹幕允许丢）
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	start := time.Now()
	resp, err := client.OnMessage(ctx, &pb.OnMessageReq{
		RoomId: m.roomID, Uid: m.uid, Content: m.content,
		ClientTs: m.clientTS, ClientTsNano: m.clientTSN, SourceComet: c.id,
		OffsetMs: m.offsetMS,
	})
	cancel()
	if err != nil {
		log.Printf("[comet] logic OnMessage error: %v", err)
		return
	}
	// msg_id 由 logic 生成，所以上行 span 只能等 RPC 返回后补记。
	// 采样判定用同一套确定性哈希，与 logic 的结论必然一致。
	if c.tracer.Sampled(resp.MsgId) {
		c.tracer.Record(resp.MsgId, core.HopCometUplink, m.roomID,
			"logic_rtt_ms="+strconv.FormatInt(time.Since(start).Milliseconds(), 10),
			time.Now().UnixNano())
	}
}

// localBroadcast standalone 模式：不经 logic/kafka，本机过滤+生成 msg_id+广播。
func (c *comet) localBroadcast(m uplinkMsg) {
	msg := core.Message{
		Type:         "danmu",
		MsgID:        c.id + "-" + strconv.FormatUint(c.msgIDSeq.Add(1), 10),
		RoomID:       m.roomID,
		UID:          m.uid,
		Content:      c.filter.Filter(m.content),
		ClientTS:     m.clientTS,
		ClientTSNano: m.clientTSN,
		ServerTS:     time.Now().UnixMilli(),
		SourceServer: c.id,
		OffsetMS:     m.offsetMS,
	}
	data, err := json.Marshal([]*core.Message{&msg})
	if err != nil {
		return
	}
	n := c.hub.BroadcastToRoom(m.roomID, data)
	if n > 0 {
		core.MetricMsgOut(n)
	}
	core.ObserveBroadcast(time.Since(time.UnixMilli(msg.ServerTS)).Seconds())
}

// handleWebSocket WS 升级入口：鉴权 → 建连 → 注册 → 下发会话令牌。
// 鉴权双模（M2）：
//  1. token == DANMU_AUTH_TOKEN → 压测/联调 secret（loadtest 兼容）
//  2. 否则按业务 JWT（HS256, claim uid）校验；成功则强制用 JWT 内 uid（防冒充）
func (c *comet) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	roomID := r.URL.Query().Get("room")
	token := r.URL.Query().Get("token")
	if roomID == "" {
		http.Error(w, "missing room", http.StatusBadRequest)
		return
	}
	if token == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	authed := false
	if c.authToken != "" && token == c.authToken {
		authed = true // 压测 secret
		if uid == "" {
			http.Error(w, "missing uid", http.StatusBadRequest)
			return
		}
	} else if c.jwtSecret != "" {
		jwtUID, err := core.VerifyBusinessJWT(token, c.jwtSecret)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// 业务 JWT：以 claim 为准，忽略/覆盖 query uid 防伪造
		uid = jwtUID
		authed = true
	}
	if !authed {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if uid == "" {
		http.Error(w, "missing uid", http.StatusBadRequest)
		return
	}

	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := core.NewClient(c.hub, conn, uid, roomID, c.hub.Context())
	c.hub.AddClient(client)
	go client.WritePump()
	go client.ReadPump()

	if c.hub.TokenIssuer != nil {
		st, exp := c.hub.TokenIssuer.Issue(uid, roomID, core.SessionTTL)
		payload, _ := json.Marshal([]map[string]any{{"type": "session_token", "token": st, "expires_at": exp.UnixMilli()}})
		client.TrySend(payload)
	}
}

func main() {
	wsAddr := flag.String("ws-addr", ":8080", "WebSocket/HTTP listen address")
	rpcAddr := flag.String("rpc-addr", ":7500", "gRPC listen address (Job 调用 PushRoom)")
	advertise := flag.String("advertise", "", "对外可达的 gRPC 地址(host:port)，默认由 rpc-addr 推导")
	advertiseHTTP := flag.String("advertise-http", "", "对外可达的 HTTP 观测地址(host:port)，默认由 advertise 主机名 + ws-addr 端口推导")
	id := flag.String("id", "comet1", "comet instance id")
	registryURL := flag.String("registry", "", "registry base URL；配置后注册 comet 并发现 logic")
	logicAddrs := flag.String("logic", "", "logic gRPC 地址静态列表(逗号分隔)；留空则走 registry 发现")
	pprofAddr := flag.String("pprof", "", "pprof listen address；默认空=关闭（生产勿开，会暴露运行时信息）")
	sessionTTL := flag.Duration("session-ttl", 10*time.Minute, "会话令牌有效期")
	traceRate := flag.Uint("trace-sample", 100, "消息 trace 采样率：1/N 采样，0=关闭。须与 logic 一致才有意义")
	traceBuf := flag.Int("trace-buffer", 512, "trace span 环形缓冲条数")
	flag.Parse()

	core.SessionTTL = *sessionTTL
	authToken := os.Getenv("DANMU_AUTH_TOKEN")
	if authToken == "" {
		authToken = "danmu-secret-token"
		log.Println("[WARN] DANMU_AUTH_TOKEN not set, using default")
	}
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "dev-only-change-me"
		log.Println("[WARN] JWT_SECRET not set, using default")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := core.NewHub(*id, ctx)
	hub.TokenIssuer = core.NewTokenIssuer(authToken)

	var staticLogic []string
	if *logicAddrs != "" {
		staticLogic = strings.Split(*logicAddrs, ",")
	}
	standalone := *logicAddrs == "" && *registryURL == ""

	c := &comet{
		id:         *id,
		hub:        hub,
		logics:     newLogicPool(*registryURL, staticLogic),
		authToken:  authToken,
		jwtSecret:  jwtSecret,
		startTime:  time.Now(),
		uplinkCh:   make(chan uplinkMsg, 100000),
		standalone: standalone,
		ctx:        ctx,
		filter:     core.NewSensitiveFilter(core.DefaultSensitiveWords),
		tracer:     core.NewTraceRecorder(*id, uint32(*traceRate), *traceBuf),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     makeCheckOrigin(os.Getenv("WS_ALLOWED_ORIGINS")),
		},
	}
	// msg_id 序列用进程启动时刻播种：standalone 模式下重启后 seq 归零也不会与旧 msg_id 重复。
	c.msgIDSeq.Store(uint64(time.Now().UnixNano()))
	log.Printf("[comet] WS auth: secret=%q + business JWT (JWT_SECRET set)", maskSecret(authToken))
	if o := os.Getenv("WS_ALLOWED_ORIGINS"); o != "" {
		log.Printf("[comet] WS CheckOrigin whitelist=%s", o)
	} else {
		log.Println("[comet] WS CheckOrigin=allow all (set WS_ALLOWED_ORIGINS for prod)")
	}
	hub.Uplink = c.onUplink

	if standalone {
		log.Println("[comet] standalone mode: 本机过滤+广播，不经 logic/kafka/job")
	}

	// 上行 worker 池
	for i := 0; i < runtime.NumCPU(); i++ {
		go c.uplinkWorker()
	}
	// 定期刷新 logic 列表
	if !standalone && *logicAddrs == "" {
		go func() {
			t := time.NewTicker(3 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					c.logics.refresh()
				}
			}
		}()
	}
	c.startQPSTracker(ctx)

	// gRPC server（Job → Comet.PushRoom）
	grpcSrv := grpc.NewServer()
	pb.RegisterCometServiceServer(grpcSrv, c)
	rpcLis, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("[comet] rpc listen: %v", err)
	}
	go func() {
		if err := grpcSrv.Serve(rpcLis); err != nil && err != grpc.ErrServerStopped {
			// ErrServerStopped 是 GracefulStop 后的正常返回，不能当致命错误（同 logic）
			log.Fatalf("[comet] rpc serve: %v", err)
		}
	}()

	// 注册到 registry 供 Job 发现（注册的是 gRPC 地址）
	if *registryURL != "" {
		go registry.KeepAlive(ctx, *registryURL, "comet", advertiseAddr(*advertise, *rpcAddr), 10*time.Second)
		// 额外注册 comet-http（HTTP 观测地址），供 Ops Console 经 registry 发现
		go registry.KeepAlive(ctx, *registryURL, "comet-http", advertiseHTTPAddr(*advertiseHTTP, *advertise, *wsAddr), 10*time.Second)
	}

	// HTTP/WS server + REST admin
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/ws", c.handleWebSocket)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/api/v1/stats", c.auth(c.handleStats))
	mux.HandleFunc("/api/v1/rooms", c.auth(c.handleRooms))
	mux.HandleFunc("/api/v1/traces", c.auth(c.handleTraces))
	mux.HandleFunc("/api/v1/session-token", c.auth(c.handleSessionToken))
	mux.Handle("/", http.FileServer(http.Dir("web")))

	httpSrv := &http.Server{Addr: *wsAddr, Handler: c.qpsMW(mux), ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
	go func() {
		log.Printf("[comet] id=%s ws=%s rpc=%s standalone=%v", *id, *wsAddr, *rpcAddr, standalone)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[comet] http serve: %v", err)
		}
	}()
	if *pprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("[comet] pprof serve: %v", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[comet] shutting down")
	cancel()
	shutCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	httpSrv.Shutdown(shutCtx)
	grpcSrv.GracefulStop()
}

func advertiseAddr(advertise, listen string) string {
	if advertise != "" {
		return advertise
	}
	if len(listen) > 0 && listen[0] == ':' {
		return "localhost" + listen
	}
	return listen
}

// advertiseHTTPAddr 推导 HTTP 观测地址：显式 advertise-http 优先；
// 否则取 advertise 的主机名 + ws-addr 的端口；advertise 为空则主机名用 localhost。
func advertiseHTTPAddr(advertiseHTTP, advertise, wsAddr string) string {
	if advertiseHTTP != "" {
		return advertiseHTTP
	}
	host := "localhost"
	if advertise != "" {
		if h, _, err := net.SplitHostPort(advertise); err == nil && h != "" {
			host = h
		}
	}
	if _, port, err := net.SplitHostPort(wsAddr); err == nil && port != "" {
		return net.JoinHostPort(host, port)
	}
	return host
}
