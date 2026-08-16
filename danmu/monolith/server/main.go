package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 命令行参数
	addr := flag.String("addr", ":8080", "HTTP listen address")
	serverID := flag.String("id", "srv1", "server ID")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis database")
	redisSharded := flag.Bool("redis-sharded", true, "跨机广播使用 Redis 7 sharded Pub/Sub（SPUBLISH/SSUBSCRIBE）；false 回退经典 Pub/Sub")
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("kafka-topic", "danmu-history", "Kafka topic name")
	mqMode := flag.String("mq", "both", "MQ mode: redis|kafka|both")
	pprofAddr := flag.String("pprof", ":6060", "pprof listen address")
	sessionTTLFlag := flag.Duration("session-ttl", 10*time.Minute, "会话令牌有效期，到期未 reauth 则断开长连接")
	chAddr := flag.String("clickhouse-addr", "", "ClickHouse 只读地址(native TCP)，配置后 /api/v1/history 才可用；空=禁用历史查询")
	chDatabase := flag.String("clickhouse-db", "default", "ClickHouse database")
	chUser := flag.String("clickhouse-user", "default", "ClickHouse username")
	chPassword := flag.String("clickhouse-password", "", "ClickHouse password")
	histSize := flag.Int("hist-size", 100, "每房间热历史条数（断线重连补发/进房拉最近 N 条）")
	histTTL := flag.Duration("hist-ttl", 5*time.Minute, "热历史 TTL，超时未写入视为新会话")
	flag.Parse()

	// 在启动任何连接前设置会话 TTL（运行期只读）
	sessionTTL = *sessionTTLFlag

	// 鉴权 token 从环境变量读取
	authToken := os.Getenv("DANMU_AUTH_TOKEN")
	if authToken == "" {
		authToken = "danmu-secret-token" // 开发默认值
		log.Println("[WARN] DANMU_AUTH_TOKEN not set, using default token")
	}

	log.Printf("[main] server=%s addr=%s mq=%s", *serverID, *addr, *mqMode)

	// 根 context，收到 SIGINT/SIGTERM 时 cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hub
	hub := NewHub(*serverID, *mqMode, ctx, cancel)
	hub.tokenIssuer = NewTokenIssuer(authToken)
	hub.hist = NewRoomHist(*histSize, *histTTL)
	go hub.hist.SweepLoop(ctx, time.Minute)
	go hub.Run()

	// Redis Pub/Sub（默认 sharded，需 Redis ≥7；Dragonfly 亦实现该命令族）
	if *mqMode == "redis" || *mqMode == "both" {
		redisHub, err := NewRedisHub(*redisAddr, *redisPassword, *redisDB, hub, ctx, defaultShardCount, *redisSharded)
		if err != nil {
			log.Printf("[WARN] Redis connection failed: %v, running without Redis", err)
		} else {
			hub.redisHub = redisHub
			// 启动订阅循环（sharded：订阅全部固定复用频道；经典：订阅 room:* pattern）
			go redisHub.SubscribeLoop()
			log.Printf("[main] Redis Pub/Sub enabled (sharded=%v)", *redisSharded)
			defer redisHub.Close()
		}
	}

	// Kafka Producer
	if *mqMode == "kafka" || *mqMode == "both" {
		brokers := strings.Split(*kafkaBrokers, ",")
		// 尝试创建 topic
		if err := EnsureTopic(brokers, *kafkaTopic, 10); err != nil {
			log.Printf("[WARN] Kafka topic creation failed: %v", err)
		}
		kafkaProd := NewKafkaProducer(brokers, *kafkaTopic, ctx)
		hub.kafkaProd = kafkaProd
		log.Println("[main] Kafka producer enabled")
		defer kafkaProd.Close()
	}

	// Worker Pool
	wp := NewWorkerPool(hub)
	wp.Start()

	// API
	api := NewAPI(hub, authToken)
	api.StartQPSTracker()

	// 可选：接入 ClickHouse 只读连接，启用 /api/v1/history 历史查询
	if *chAddr != "" {
		historyDB, err := NewHistoryDB(*chAddr, *chDatabase, *chUser, *chPassword)
		if err != nil {
			log.Printf("[WARN] ClickHouse history disabled: %v", err)
		} else {
			api.historyDB = historyDB
			log.Printf("[main] history query enabled via ClickHouse %s", *chAddr)
			defer historyDB.Close()
		}
	}

	mux := http.NewServeMux()
	api.SetupRoutes(mux)

	// 中间件链
	handler := wrapMiddleware(mux,
		corsMiddleware,
		requestIDMiddleware,
		loggingMiddleware,
		api.qpsMiddleware,
	)

	// HTTP Server
	server := &http.Server{
		Addr:         *addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// pprof
	go func() {
		log.Printf("[pprof] listening on %s", *pprofAddr)
		if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
			log.Printf("[pprof] error: %v", err)
		}
	}()

	// 启动 HTTP Server
	go func() {
		log.Printf("[main] HTTP server listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] server error: %v", err)
		}
	}()

	// 优雅退出：收到 SIGINT/SIGTERM 时 cancel 根 context
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[main] shutdown signal received")
	cancel()

	// 等待在途消息处理完（带超时）
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] server shutdown error: %v", err)
	}

	wp.Wait()
	fmt.Println("[main] server stopped gracefully")
}
