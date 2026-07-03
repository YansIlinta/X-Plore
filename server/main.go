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
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	kafkaTopic := flag.String("kafka-topic", "danmu-history", "Kafka topic name")
	mqMode := flag.String("mq", "both", "MQ mode: redis|kafka|both")
	pprofAddr := flag.String("pprof", ":6060", "pprof listen address")
	flag.Parse()

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
	go hub.Run()

	// Redis Pub/Sub
	if *mqMode == "redis" || *mqMode == "both" {
		redisHub, err := NewRedisHub(*redisAddr, *redisPassword, *redisDB, hub, ctx)
		if err != nil {
			log.Printf("[WARN] Redis connection failed: %v, running without Redis", err)
		} else {
			hub.redisHub = redisHub
			// 启动 Redis 模式订阅（订阅所有房间频道）
			go redisHub.SubscribePattern()
			log.Println("[main] Redis Pub/Sub enabled")
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
