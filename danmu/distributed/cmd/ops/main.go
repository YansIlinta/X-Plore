// ops 是 Danmu Ops Console 的后端：旁路观测服务，只做 read + aggregate。
// 数据源：registry（服务发现）→ 各服务 *-http 观测端点（/health、/api/v1/stats、
// comet /metrics）→ Kafka consumer lag。它不参与消息链路，挂掉不影响弹幕系统。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YansIlinta/danmu-distributed/ops"
	web "github.com/YansIlinta/danmu-distributed/ops/web"
)

func main() {
	addr := flag.String("addr", ":7900", "Ops Console HTTP listen address")
	registryURL := flag.String("registry", "http://localhost:7350", "registry base URL")
	token := flag.String("token", "", "comet 观测 API 的 Bearer token（默认取 env DANMU_AUTH_TOKEN，再默认 danmu-secret-token）")
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers（逗号分隔）；空字符串=不观测 Kafka")
	kafkaTopic := flag.String("kafka-topic", "danmu-broadcast", "广播 Kafka topic")
	poll := flag.Duration("poll", 2*time.Second, "采集周期")
	mock := flag.Bool("mock", false, "mock 模式：喂演示数据（响应带 mock:true，UI 显著标记）")
	loadtestBin := flag.String("loadtest-bin", "bin/loadtest", "loadtest 二进制路径（../monolith 构建）；不存在则压测功能不可用")
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("DANMU_AUTH_TOKEN")
	}
	if *token == "" {
		*token = "danmu-secret-token"
		log.Printf("[ops] WARN: using default token for instance probing; set -token or DANMU_AUTH_TOKEN")
	}

	col := ops.NewCollector(ops.Config{
		RegistryURL:  *registryURL,
		Token:        *token,
		KafkaBrokers: *kafkaBrokers,
		KafkaTopic:   *kafkaTopic,
		KafkaGroups:  []string{"danmu-job", "danmu-storage"},
		Poll:         *poll,
		Mock:         *mock,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	col.Run(ctx)

	lt := ops.NewLoadtestManager(*loadtestBin, *token)
	if !*mock {
		if st := lt.Status(); st["available"] == false {
			log.Printf("[ops] loadtest binary unavailable (%s): Load Test 页将显示不可用", *loadtestBin)
		}
	}

	api := ops.NewAPI(col, lt)

	// 路由：/api/* 给观测 API，其余走内嵌前端（SPA fallback：未知路径回 index.html）。
	mux := http.NewServeMux()
	mux.Handle("/api/", api.Handler())
	assets := web.Assets()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		f, err := assets.Open(p)
		if err != nil {
			// SPA fallback：前端 hash 路由用不到，但刷新/直链路径时兜底
			r.URL.Path = "/"
			http.FileServer(assets).ServeHTTP(w, r)
			return
		}
		f.Close()
		http.FileServer(assets).ServeHTTP(w, r)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("[ops] listening on %s (registry=%s, kafka=%q, mock=%v)", *addr, *registryURL, *kafkaBrokers, *mock)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[ops] serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[ops] shutting down")
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shCtx)
}
