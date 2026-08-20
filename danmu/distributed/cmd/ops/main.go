// ops 是 Danmu Ops Console 的后端：旁路观测服务，只做 read + aggregate。
// 数据源：etcd（服务发现）→ 各服务 *-http 观测端点（/health、/api/v1/stats、
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

	"github.com/YansIlinta/danmu-distributed/etcdreg"
	"github.com/YansIlinta/danmu-distributed/ops"
	web "github.com/YansIlinta/danmu-distributed/ops/web"
)

func main() {
	addr := flag.String("addr", ":7900", "Ops Console HTTP listen address")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "etcd 客户端端点(逗号分隔)")
	token := flag.String("token", "", "comet 观测 API 的 Bearer token（默认取 env DANMU_AUTH_TOKEN，再默认 danmu-secret-token）")
	kafkaBrokers := flag.String("kafka", "localhost:9092", "Kafka brokers（逗号分隔）；空字符串=不观测 Kafka")
	kafkaTopic := flag.String("kafka-topic", "danmu-broadcast", "广播 Kafka topic")
	poll := flag.Duration("poll", 2*time.Second, "采集周期")
	mock := flag.Bool("mock", false, "mock 模式：喂演示数据（响应带 mock:true，UI 显著标记）")
	loadtestBin := flag.String("loadtest-bin", "bin/loadtest", "loadtest 二进制路径（../monolith 构建）；不存在则压测功能不可用")
	serverBin := flag.String("server-bin", "bin/server", "monolith server 二进制路径（system-config sweep 受控进程用）；不存在则该能力不可用")
	dataDir := flag.String("data-dir", "./data", "Realtime Systems Lab 数据目录（experiments/ 与 sweeps/ 子目录存 JSON 记录）")
	repoDir := flag.String("repo-dir", "", "Git 仓库目录（采集实验的 commit/dirty 元数据；空=跳过 git 信息）")
	historyLimit := flag.Int("experiment-history", 200, "实验历史有界加载条数")
	sweepLimit := flag.Int("sweep-history", 100, "sweep 历史有界加载条数")
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("DANMU_AUTH_TOKEN")
	}
	if *token == "" {
		*token = "danmu-secret-token"
		log.Printf("[ops] WARN: using default token for instance probing; set -token or DANMU_AUTH_TOKEN")
	}

	col := ops.NewCollector(ops.Config{
		EtcdEndpoints: strings.Split(*etcdEndpoints, ","),
		EtcdTLS:       etcdreg.TLSFilesFromEnv(),
		Token:         *token,
		KafkaBrokers:  *kafkaBrokers,
		KafkaTopic:    *kafkaTopic,
		KafkaGroups:   []string{"danmu-job", "danmu-storage"},
		Poll:          *poll,
		Mock:          *mock,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	col.Run(ctx)

	lt := ops.NewLoadtestManager(*loadtestBin, *token, ctx)
	if !*mock {
		if st := lt.Status(); st["available"] == false {
			log.Printf("[ops] loadtest binary unavailable (%s): Load Test 页将显示不可用", *loadtestBin)
		}
	}

	// Realtime Systems Lab：旁路实验层（JSON 文件存储，绝不引入 DB / 绝不进消息主链）。
	store, err := ops.NewExperimentStore(*dataDir, *historyLimit)
	if err != nil {
		log.Fatalf("[ops] experiment store init failed: %v", err)
	}
	sweepStore, err := ops.NewSweepStore(*dataDir, *sweepLimit)
	if err != nil {
		log.Fatalf("[ops] sweep store init failed: %v", err)
	}
	// 受控 server 进程管理器（system-config sweep 用；不可用不影响其余功能）。
	spm := ops.NewServerProcessManager(*serverBin, *token, ctx)
	if !spm.Available() {
		log.Printf("[ops] controlled server binary unavailable (%s): system-config sweeps disabled", *serverBin)
	}
	em := ops.NewExperimentManagerFull(store, lt, *repoDir, col, spm, *token)
	sm := ops.NewSweepManager(sweepStore, em)

	api := ops.NewAPI(col, em).WithSweeps(sm)

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
		log.Printf("[ops] listening on %s (etcd=%s, kafka=%q, mock=%v)", *addr, *etcdEndpoints, *kafkaBrokers, *mock)
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
