// track 服务入口。比 user 服务多三样东西：
//  1. 一个指向 user 服务的 gRPC 客户端（服务间通信）
//  2. 一个 asynq 客户端（向 media worker 投递异步任务）
//  3. JWT 中间件 + selector（只保护需要登录的两个接口）
package main

import (
	"context"
	"log"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	trackv1 "github.com/YansIlinta/wavehub-micro/api/track/v1"
	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/app/track/internal/biz"
	"github.com/YansIlinta/wavehub-micro/app/track/internal/data"
	mw "github.com/YansIlinta/wavehub-micro/app/track/internal/middleware"
	"github.com/YansIlinta/wavehub-micro/app/track/internal/service"
	"github.com/YansIlinta/wavehub-micro/pkg/env"
)

func main() {
	redisAddr := env.Get("REDIS_ADDR", "localhost:6379")

	db := data.NewDB(env.Get("PG_DSN",
		"host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable"))
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	// 连接 user 服务：现在用静态地址(环境变量)，上了 etcd 注册中心后这里换成 discovery:///wavehub.user
	userConn, err := kgrpc.DialInsecure(context.Background(),
		kgrpc.WithEndpoint(env.Get("USER_GRPC_ADDR", "localhost:9001")))
	if err != nil {
		log.Fatalf("连接 user 服务失败: %v", err)
	}
	defer userConn.Close()

	queue := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer queue.Close()

	storage := data.NewMinioStorage(
		env.Get("MINIO_ADDR", "localhost:9000"),
		env.Get("MINIO_ACCESS_KEY", "wavehub"),
		env.Get("MINIO_SECRET_KEY", "wavehub123"),
		env.Get("MINIO_BUCKET", "wavehub-audio"),
	)

	repo := data.NewTrackRepo(db, rdb)
	uc := biz.NewTrackUsecase(repo, userv1.NewUserClient(userConn), queue, storage)
	svc := service.NewTrackService(uc)

	// selector：JWT 只挂在"写"接口上，列表/详情匿名可看(和 B 站一样)
	authMw := selector.Server(mw.JWTAuth(env.Get("JWT_SECRET", "dev-only-change-me"))).
		Match(func(ctx context.Context, operation string) bool {
			return operation == trackv1.OperationTrackCreateTrack ||
				operation == trackv1.OperationTrackCompleteUpload
		}).Build()

	httpSrv := khttp.NewServer(
		khttp.Address(env.Get("HTTP_ADDR", ":8002")),
		khttp.Middleware(authMw),
	)
	// gRPC 端口只给内部服务(media 回写)用，不做用户鉴权；生产上靠内网隔离/mTLS 保护
	grpcSrv := kgrpc.NewServer(kgrpc.Address(env.Get("GRPC_ADDR", ":9002")))
	trackv1.RegisterTrackHTTPServer(httpSrv, svc)
	trackv1.RegisterTrackServer(grpcSrv, svc)

	app := kratos.New(
		kratos.Name("wavehub.track"),
		kratos.Server(httpSrv, grpcSrv),
	)
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
