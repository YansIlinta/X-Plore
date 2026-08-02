// video 服务：点播稿件 CRUD + 预签名上传 + 异步转码投递。
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

	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	videov1 "github.com/YansIlinta/wavehub-micro/api/video/v1"
	"github.com/YansIlinta/wavehub-micro/app/video/internal/biz"
	"github.com/YansIlinta/wavehub-micro/app/video/internal/data"
	mw "github.com/YansIlinta/wavehub-micro/pkg/authmw"
	"github.com/YansIlinta/wavehub-micro/app/video/internal/service"
	"github.com/YansIlinta/wavehub-micro/pkg/env"
)

func main() {
	redisAddr := env.Get("REDIS_ADDR", "localhost:6379")

	db := data.NewDB(env.Get("PG_DSN",
		"host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable"))
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	userConn, err := kgrpc.DialInsecure(context.Background(),
		kgrpc.WithEndpoint(env.Get("USER_GRPC_ADDR", "localhost:9001")))
	if err != nil {
		log.Fatalf("连接 user 服务失败: %v", err)
	}
	defer userConn.Close()

	queue := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer queue.Close()

	bucket := env.Get("MINIO_BUCKET", "xplore-video")
	// 公共读基址：浏览器播 HLS 时 m3u8 内相对 .ts 必须也能匿名 GET
	publicBase := env.Get("MINIO_PUBLIC_BASE", "http://localhost:9000/"+bucket)
	storage := data.NewMinioStorage(
		env.Get("MINIO_ADDR", "localhost:9000"),
		env.Get("MINIO_ACCESS_KEY", "wavehub"),
		env.Get("MINIO_SECRET_KEY", "wavehub123"),
		bucket,
		publicBase,
	)

	repo := data.NewVideoRepo(db, rdb)
	uc := biz.NewVideoUsecase(
		repo,
		userv1.NewUserClient(userConn),
		queue,
		storage,
		env.Get("DANMU_WS_URL", "ws://localhost:8080/ws"),
	)
	svc := service.NewVideoService(uc)

	// 可选 JWT：GetVideo / stats / comments 列表在有 token 时解析用户态
	optionalJWT := mw.OptionalJWTAuth(env.Get("JWT_SECRET", "dev-only-change-me"))
	authMw := selector.Server(mw.JWTAuth(env.Get("JWT_SECRET", "dev-only-change-me"))).
		Match(func(ctx context.Context, operation string) bool {
			return operation == videov1.OperationVideoCreateVideo ||
				operation == videov1.OperationVideoCompleteUpload ||
				operation == videov1.OperationVideoListMyVideos ||
				operation == videov1.OperationVideoPostDanmu ||
				operation == videov1.OperationVideoPostComment ||
				operation == videov1.OperationVideoToggleLike ||
				operation == videov1.OperationVideoToggleFavorite
		}).Build()
	optionalMw := selector.Server(optionalJWT).
		Match(func(ctx context.Context, operation string) bool {
			return operation == videov1.OperationVideoGetVideo ||
				operation == videov1.OperationVideoGetInteractStats ||
				operation == videov1.OperationVideoListComments
		}).Build()

	httpSrv := khttp.NewServer(
		khttp.Address(env.Get("HTTP_ADDR", ":8003")),
		khttp.Middleware(optionalMw, authMw),
	)
	grpcSrv := kgrpc.NewServer(kgrpc.Address(env.Get("GRPC_ADDR", ":9003")))
	videov1.RegisterVideoHTTPServer(httpSrv, svc)
	videov1.RegisterVideoServer(grpcSrv, svc)

	app := kratos.New(
		kratos.Name("xplore.video"),
		kratos.Server(httpSrv, grpcSrv),
	)
	log.Println("video service listening HTTP :8003 / gRPC :9003")
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
