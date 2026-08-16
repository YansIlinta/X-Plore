// search 服务:标题/简介检索 + 搜索联想(V2 新增,见 ROADMAP-V2.md)。
// 共库只读 videos 表(读旁路);演进为 ES 时只换 data 层实现。
package main

import (
	"context"
	"log"

	"github.com/go-kratos/kratos/v2"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	searchv1 "github.com/YansIlinta/wavehub-micro/api/search/v1"
	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/app/search/internal/biz"
	"github.com/YansIlinta/wavehub-micro/app/search/internal/data"
	"github.com/YansIlinta/wavehub-micro/app/search/internal/service"
	"github.com/YansIlinta/wavehub-micro/pkg/env"
)

func main() {
	db := data.NewDB(env.Get("PG_DSN",
		"host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable"))

	userConn, err := kgrpc.DialInsecure(context.Background(),
		kgrpc.WithEndpoint(env.Get("USER_GRPC_ADDR", "localhost:9001")))
	if err != nil {
		log.Fatalf("连接 user 服务失败: %v", err)
	}
	defer userConn.Close()

	bucket := env.Get("MINIO_BUCKET", "xplore-video")
	coverBase := env.Get("MINIO_PUBLIC_BASE", "http://localhost:9000/"+bucket)

	uc := biz.NewSearchUsecase(data.NewPGSearcher(db), userv1.NewUserClient(userConn), coverBase)
	svc := service.NewSearchService(uc)

	httpSrv := khttp.NewServer(khttp.Address(env.Get("HTTP_ADDR", ":8005")))
	grpcSrv := kgrpc.NewServer(kgrpc.Address(env.Get("GRPC_ADDR", ":9005")))
	searchv1.RegisterSearchHTTPServer(httpSrv, svc)
	searchv1.RegisterSearchServer(grpcSrv, svc)

	app := kratos.New(
		kratos.Name("xplore.search"),
		kratos.Server(httpSrv, grpcSrv),
	)
	log.Println("search service listening HTTP :8005 / gRPC :9005")
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
