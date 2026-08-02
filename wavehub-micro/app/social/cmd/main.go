// social 服务:关注/粉丝关系 + UP 主公开信息(V2 新增,见 ROADMAP-V2.md)。
// 用户名经 gRPC 调 user;本服务只拥有 user_follows 表。
package main

import (
	"context"
	"log"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	socialv1 "github.com/YansIlinta/wavehub-micro/api/social/v1"
	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/app/social/internal/biz"
	"github.com/YansIlinta/wavehub-micro/app/social/internal/data"
	"github.com/YansIlinta/wavehub-micro/app/social/internal/service"
	mw "github.com/YansIlinta/wavehub-micro/pkg/authmw"
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

	uc := biz.NewSocialUsecase(data.NewFollowRepo(db), userv1.NewUserClient(userConn))
	svc := service.NewSocialService(uc)

	secret := env.Get("JWT_SECRET", "dev-only-change-me")
	authMw := selector.Server(mw.JWTAuth(secret)).
		Match(func(ctx context.Context, operation string) bool {
			return operation == socialv1.OperationSocialToggleFollow
		}).Build()
	optionalMw := selector.Server(mw.OptionalJWTAuth(secret)).
		Match(func(ctx context.Context, operation string) bool {
			return operation == socialv1.OperationSocialGetProfile
		}).Build()

	httpSrv := khttp.NewServer(
		khttp.Address(env.Get("HTTP_ADDR", ":8004")),
		khttp.Middleware(optionalMw, authMw),
	)
	grpcSrv := kgrpc.NewServer(kgrpc.Address(env.Get("GRPC_ADDR", ":9004")))
	socialv1.RegisterSocialHTTPServer(httpSrv, svc)
	socialv1.RegisterSocialServer(grpcSrv, svc)

	app := kratos.New(
		kratos.Name("xplore.social"),
		kratos.Server(httpSrv, grpcSrv),
	)
	log.Println("social service listening HTTP :8004 / gRPC :9004")
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
