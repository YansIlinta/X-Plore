// user 服务入口。一个 Kratos 服务 = 同一份 service 实现，同时挂到 HTTP(给前端) 和 gRPC(给其他服务)。
// 说明：官方 `kratos new` 模板用 wire 做依赖注入、用 proto 定义配置文件；
// 这里刻意手工组装 + 环境变量配置，先看清楚每根线怎么接，wire 等熟练后再上（见 MICROSERVICES.md）。
package main

import (
	"log"

	"github.com/go-kratos/kratos/v2"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	v1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/app/user/internal/biz"
	"github.com/YansIlinta/wavehub-micro/app/user/internal/data"
	"github.com/YansIlinta/wavehub-micro/app/user/internal/service"
	"github.com/YansIlinta/wavehub-micro/pkg/env"
)

func main() {
	// 组装顺序永远是 data → biz → service → server
	jwtSecret := env.Get("JWT_SECRET", "dev-only-change-me")
	if env.IsProd() && jwtSecret == "dev-only-change-me" {
		log.Fatal("APP_ENV=prod 时必须设置强随机 JWT_SECRET，禁止使用默认值")
	}
	db := data.NewDB(env.Get("PG_DSN",
		"host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable"))
	repo := data.NewUserRepo(db)
	uc := biz.NewUserUsecase(repo, jwtSecret)
	svc := service.NewUserService(uc)

	httpSrv := khttp.NewServer(khttp.Address(env.Get("HTTP_ADDR", ":8001")))
	grpcSrv := kgrpc.NewServer(kgrpc.Address(env.Get("GRPC_ADDR", ":9001")))
	v1.RegisterUserHTTPServer(httpSrv, svc)
	v1.RegisterUserServer(grpcSrv, svc)

	app := kratos.New(
		kratos.Name("wavehub.user"),
		kratos.Server(httpSrv, grpcSrv),
	)
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
