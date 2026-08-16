package router

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub/internal/config"
	"github.com/YansIlinta/wavehub/internal/handler"
	"github.com/YansIlinta/wavehub/internal/middleware"
)

func New(db *gorm.DB, rdb *redis.Client, cfg config.Config) *gin.Engine {
	r := gin.Default() // 自带 Logger 和 Recovery(panic 不会打挂整个进程)

	trackH := handler.NewTrackHandler(db, rdb)
	userH := handler.NewUserHandler(db, cfg.JWTSecret)

	api := r.Group("/api/v1")
	{
		// 公开接口。注册/登录必须在这里而不是 auth 组——用户还没有 token，过不了 JWT 关卡
		api.POST("/register", userH.Register)
		api.POST("/login", userH.Login)
		api.GET("/tracks", trackH.List)       // ?page=1&size=20
		api.GET("/tracks/:id", trackH.Detail) // 详情含 peaks，前端直接画波形

		// 需要登录的接口：整组挂上 JWT 中间件
		auth := api.Group("", middleware.JWT(cfg.JWTSecret))
		{
			auth.POST("/tracks", trackH.Create) // 返回预签名上传 URL
			auth.POST("/tracks/:id/complete", trackH.CompleteUpload)
		}
	}
	return r
}
