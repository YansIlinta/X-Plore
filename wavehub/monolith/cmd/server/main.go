// 入口只做一件事：把各个组件"组装"起来。业务逻辑一行都不要写在这里。
package main

import (
	"log"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub/internal/config"
	"github.com/YansIlinta/wavehub/internal/model"
	"github.com/YansIlinta/wavehub/internal/router"
)

func main() {
	cfg := config.Load()

	db, err := gorm.Open(postgres.Open(cfg.PostgresDSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接 PostgreSQL 失败: %v", err)
	}
	// 学习期用 AutoMigrate 自动建表；生产项目应换成迁移工具(如 golang-migrate)管理 SQL 版本
	if err := db.AutoMigrate(&model.User{}, &model.Track{}); err != nil {
		log.Fatalf("建表失败: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	r := router.New(db, rdb, cfg)
	log.Printf("WaveHub 启动于 %s", cfg.ListenAddr)
	if err := r.Run(cfg.ListenAddr); err != nil {
		log.Fatal(err)
	}
}
