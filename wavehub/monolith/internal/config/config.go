// 配置全部来自环境变量（12-Factor 原则：同一个二进制，靠环境变量区分开发/生产）。
// 学习期不引入 viper 这类配置库——标准库 os.Getenv 完全够用，依赖越少越好。
package config

import "os"

type Config struct {
	ListenAddr  string
	PostgresDSN string
	RedisAddr   string
	JWTSecret   string
	MinioAddr   string
}

func Load() Config {
	return Config{
		ListenAddr:  getEnv("LISTEN_ADDR", ":8080"),
		PostgresDSN: getEnv("PG_DSN", "host=localhost user=wavehub password=wavehub dbname=wavehub port=5432 sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		JWTSecret:   getEnv("JWT_SECRET", "dev-only-change-me"),
		MinioAddr:   getEnv("MINIO_ADDR", "localhost:9000"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
