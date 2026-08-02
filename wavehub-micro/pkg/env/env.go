// 各服务共用的环境变量读取。微服务配置的原则和单体一样：环境变量优先(12-Factor)。
// 量大之后再上配置中心(Nacos/Apollo/etcd)，学习期不引入。
package env

import (
	"os"
	"strconv"
)

func Get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func GetInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// IsProd APP_ENV=prod|production
func IsProd() bool {
	e := Get("APP_ENV", "dev")
	return e == "prod" || e == "production"
}
