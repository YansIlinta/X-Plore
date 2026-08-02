// JWT 鉴权中间件 —— Gin 中间件模式的最佳练手题。
// 模式：校验通过 c.Next() 放行，失败 c.Abort() 熔断，用 c.Set 向后传数据。
package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func JWT(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 约定请求头: Authorization: Bearer <token>
		raw := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		if raw == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
			return
		}

		token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
			return []byte(secret), nil // 生产环境还应校验 t.Method 是预期算法
		})
		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token 无效或已过期"})
			return
		}

		claims := token.Claims.(jwt.MapClaims)
		c.Set("userID", uint64(claims["uid"].(float64))) // handler 里 c.GetUint64("userID") 取
		c.Next()
	}
}
