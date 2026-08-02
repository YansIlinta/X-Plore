// Kratos 风格的 JWT 中间件：和 Gin 版(wavehub)对比着看 ——
// Gin 用 c.Abort/c.Next/c.Set，Kratos 用"返回错误即熔断 + context.WithValue 传值"，
// 因为 Kratos 中间件要同时适用于 HTTP 和 gRPC 两种传输层。
package middleware

import (
	"context"
	"strings"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/golang-jwt/jwt/v5"
)

type userIDKey struct{}

func JWTAuth(secret string) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return nil, errors.Unauthorized("NO_TRANSPORT", "无法获取请求信息")
			}
			raw := strings.TrimPrefix(tr.RequestHeader().Get("Authorization"), "Bearer ")
			if raw == "" {
				return nil, errors.Unauthorized("NO_TOKEN", "未登录")
			}
			token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
				if t.Method != jwt.SigningMethodHS256 { // 防算法替换攻击
					return nil, errors.Unauthorized("BAD_ALG", "非法签名算法")
				}
				return []byte(secret), nil
			})
			if err != nil || !token.Valid {
				return nil, errors.Unauthorized("BAD_TOKEN", "token 无效或已过期")
			}
			claims, _ := token.Claims.(jwt.MapClaims)
			uid, _ := claims["uid"].(float64)
			return handler(context.WithValue(ctx, userIDKey{}, uint64(uid)), req)
		}
	}
}

// UserIDFromContext 给 service 层取当前登录用户
func UserIDFromContext(ctx context.Context) uint64 {
	uid, _ := ctx.Value(userIDKey{}).(uint64)
	return uid
}
