// Package authmw 提供各业务服务共用的 JWT 鉴权中间件(自 app/video 提升为公共包)。
// 统一 context key,保证 UserIDFromContext 在所有服务行为一致。
package authmw

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
				if t.Method != jwt.SigningMethodHS256 {
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

func UserIDFromContext(ctx context.Context) uint64 {
	uid, _ := ctx.Value(userIDKey{}).(uint64)
	return uid
}

// OptionalJWTAuth 有 Bearer 则解析注入 uid,无效/缺失不拦截(用于详情页「是否已赞/已关注」)。
func OptionalJWTAuth(secret string) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return handler(ctx, req)
			}
			raw := strings.TrimPrefix(tr.RequestHeader().Get("Authorization"), "Bearer ")
			if raw == "" {
				return handler(ctx, req)
			}
			token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
				if t.Method != jwt.SigningMethodHS256 {
					return nil, errors.Unauthorized("BAD_ALG", "非法签名算法")
				}
				return []byte(secret), nil
			})
			if err != nil || !token.Valid {
				return handler(ctx, req) // 可选:坏 token 当匿名
			}
			claims, _ := token.Claims.(jwt.MapClaims)
			uid, _ := claims["uid"].(float64)
			return handler(context.WithValue(ctx, userIDKey{}, uint64(uid)), req)
		}
	}
}
