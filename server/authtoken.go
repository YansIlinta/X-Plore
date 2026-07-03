package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidToken = errors.New("invalid session token")
	errTokenExpired = errors.New("session token expired")
)

// sessionTTL 会话令牌有效期：WebSocket 握手时用的静态 token 只鉴权一次，
// 连接建立后由服务端签发一个短时效的会话令牌，客户端需在到期前通过 "reauth"
// 消息用刷新到的新令牌续期，否则服务端在到期后主动断开连接
const sessionTTL = 10 * time.Minute

// TokenIssuer 签发/校验带过期时间、绑定 uid+room 的会话令牌（HMAC-SHA256签名）
// 签名密钥并非直接使用 DANMU_AUTH_TOKEN 本身，而是派生密钥——避免会话令牌的
// 签名素材与握手用的静态令牌完全等价
type TokenIssuer struct {
	secret []byte
}

// deriveSigningKey 从静态鉴权 token 派生独立的签名密钥
func deriveSigningKey(authToken string) []byte {
	sum := sha256.Sum256([]byte(authToken + ":danmu-session-signing"))
	return sum[:]
}

func NewTokenIssuer(authToken string) *TokenIssuer {
	return &TokenIssuer{secret: deriveSigningKey(authToken)}
}

// Issue 签发一个绑定 uid+roomID、ttl 后过期的会话令牌
func (ti *TokenIssuer) Issue(uid, roomID string, ttl time.Duration) (token string, expiresAt time.Time) {
	expiresAt = time.Now().Add(ttl)
	payload := fmt.Sprintf("%s|%s|%d", uid, roomID, expiresAt.Unix())
	sig := ti.sign(payload)
	token = base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(sig)
	return token, expiresAt
}

func (ti *TokenIssuer) sign(payload string) []byte {
	mac := hmac.New(sha256.New, ti.secret)
	mac.Write([]byte(payload))
	return mac.Sum(nil)
}

// Verify 校验令牌签名、绑定的 uid/room 是否匹配、以及是否已过期
func (ti *TokenIssuer) Verify(token, uid, roomID string) (time.Time, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return time.Time{}, errInvalidToken
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return time.Time{}, errInvalidToken
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errInvalidToken
	}
	if !hmac.Equal(sig, ti.sign(string(payloadBytes))) {
		return time.Time{}, errInvalidToken
	}

	fields := strings.SplitN(string(payloadBytes), "|", 3)
	if len(fields) != 3 || fields[0] != uid || fields[1] != roomID {
		return time.Time{}, errInvalidToken
	}
	expUnix, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return time.Time{}, errInvalidToken
	}
	expiresAt := time.Unix(expUnix, 0)
	if time.Now().After(expiresAt) {
		return time.Time{}, errTokenExpired
	}
	return expiresAt, nil
}
