package core

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
	ErrInvalidToken = errors.New("invalid session token")
	ErrTokenExpired = errors.New("session token expired")
)

// SessionTTL 会话令牌有效期，由 comet 通过 -session-ttl 覆盖（启动前设置一次，运行期只读）。
var SessionTTL = 10 * time.Minute

// TokenIssuer 签发/校验带过期时间、绑定 uid+room 的会话令牌（HMAC-SHA256）。
type TokenIssuer struct {
	secret []byte
}

func deriveSigningKey(authToken string) []byte {
	sum := sha256.Sum256([]byte(authToken + ":danmu-session-signing"))
	return sum[:]
}

func NewTokenIssuer(authToken string) *TokenIssuer {
	return &TokenIssuer{secret: deriveSigningKey(authToken)}
}

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

func (ti *TokenIssuer) Verify(token, uid, roomID string) (time.Time, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return time.Time{}, ErrInvalidToken
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return time.Time{}, ErrInvalidToken
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, ErrInvalidToken
	}
	if !hmac.Equal(sig, ti.sign(string(payloadBytes))) {
		return time.Time{}, ErrInvalidToken
	}
	fields := strings.SplitN(string(payloadBytes), "|", 3)
	if len(fields) != 3 || fields[0] != uid || fields[1] != roomID {
		return time.Time{}, ErrInvalidToken
	}
	expUnix, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return time.Time{}, ErrInvalidToken
	}
	expiresAt := time.Unix(expUnix, 0)
	if time.Now().After(expiresAt) {
		return time.Time{}, ErrTokenExpired
	}
	return expiresAt, nil
}
