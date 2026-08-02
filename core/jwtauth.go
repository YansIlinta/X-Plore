package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 业务 JWT 与 wavehub-micro user 服务约定一致：
//   HS256, claims: { "uid": <number>, "exp": <unix_sec> }
// 用于 comet WS 双模鉴权（与 DANMU_AUTH_TOKEN 压测 secret 并存）。

var (
	ErrJWTMalformed = errors.New("jwt malformed")
	ErrJWTBadSig    = errors.New("jwt bad signature")
	ErrJWTExpired   = errors.New("jwt expired")
	ErrJWTNoUID     = errors.New("jwt missing uid")
)

// VerifyBusinessJWT 校验 HS256 JWT，返回字符串形式的用户 id。
func VerifyBusinessJWT(token, secret string) (uid string, err error) {
	if secret == "" || token == "" {
		return "", ErrJWTMalformed
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", ErrJWTMalformed
	}
	headerJSON, err := b64Decode(parts[0])
	if err != nil {
		return "", ErrJWTMalformed
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return "", ErrJWTMalformed
	}
	if header.Alg != "HS256" {
		return "", fmt.Errorf("jwt unsupported alg %q", header.Alg)
	}

	signing := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signing))
	expected := mac.Sum(nil)
	sig, err := b64Decode(parts[2])
	if err != nil {
		return "", ErrJWTMalformed
	}
	if !hmac.Equal(sig, expected) {
		return "", ErrJWTBadSig
	}

	payloadJSON, err := b64Decode(parts[1])
	if err != nil {
		return "", ErrJWTMalformed
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return "", ErrJWTMalformed
	}
	if exp, ok := claims["exp"]; ok {
		var expUnix int64
		switch v := exp.(type) {
		case float64:
			expUnix = int64(v)
		case json.Number:
			expUnix, _ = v.Int64()
		}
		if expUnix > 0 && time.Now().Unix() > expUnix {
			return "", ErrJWTExpired
		}
	}
	switch v := claims["uid"].(type) {
	case float64:
		return fmt.Sprintf("%.0f", v), nil
	case string:
		if v == "" {
			return "", ErrJWTNoUID
		}
		return v, nil
	default:
		return "", ErrJWTNoUID
	}
}

func b64Decode(s string) ([]byte, error) {
	// JWT 使用 RawURLEncoding（无 padding）；兼容标准 base64url
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}
