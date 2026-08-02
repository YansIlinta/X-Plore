package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func signTestJWT(t *testing.T, secret string, claims map[string]any) string {
	t.Helper()
	hb, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	pb, _ := json.Marshal(claims)
	h := base64.RawURLEncoding.EncodeToString(hb)
	p := base64.RawURLEncoding.EncodeToString(pb)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(h + "." + p))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return h + "." + p + "." + sig
}

func TestVerifyBusinessJWT_OK(t *testing.T) {
	secret := "dev-only-change-me"
	tok := signTestJWT(t, secret, map[string]any{
		"uid": float64(42),
		"exp": float64(time.Now().Add(time.Hour).Unix()),
	})
	uid, err := VerifyBusinessJWT(tok, secret)
	if err != nil {
		t.Fatal(err)
	}
	if uid != "42" {
		t.Fatalf("uid=%q", uid)
	}
}

func TestVerifyBusinessJWT_Expired(t *testing.T) {
	secret := "s"
	tok := signTestJWT(t, secret, map[string]any{
		"uid": float64(1),
		"exp": float64(time.Now().Add(-time.Hour).Unix()),
	})
	_, err := VerifyBusinessJWT(tok, secret)
	if err != ErrJWTExpired {
		t.Fatalf("want expired, got %v", err)
	}
}

func TestVerifyBusinessJWT_BadSig(t *testing.T) {
	tok := signTestJWT(t, "a", map[string]any{"uid": float64(1), "exp": float64(time.Now().Add(time.Hour).Unix())})
	_, err := VerifyBusinessJWT(tok, "b")
	if err != ErrJWTBadSig {
		t.Fatalf("want bad sig, got %v", err)
	}
}
