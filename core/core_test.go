package core

import (
	"strings"
	"testing"
	"time"
)

func TestSensitiveFilter(t *testing.T) {
	f := NewSensitiveFilter(DefaultSensitiveWords)
	cases := []struct {
		in     string
		masked bool
	}{
		{"你好世界", false},
		{"办假证快来", true},
		{"正常内容加微信代刷单结尾", true},
		{"", false},
	}
	for _, c := range cases {
		out := f.Filter(c.in)
		has := strings.Contains(out, "*")
		if has != c.masked {
			t.Errorf("Filter(%q)=%q masked=%v want %v", c.in, out, has, c.masked)
		}
		if !c.masked && out != c.in {
			t.Errorf("Filter(%q) 改动了未命中文本: %q", c.in, out)
		}
	}
	// 命中词等长打码
	if got := f.Filter("办假证"); got != "***" {
		t.Errorf("Filter(办假证)=%q want ***", got)
	}
}

func TestTokenIssuerRoundTrip(t *testing.T) {
	ti := NewTokenIssuer("secret-abc")
	tok, exp := ti.Issue("u1", "room-1", time.Minute)
	got, err := ti.Verify(tok, "u1", "room-1")
	if err != nil {
		t.Fatalf("Verify 失败: %v", err)
	}
	if got.Unix() != exp.Unix() {
		t.Errorf("过期时间不一致: %v vs %v", got, exp)
	}
	// 绑定 uid/room 不匹配应拒绝
	if _, err := ti.Verify(tok, "u2", "room-1"); err == nil {
		t.Error("uid 不匹配应拒绝")
	}
	if _, err := ti.Verify(tok, "u1", "room-2"); err == nil {
		t.Error("room 不匹配应拒绝")
	}
	// 篡改签名应拒绝
	if _, err := ti.Verify(tok+"x", "u1", "room-1"); err == nil {
		t.Error("篡改令牌应拒绝")
	}
	// 另一个密钥签发的不认
	other := NewTokenIssuer("secret-xyz")
	if _, err := ti.Verify(mustToken(other, "u1", "room-1"), "u1", "room-1"); err == nil {
		t.Error("异密钥令牌应拒绝")
	}
}

func mustToken(ti *TokenIssuer, uid, room string) string {
	tok, _ := ti.Issue(uid, room, time.Minute)
	return tok
}

func TestTokenBucket(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 5 突发
	allowed := 0
	for i := 0; i < 5; i++ {
		if tb.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("突发应放行 5，实际 %d", allowed)
	}
	if tb.Allow() {
		t.Error("突发耗尽后应拒绝")
	}
}
