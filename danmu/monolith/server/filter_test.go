package main

import "testing"

func TestSensitiveFilter(t *testing.T) {
	f := NewSensitiveFilter([]string{"办假证", "私彩博彩", "spam"})

	cases := []struct {
		in   string
		want string
	}{
		{"你好世界", "你好世界"},
		{"帮我办假证要多少钱", "帮我***要多少钱"},
		{"来玩私彩博彩啊", "来玩****啊"},
		{"contains spam word", "contains **** word"},
		{"", ""},
		{"办假", "办假"}, // 不完整匹配，不应替换
	}

	for _, c := range cases {
		got := f.Filter(c.in)
		if got != c.want {
			t.Errorf("Filter(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSensitiveFilterOverlap(t *testing.T) {
	// 重叠敏感词：ab 和 bc 都命中，应覆盖整个 abc
	f := NewSensitiveFilter([]string{"ab", "bc"})
	got := f.Filter("xabcx")
	want := "x***x"
	if got != want {
		t.Errorf("Filter overlap = %q, want %q", got, want)
	}
}
