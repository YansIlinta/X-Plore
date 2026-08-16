package main

import (
	"testing"
	"unicode/utf8"
)

func TestMergeJSONArrays(t *testing.T) {
	cases := []struct {
		name   string
		in     [][]byte
		want   string
	}{
		{"single", [][]byte{[]byte(`[{"a":1}]`)}, `[{"a":1}]`},
		{"multi", [][]byte{[]byte(`[{"a":1}]`), []byte(`[{"b":2},{"c":3}]`)}, `[{"a":1},{"b":2},{"c":3}]`},
		{"three", [][]byte{[]byte(`[1]`), []byte(`[2]`), []byte(`[3]`)}, `[1,2,3]`},
		{"empty", [][]byte{[]byte(`[]`)}, `[]`},
		{"empty mixed", [][]byte{[]byte(`[]`), []byte(`[1]`)}, `[1]`},
		{"whitespace", [][]byte{[]byte("  [1]  "), []byte("[2]")}, `[1,2]`},
		{"too short", [][]byte{[]byte(`[`)}, `[]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := string(mergeJSONArrays(c.in)); got != c.want {
				t.Fatalf("mergeJSONArrays(%v) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestTruncateContent(t *testing.T) {
	// 500 字节以内的内容原样返回
	ascii := string(make([]byte, 400)) // 400 ASCII 字节
	if got := truncateContent(ascii, 500); got != ascii {
		t.Fatal("content within limit was modified")
	}

	// 超长 ASCII：截断到 500 字节
	long := string(make([]byte, 600))
	if got := truncateContent(long, 500); len(got) != 500 {
		t.Fatalf("ascii truncation len = %d, want 500", len(got))
	}

	// 超长中文：不能产生乱码（结果必须是合法 UTF-8，且按 rune 截断）
	chinese := ""
	for i := 0; i < 300; i++ {
		chinese += "弹" // 3 字节/字，300 字 = 900 字节 > 500
	}
	got := truncateContent(chinese, 500)
	if !utf8.ValidString(got) {
		t.Fatal("truncated content is not valid UTF-8")
	}
	runes := []rune(got)
	if len(runes) != 300 {
		// 900 字节 > 500 但 rune 数 300 <= 500：按 rune 语义不应截断
		t.Fatalf("rune count = %d, want 300", len(runes))
	}
	if got != chinese {
		t.Fatal("content longer than 500 bytes but within 500 runes should be unchanged")
	}

	// 超过 500 个 rune 时截断到 500 rune
	many := ""
	for i := 0; i < 600; i++ {
		many += "弹"
	}
	got = truncateContent(many, 500)
	if !utf8.ValidString(got) {
		t.Fatal("truncated many-rune content is not valid UTF-8")
	}
	if r := []rune(got); len(r) != 500 {
		t.Fatalf("rune count = %d, want 500", len(r))
	}
}
