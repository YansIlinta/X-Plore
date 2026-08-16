package ops

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// NewLoadtestManager 路径解析：存在（含 Windows 无扩展名 PE）→ available；
// 不存在 → available=false 且 Start 返回明确错误，绝不假装在压测。
func TestLoadtestManagerResolve(t *testing.T) {
	dir := t.TempDir()

	// 无扩展名文件（Windows 上 go build -o bin/loadtest 的产物形态）也要能解析
	bin := filepath.Join(dir, "loadtest")
	if err := os.WriteFile(bin, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	m := NewLoadtestManager(bin, "tok", context.Background())
	if !m.available {
		t.Fatalf("extensionless file not resolved: %+v", m)
	}

	// 带 .exe 的文件：bin 不带扩展名传入时兜底到 bin+".exe"
	binExe := filepath.Join(dir, "loadtest2.exe")
	if err := os.WriteFile(binExe, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	m = NewLoadtestManager(binExe[:len(binExe)-4], "tok", context.Background())
	if !m.available || filepath.Base(m.bin) != "loadtest2.exe" {
		t.Fatalf("exe fallback failed: %+v", m)
	}

	// 不存在：available=false，Start 报"not found"（api 层映射为 503）
	m = NewLoadtestManager(filepath.Join(dir, "nope"), "tok", context.Background())
	if m.available {
		t.Fatalf("missing binary marked available")
	}
	if err := m.Start(nil); err == nil {
		t.Fatalf("Start on missing binary must fail")
	}
}
