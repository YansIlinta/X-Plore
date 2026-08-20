package ops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---- 测试辅助（Phase 1.5 重复实验 / 恢复 / 采样）----

func nowTime() time.Time              { return time.Now().UTC() }
func timeNow() time.Time              { return time.Now() }
func timeoutSec() time.Duration       { return time.Second }
func filepathJoin(a, b string) string { return filepath.Join(a, b) }
func contextBG() context.Context      { return context.Background() }

func sleepMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

// fakeStep 是一次假 loadtest 调用的行为。
type fakeStep struct {
	report   string // 写进 -output-json 的内容（"" = 不写）
	exitCode int
	sleepSec float64
}

// writeFakeLoadtestSequence 生成一个按调用次数依次执行 fakeStep 的假 loadtest
// （把步骤内嵌进脚本，不依赖 python3/JSON；超出步骤数则复用最后一步）。
func writeFakeLoadtestSequence(t *testing.T, dir string, steps []fakeStep) string {
	t.Helper()
	if len(steps) == 0 {
		steps = []fakeStep{{report: standardReportJSON(), exitCode: 0, sleepSec: 0.01}}
	}
	path := filepath.Join(dir, "sequence-loadtest")
	cntFile := filepath.Join(dir, "counter")
	var b []byte
	b = fmt.Appendf(b, "#!/bin/sh\n")
	b = fmt.Append(b, "OUT=\"\"\n")
	b = fmt.Append(b, "while [ \"$#\" -gt 0 ]; do\n")
	b = fmt.Append(b, "  if [ \"$1\" = \"-output-json\" ]; then OUT=\"$2\"; shift 2; else shift; fi\n")
	b = fmt.Append(b, "done\n")
	b = fmt.Appendf(b, "CNT_FILE=%s\n", shellQuote(cntFile))
	b = fmt.Append(b, "if [ ! -f \"$CNT_FILE\" ]; then echo 0 > \"$CNT_FILE\"; fi\n")
	b = fmt.Append(b, "CNT=$(cat \"$CNT_FILE\")\n")
	b = fmt.Append(b, "CNT=$((CNT + 1))\n")
	b = fmt.Append(b, "echo \"$CNT\" > \"$CNT_FILE\"\n")
	b = fmt.Append(b, "case \"$CNT\" in\n")
	for i, st := range steps {
		b = fmt.Appendf(b, "  %d) REPORT=%s; EXIT=%d; SLEEP=%s ;;\n", i+1, shellQuote(st.report), st.exitCode, ftoa(st.sleepSec))
	}
	last := steps[len(steps)-1]
	b = fmt.Appendf(b, "  *) REPORT=%s; EXIT=%d; SLEEP=%s ;;\n", shellQuote(last.report), last.exitCode, ftoa(last.sleepSec))
	b = fmt.Append(b, "esac\n")
	b = fmt.Appendf(b, "if [ -n \"$OUT\" ] && [ -n \"$REPORT\" ]; then printf '%%s' \"$REPORT\" > \"$OUT\"; fi\n")
	b = fmt.Append(b, "sleep \"$SLEEP\"\n")
	b = fmt.Append(b, "exit \"$EXIT\"\n")
	if err := os.WriteFile(path, b, 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func ftoa(v float64) string {
	return fmt.Sprintf("%g", v)
}
