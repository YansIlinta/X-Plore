package main

import (
	"os"
	"strconv"
	"strings"
)

// processResource 读取本进程自身的 /proc/self 资源（RSS、累计 CPU 时间）。
// Linux 可读；其他平台/读失败返回 nil（stats 端点对应字段保持 null，绝不猜）。
// 这是"目标 process 自己的 /proc/self"，不是读全系统或任意 pid。
type processResource struct {
	rssBytes int64
	cpuNanos int64
}

func sampleProcessResource() *processResource {
	p := &processResource{}
	if !readProcSelfStatus(p) {
		return nil
	}
	readProcSelfStat(p)
	return p
}

func readProcSelfStatus(p *processResource) bool {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					p.rssBytes = kb * 1024
					return true
				}
			}
			return false
		}
	}
	return false
}

func readProcSelfStat(p *processResource) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return
	}
	s := string(data)
	closeIdx := strings.LastIndex(s, ")")
	if closeIdx < 0 || closeIdx+2 > len(s) {
		return
	}
	rest := strings.Fields(s[closeIdx+1:])
	if len(rest) < 12 {
		return
	}
	utime, err1 := strconv.ParseInt(rest[10], 10, 64)
	stime, err2 := strconv.ParseInt(rest[11], 10, 64)
	if err1 != nil || err2 != nil {
		return
	}
	const clockTicks = 100
	p.cpuNanos = (utime + stime) * (1e9 / clockTicks)
}
