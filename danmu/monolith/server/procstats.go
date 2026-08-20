package main

import (
	"os"
	"strconv"
	"strings"
)

// processResource 读取本进程自身的 /proc/self 资源（RSS、累计 CPU 时间）。
// Linux 可读；其他平台/读失败返回 nil（stats 端点对应字段保持 null，绝不猜）。
//
// 注意：这是"目标 process 自己的 /proc/self"，不是读全系统或任意 pid ——
// 不越权，不会把机器负载冒充成这个 server 的负载。
type processResource struct {
	rssBytes int64 // VmRSS
	cpuNanos int64 // utime+stime（进程累计 CPU 时间，纳秒）
}

func sampleProcessResource() *processResource {
	p := &processResource{}
	if !readProcSelfStatus(p) {
		return nil
	}
	readProcSelfStat(p)
	return p
}

// readProcSelfStatus 解析 /proc/self/status 的 VmRSS。
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

// readProcSelfStat 解析 /proc/self/stat 的 utime/stime（jiffies，FIELD 14/15，
// 注意第 3 字段 comm 可能含空格，先跳过括号）。失败则 cpuNanos 保持 0。
func readProcSelfStat(p *processResource) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return
	}
	s := string(data)
	// 跳过 "pid (comm) "：括号后第一个空格开始才是字段 3 之后。
	closeIdx := strings.LastIndex(s, ")")
	if closeIdx < 0 || closeIdx+2 > len(s) {
		return
	}
	rest := strings.Fields(s[closeIdx+1:])
	// 字段 14=utime, 15=stime（rest[10], rest[11]，因为 rest[0] 对应字段 3）。
	// 需要 utime/stime 之前有 11 个字段（state 之后）。
	if len(rest) < 12 {
		return
	}
	utime, err1 := strconv.ParseInt(rest[10], 10, 64)
	stime, err2 := strconv.ParseInt(rest[11], 10, 64)
	if err1 != nil || err2 != nil {
		return
	}
	const clockTicks = 100 // Linux USER_HZ 通常为 100
	p.cpuNanos = (utime + stime) * (1e9 / clockTicks)
}
