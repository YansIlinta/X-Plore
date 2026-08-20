package ops

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// captureEnvironment 采集"复现一个性能数字"所需的最小环境信息。
//
// 这是宿主侧只读元数据（git/go/os/内存），不是 API 可触达的任意命令通道：
// 参数完全固定 argv，不经过 shell；失败只降级为 null，绝不影响实验运行。
// repoDir 为空（未配置 -repo-dir）时 git 信息为 null。
func captureEnvironment(repoDir string) *EnvironmentSnapshot {
	e := &EnvironmentSnapshot{
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUCores:  runtime.NumCPU(),
	}
	if host, err := os.Hostname(); err == nil {
		e.Hostname = &host
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		e.MemoryBytes = memTotalBytes(string(b))
	}
	if strings.TrimSpace(repoDir) != "" {
		gitCommit, gitDirty := gitInfo(repoDir)
		e.GitCommit = gitCommit
		e.GitDirty = gitDirty
	}
	return e
}

// gitInfo 读取仓库 commit 与 dirty 状态；任意一步失败都返回 null（不猜）。
func gitInfo(repoDir string) (*string, *bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var commit *string
	out, err := exec.CommandContext(ctx, "git", "-C", repoDir, "rev-parse", "HEAD").Output()
	if err == nil {
		s := strings.TrimSpace(string(out))
		if s != "" {
			commit = &s
		}
	}

	var dirty *bool
	out, err = exec.CommandContext(ctx, "git", "-C", repoDir, "status", "--porcelain").Output()
	if err == nil {
		d := strings.TrimSpace(string(out)) != ""
		dirty = &d
	}
	return commit, dirty
}

// memTotalBytes 从 /proc/meminfo 解析 MemTotal 对应的字节数；失败返回 nil。
func memTotalBytes(meminfo string) *int64 {
	for _, line := range strings.Split(meminfo, "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				v := kb * 1024
				return &v
			}
		}
	}
	return nil
}
