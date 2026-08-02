// Package hls 使用本机 ffmpeg 将原片转成 HLS，并截取封面。
package hls

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// Result 转码产物（本地路径）。
type Result struct {
	PlaylistDir  string // 含 index.m3u8 与 .ts 的目录
	PlaylistFile string // .../index.m3u8
	CoverFile    string // .../cover.jpg
	DurationSec  float64
}

// Transcode 将 input 转为 HLS（单清晰度 720p 上限）并截封面。
// 需要 PATH 中有 ffmpeg / ffprobe。
func Transcode(input, workDir string) (*Result, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	playlist := filepath.Join(workDir, "index.m3u8")
	segmentPattern := filepath.Join(workDir, "seg_%05d.ts")
	cover := filepath.Join(workDir, "cover.jpg")

	// 单档 HLS：H.264 + AAC，约 720p
	cmd := exec.Command("ffmpeg", "-y", "-i", input,
		"-vf", "scale='min(1280,iw)':-2",
		"-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
		"-c:a", "aac", "-b:a", "128k",
		"-hls_time", "4",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", segmentPattern,
		playlist,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg hls: %w: %s", err, tail(stderr.String(), 800))
	}

	// 封面：第 1 秒
	coverCmd := exec.Command("ffmpeg", "-y", "-ss", "1", "-i", input,
		"-frames:v", "1", "-q:v", "3", cover)
	var coverErr bytes.Buffer
	coverCmd.Stderr = &coverErr
	if err := coverCmd.Run(); err != nil {
		// 封面失败不阻断成片
		cover = ""
	}

	dur, _ := probeDuration(input)
	return &Result{
		PlaylistDir:  workDir,
		PlaylistFile: playlist,
		CoverFile:    cover,
		DurationSec:  dur,
	}, nil
}

func probeDuration(input string) (float64, error) {
	cmd := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1", input)
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
