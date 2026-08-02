// FFmpeg 波形峰值提取 —— 和单体版(wavehub/internal/service/audio.go)算法完全相同。
// 微服务化后它搬进了独立的 media 服务：转码是 CPU 密集型工作，
// 独立部署后可以单独加机器扩容，而不用把 API 服务一起扩(这才是拆服务的正当理由)。
package peaks

import (
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
)

const (
	peaksCount = 1000
	sampleRate = 8000
)

// Extract 返回归一化峰值数组和音频时长(秒)。
func Extract(audioPath string) ([]float32, float64, error) {
	// -ac 1 单声道  -ar 8000 降采样  -f s16le 裸 PCM 到 stdout
	cmd := exec.Command("ffmpeg",
		"-i", audioPath,
		"-ac", "1", "-ar", fmt.Sprint(sampleRate),
		"-f", "s16le", "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, 0, err
	}
	if err := cmd.Start(); err != nil {
		return nil, 0, fmt.Errorf("ffmpeg 启动失败(装了吗?): %w", err)
	}

	var samples []int16
	buf := make([]byte, 8192)
	for {
		n, err := stdout.Read(buf)
		for i := 0; i+1 < n; i += 2 {
			samples = append(samples, int16(binary.LittleEndian.Uint16(buf[i:])))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}
	}
	if err := cmd.Wait(); err != nil {
		return nil, 0, fmt.Errorf("ffmpeg 解码失败: %w", err)
	}
	if len(samples) == 0 {
		return nil, 0, fmt.Errorf("没有解出任何音频数据")
	}

	window := len(samples) / peaksCount
	if window == 0 {
		window = 1
	}
	out := make([]float32, 0, peaksCount)
	for start := 0; start < len(samples); start += window {
		end := min(start+window, len(samples))
		var peak int16
		for _, s := range samples[start:end] {
			if s < 0 {
				s = -s
			}
			if s > peak {
				peak = s
			}
		}
		out = append(out, float32(peak)/32768.0)
	}
	return out, float64(len(samples)) / sampleRate, nil
}
