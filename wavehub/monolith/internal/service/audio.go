// 音频处理核心：调 FFmpeg 提取波形峰值。这是"音乐可视化"的服务端一半——
// 把任意音频压缩成 ~1000 个归一化振幅值，前端 Canvas/wavesurfer.js 直接照着画。
//
// 原理：ffmpeg 把音频解码成单声道 8kHz 的原始 16-bit PCM 流，
// 我们按窗口切分，每个窗口取振幅峰值，归一化到 [0,1]。
package service

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub/internal/model"
)

const peaksCount = 1000 // 输出多少个峰值点：1000 个点画 1000px 宽的波形正合适

// ExtractPeaks 从音频文件提取归一化波形峰值。
func ExtractPeaks(audioPath string) ([]float32, error) {
	// -ac 1 单声道  -ar 8000 降采样(画波形不需要高保真)  -f s16le 裸 PCM 输出到 stdout
	cmd := exec.Command("ffmpeg",
		"-i", audioPath,
		"-ac", "1", "-ar", "8000",
		"-f", "s16le", "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("ffmpeg 启动失败(装了吗?): %w", err)
	}

	// 先把全部样本读出来（一首歌 8kHz 单声道也就几 MB，可以接受）
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
			return nil, err
		}
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("ffmpeg 解码失败: %w", err)
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("没有解出任何音频数据")
	}

	// 分窗取峰值并归一化
	window := len(samples) / peaksCount
	if window == 0 {
		window = 1
	}
	peaks := make([]float32, 0, peaksCount)
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
		peaks = append(peaks, float32(peak)/32768.0)
	}
	return peaks, nil
}

// ProcessAudio 是异步任务的入口：提峰值 → 更新数据库状态。
// TODO(学习路线第5步): 从 MinIO 下载 track.ObjectKey 到临时文件、转码出播放格式、
// 用 ffprobe 读时长，这里先只演示峰值主流程。
func ProcessAudio(db *gorm.DB, trackID uint64) {
	var track model.Track
	if err := db.First(&track, trackID).Error; err != nil {
		return
	}

	peaks, err := ExtractPeaks(track.ObjectKey) // 暂以本地路径演示
	if err != nil {
		db.Model(&track).Update("status", "failed")
		return
	}
	peaksJSON, _ := json.Marshal(peaks)
	db.Model(&track).Updates(map[string]any{
		"status": "ready",
		"peaks":  peaksJSON,
	})
}
