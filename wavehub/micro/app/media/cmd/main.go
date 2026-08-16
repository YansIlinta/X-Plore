// media worker：从 asynq 领取音频 peaks / 视频 HLS 任务，完成后 gRPC 回写。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/hibiken/asynq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	trackv1 "github.com/YansIlinta/wavehub-micro/api/track/v1"
	videov1 "github.com/YansIlinta/wavehub-micro/api/video/v1"
	"github.com/YansIlinta/wavehub-micro/app/media/internal/hls"
	"github.com/YansIlinta/wavehub-micro/app/media/internal/peaks"
	"github.com/YansIlinta/wavehub-micro/pkg/env"
	"github.com/YansIlinta/wavehub-micro/pkg/task"
)

type worker struct {
	trackCli    trackv1.TrackClient
	videoCli    videov1.VideoClient
	minio       *minio.Client
	audioBucket string
	videoBucket string
}

func (w *worker) processAudio(ctx context.Context, t *asynq.Task) error {
	if w.trackCli == nil {
		return fmt.Errorf("track client 未配置，无法处理音频任务")
	}
	var p task.ProcessAudioPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}
	log.Printf("开始处理 track %d: %s", p.TrackID, p.ObjectKey)

	tmp, err := os.CreateTemp("", "wavehub-*.audio")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)
	if err := w.minio.FGetObject(ctx, w.audioBucket, p.ObjectKey, tmpPath, minio.GetObjectOptions{}); err != nil {
		return err
	}

	pk, duration, err := peaks.Extract(tmpPath)
	if err != nil {
		log.Printf("track %d 处理失败: %v", p.TrackID, err)
		_, rerr := w.trackCli.ReportProcessed(ctx, &trackv1.ReportProcessedRequest{
			Id: p.TrackID, Success: false,
		})
		return rerr
	}

	_, err = w.trackCli.ReportProcessed(ctx, &trackv1.ReportProcessedRequest{
		Id: p.TrackID, Success: true, DurationSec: duration, Peaks: pk,
	})
	return err
}

func (w *worker) processVideo(ctx context.Context, t *asynq.Task) error {
	var p task.ProcessVideoPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}
	log.Printf("开始处理 video %d: %s", p.VideoID, p.ObjectKey)

	workRoot, err := os.MkdirTemp("", "xplore-video-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workRoot)

	srcPath := filepath.Join(workRoot, "original")
	if err := w.minio.FGetObject(ctx, w.videoBucket, p.ObjectKey, srcPath, minio.GetObjectOptions{}); err != nil {
		return err
	}

	hlsDir := filepath.Join(workRoot, "hls")
	res, err := hls.Transcode(srcPath, hlsDir)
	if err != nil {
		log.Printf("video %d 转码失败: %v", p.VideoID, err)
		_, rerr := w.videoCli.ReportProcessed(ctx, &videov1.ReportProcessedRequest{
			Id: p.VideoID, Success: false, ErrorMessage: err.Error(),
		})
		return rerr
	}

	// 上传 HLS 目录内全部文件
	prefix := fmt.Sprintf("videos/%d/hls", p.VideoID)
	playlistKey := prefix + "/index.m3u8"
	if err := uploadDir(ctx, w.minio, w.videoBucket, hlsDir, prefix); err != nil {
		return err
	}

	coverKey := ""
	if res.CoverFile != "" {
		coverKey = fmt.Sprintf("videos/%d/cover.jpg", p.VideoID)
		if _, err := w.minio.FPutObject(ctx, w.videoBucket, coverKey, res.CoverFile, minio.PutObjectOptions{
			ContentType: "image/jpeg",
		}); err != nil {
			log.Printf("video %d 封面上传失败(忽略): %v", p.VideoID, err)
			coverKey = ""
		}
	}

	_, err = w.videoCli.ReportProcessed(ctx, &videov1.ReportProcessedRequest{
		Id:          p.VideoID,
		Success:     true,
		DurationSec: res.DurationSec,
		PlaylistKey: playlistKey,
		CoverKey:    coverKey,
	})
	if err != nil {
		return err
	}
	log.Printf("video %d 处理完成 duration=%.1fs playlist=%s", p.VideoID, res.DurationSec, playlistKey)
	return nil
}

func uploadDir(ctx context.Context, cli *minio.Client, bucket, localDir, keyPrefix string) error {
	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		key := keyPrefix + "/" + rel
		ct := contentType(path)
		_, err = cli.FPutObject(ctx, bucket, key, path, minio.PutObjectOptions{ContentType: ct})
		return err
	})
}

func contentType(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".ts":
		return "video/mp2t"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	default:
		return "application/octet-stream"
	}
}

func main() {
	// track 可选：仅跑点播平台时可不启 track 服务
	var trackCli trackv1.TrackClient
	if env.Get("ENABLE_TRACK_WORKER", "true") == "true" {
		trackConn, err := kgrpc.DialInsecure(context.Background(),
			kgrpc.WithEndpoint(env.Get("TRACK_GRPC_ADDR", "localhost:9002")))
		if err != nil {
			log.Printf("警告: 连接 track 失败，将跳过音频任务: %v", err)
		} else {
			defer trackConn.Close()
			trackCli = trackv1.NewTrackClient(trackConn)
		}
	}

	videoConn, err := kgrpc.DialInsecure(context.Background(),
		kgrpc.WithEndpoint(env.Get("VIDEO_GRPC_ADDR", "localhost:9003")))
	if err != nil {
		log.Fatalf("连接 video 服务失败: %v", err)
	}
	defer videoConn.Close()

	mcli, err := minio.New(env.Get("MINIO_ADDR", "localhost:9000"), &minio.Options{
		Creds: credentials.NewStaticV4(
			env.Get("MINIO_ACCESS_KEY", "wavehub"),
			env.Get("MINIO_SECRET_KEY", "wavehub123"), ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("连接 MinIO 失败: %v", err)
	}

	w := &worker{
		trackCli:    trackCli,
		videoCli:    videov1.NewVideoClient(videoConn),
		minio:       mcli,
		audioBucket: env.Get("MINIO_AUDIO_BUCKET", "wavehub-audio"),
		videoBucket: env.Get("MINIO_VIDEO_BUCKET", "xplore-video"),
	}

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: env.Get("REDIS_ADDR", "localhost:6379")},
		asynq.Config{Concurrency: 1}, // 视频转码吃 CPU，默认串行
	)
	mux := asynq.NewServeMux()
	mux.HandleFunc(task.TypeProcessAudio, w.processAudio)
	mux.HandleFunc(task.TypeProcessVideo, w.processVideo)

	log.Println("media worker 已启动（audio + video HLS）…")
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
