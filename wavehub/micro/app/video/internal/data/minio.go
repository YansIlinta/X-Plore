package data

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/YansIlinta/wavehub-micro/app/video/internal/biz"
)

type minioStorage struct {
	cli        *minio.Client
	bucket     string
	publicBase string // 如 http://localhost:9000/xplore-video ；空则不做公共 URL
}

// NewMinioStorage 创建存储。publicBase 示例：http://localhost:9000/xplore-video
// HLS 播放依赖桶策略允许匿名读 videos/*/hls/*（见 deploy/platform 的 minio-init）。
func NewMinioStorage(addr, accessKey, secretKey, bucket, publicBase string) biz.ObjectStorage {
	cli, err := minio.New(addr, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("连接 MinIO 失败: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := cli.BucketExists(ctx, bucket)
	if err != nil {
		log.Fatalf("检查 bucket 失败(MinIO 起了吗?): %v", err)
	}
	if !exists {
		if err := cli.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			log.Fatalf("创建 bucket 失败: %v", err)
		}
	}
	return &minioStorage{cli: cli, bucket: bucket, publicBase: strings.TrimRight(publicBase, "/")}
}

func (s *minioStorage) PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	u, err := s.cli.PresignedPutObject(ctx, s.bucket, key, expiry)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *minioStorage) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	u, err := s.cli.PresignedGetObject(ctx, s.bucket, key, expiry, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *minioStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.cli.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *minioStorage) PublicURL(key string) string {
	if s.publicBase == "" || key == "" {
		return ""
	}
	return s.publicBase + "/" + strings.TrimLeft(key, "/")
}
