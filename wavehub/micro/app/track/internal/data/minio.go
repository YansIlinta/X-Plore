// biz.ObjectStorage 的 MinIO 实现。minio-go 说的是 S3 协议，
// 上线换阿里云 OSS / 腾讯 COS / AWS S3 时只改这里的连接参数，接口和业务代码都不动。
package data

import (
	"context"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/YansIlinta/wavehub-micro/app/track/internal/biz"
)

type minioStorage struct {
	cli    *minio.Client
	bucket string
}

func NewMinioStorage(addr, accessKey, secretKey, bucket string) biz.ObjectStorage {
	cli, err := minio.New(addr, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false, // 本地开发不走 TLS，上云改 true
	})
	if err != nil {
		log.Fatalf("连接 MinIO 失败: %v", err)
	}

	// 启动时确保 bucket 存在，省得第一次上传才报错
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
	return &minioStorage{cli: cli, bucket: bucket}
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
		return false, err // 网络错误等要如实上报，不能当成"不存在"
	}
	return true, nil
}
