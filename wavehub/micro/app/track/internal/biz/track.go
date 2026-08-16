package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/hibiken/asynq"

	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/pkg/task"
)

type Track struct {
	ID          uint64
	UserID      uint64
	Title       string
	Status      string // processing / ready / failed
	ObjectKey   string
	DurationSec float64
	PlayCount   int64
	Peaks       []float32
}

// ObjectStorage 由 data 层用 MinIO 实现。biz 只关心"给我能上传/下载的 URL"，
// 不关心背后是 MinIO 还是阿里云 OSS —— 换云厂商时这个接口一个字不改。
type ObjectStorage interface {
	PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error)
	PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error)
	Exists(ctx context.Context, key string) (bool, error)
}

type TrackRepo interface {
	Create(ctx context.Context, t *Track) error
	Get(ctx context.Context, id uint64) (*Track, error)
	List(ctx context.Context, offset, limit int) ([]*Track, error)
	UpdateUpload(ctx context.Context, id uint64, objectKey string) error
	UpdateProcessed(ctx context.Context, id uint64, success bool, durationSec float64, peaks []float32) error
	IncrPlay(ctx context.Context, id uint64) error
}

type TrackUsecase struct {
	repo TrackRepo
	// 跨服务取数：不 JOIN 别人的表，而是拿着生成的 gRPC 客户端(本身就是接口)去调 user 服务
	userCli userv1.UserClient
	// 教学取舍：biz 直接持有 asynq.Client；更纯的做法是在 biz 定义 Queue 接口、data 层实现
	queue   *asynq.Client
	storage ObjectStorage
}

func NewTrackUsecase(repo TrackRepo, userCli userv1.UserClient, queue *asynq.Client, storage ObjectStorage) *TrackUsecase {
	return &TrackUsecase{repo: repo, userCli: userCli, queue: queue, storage: storage}
}

// Create: 登记元数据 + 分配文件位置 + 返回预签名直传 URL。
// key 由服务端决定而不是客户端上报 —— 客户端能指定 key 就能覆盖别人的文件。
func (uc *TrackUsecase) Create(ctx context.Context, userID uint64, title string) (id uint64, uploadURL string, err error) {
	if title == "" {
		return 0, "", errors.BadRequest("INVALID_PARAM", "标题不能为空")
	}
	t := &Track{UserID: userID, Title: title, Status: "processing"}
	if err := uc.repo.Create(ctx, t); err != nil {
		return 0, "", err
	}
	key := fmt.Sprintf("tracks/%d/original", t.ID)
	if err := uc.repo.UpdateUpload(ctx, t.ID, key); err != nil {
		return 0, "", err
	}
	uploadURL, err = uc.storage.PresignPut(ctx, key, 15*time.Minute)
	return t.ID, uploadURL, err
}

// CompleteUpload: 确认文件真的在 MinIO 里之后，把耗时的转码工作丢进队列，立即返回。
// track 和 media 之间用"异步消息"而不是同步 RPC —— 转码几十秒，同步调用会把两个服务锁死在一起。
func (uc *TrackUsecase) CompleteUpload(ctx context.Context, userID, id uint64) error {
	t, err := uc.repo.Get(ctx, id)
	if err != nil {
		return errors.NotFound("TRACK_NOT_FOUND", "作品不存在")
	}
	// 认证(是谁)在中间件做，授权(能动谁的数据)必须在业务层做，这两件事经常被混为一谈
	if t.UserID != userID {
		return errors.Forbidden("NOT_OWNER", "只能操作自己的作品")
	}
	ok, err := uc.storage.Exists(ctx, t.ObjectKey) // 防止客户端没传文件就喊"完成"
	if err != nil {
		return err
	}
	if !ok {
		return errors.BadRequest("FILE_NOT_UPLOADED", "文件尚未上传")
	}
	payload, _ := json.Marshal(task.ProcessAudioPayload{TrackID: id, ObjectKey: t.ObjectKey})
	_, err = uc.queue.EnqueueContext(ctx, asynq.NewTask(task.TypeProcessAudio, payload))
	return err
}

func (uc *TrackUsecase) Get(ctx context.Context, id uint64) (t *Track, author, streamURL string, err error) {
	t, err = uc.repo.Get(ctx, id)
	if err != nil {
		return nil, "", "", errors.NotFound("TRACK_NOT_FOUND", "作品不存在")
	}
	_ = uc.repo.IncrPlay(ctx, id) // 播放计数进 Redis，失败不影响主流程

	// 微服务的代价现场：单体里一个 JOIN 的事，这里是一次网络调用，
	// 所以要考虑"user 服务挂了怎么办" —— 这里选择降级(author 留空)，页面照样能开
	if u, uerr := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: t.UserID}); uerr == nil {
		author = u.Username
	}
	if t.Status == "ready" {
		streamURL, _ = uc.storage.PresignGet(ctx, t.ObjectKey, time.Hour) // 失败同样降级
	}
	return t, author, streamURL, nil
}

func (uc *TrackUsecase) List(ctx context.Context, page, size int) ([]*Track, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return uc.repo.List(ctx, (page-1)*size, size)
}

func (uc *TrackUsecase) ReportProcessed(ctx context.Context, id uint64, success bool, durationSec float64, peaks []float32) error {
	return uc.repo.UpdateProcessed(ctx, id, success, durationSec, peaks)
}
