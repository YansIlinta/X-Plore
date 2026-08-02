package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/hibiken/asynq"

	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/pkg/task"
)

// Video 稿件领域模型。状态机：uploading → processing → ready | failed
type Video struct {
	ID          uint64
	UserID      uint64
	Title       string
	Description string
	Category    string
	Status      string
	ObjectKey   string // 原片
	PlaylistKey string // HLS m3u8
	CoverKey    string
	DurationSec float64
	PlayCount   int64
}

type ObjectStorage interface {
	PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error)
	PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error)
	Exists(ctx context.Context, key string) (bool, error)
	// PublicURL 返回可匿名 GET 的对象 URL（用于 HLS 分片可跟随 m3u8）。
	// 未配置公共前缀时返回空串，调用方应回退 PresignGet。
	PublicURL(key string) string
}

type Danmu struct {
	MsgID     string
	UserID    uint64
	Content   string
	OffsetMS  int64
	CreatedAt int64 // unix ms
}

type Comment struct {
	ID        uint64
	UserID    uint64
	Content   string
	CreatedAt int64
}

type InteractStats struct {
	LikeCount     int64
	CommentCount  int64
	FavoriteCount int64
	Liked         bool
	Favorited     bool
}

type VideoRepo interface {
	Create(ctx context.Context, v *Video) error
	Get(ctx context.Context, id uint64) (*Video, error)
	ListReady(ctx context.Context, category string, userID uint64, sort string, offset, limit int) ([]*Video, int64, error)
	ListRelated(ctx context.Context, id uint64, category string, limit int) ([]*Video, error)
	ListByUser(ctx context.Context, userID uint64, offset, limit int) ([]*Video, int64, error)
	UpdateUpload(ctx context.Context, id uint64, objectKey string) error
	MarkProcessing(ctx context.Context, id uint64) error
	UpdateProcessed(ctx context.Context, id uint64, success bool, durationSec float64, playlistKey, coverKey, errMsg string) error
	IncrPlay(ctx context.Context, id uint64) error
	InsertDanmu(ctx context.Context, videoID, userID uint64, msgID, content string, offsetMS int64) error
	ListDanmu(ctx context.Context, videoID uint64, fromMS, toMS int64, limit int) ([]*Danmu, error)
	InsertComment(ctx context.Context, videoID, userID uint64, content string) (uint64, error)
	ListComments(ctx context.Context, videoID uint64, offset, limit int) ([]*Comment, int64, error)
	CountComments(ctx context.Context, videoID uint64) (int64, error)
	ToggleLike(ctx context.Context, videoID, userID uint64) (liked bool, count int64, err error)
	CountLikes(ctx context.Context, videoID uint64) (int64, error)
	HasLike(ctx context.Context, videoID, userID uint64) (bool, error)
	ToggleFavorite(ctx context.Context, videoID, userID uint64) (favorited bool, count int64, err error)
	CountFavorites(ctx context.Context, videoID uint64) (int64, error)
	HasFavorite(ctx context.Context, videoID, userID uint64) (bool, error)
}

type VideoUsecase struct {
	repo      VideoRepo
	userCli   userv1.UserClient
	queue     *asynq.Client
	storage   ObjectStorage
	danmuWS   string
}

func NewVideoUsecase(repo VideoRepo, userCli userv1.UserClient, queue *asynq.Client, storage ObjectStorage, danmuWS string) *VideoUsecase {
	return &VideoUsecase{repo: repo, userCli: userCli, queue: queue, storage: storage, danmuWS: danmuWS}
}

func RoomID(videoID uint64) string {
	return strconv.FormatUint(videoID, 10)
}

func (uc *VideoUsecase) Create(ctx context.Context, userID uint64, title, description, category string) (id uint64, uploadURL, roomID string, err error) {
	if title == "" {
		return 0, "", "", errors.BadRequest("INVALID_PARAM", "标题不能为空")
	}
	if category == "" {
		category = "general"
	}
	v := &Video{
		UserID:      userID,
		Title:       title,
		Description: description,
		Category:    category,
		Status:      "uploading",
	}
	if err := uc.repo.Create(ctx, v); err != nil {
		return 0, "", "", err
	}
	key := fmt.Sprintf("videos/%d/original", v.ID)
	if err := uc.repo.UpdateUpload(ctx, v.ID, key); err != nil {
		return 0, "", "", err
	}
	uploadURL, err = uc.storage.PresignPut(ctx, key, 30*time.Minute)
	if err != nil {
		return 0, "", "", err
	}
	return v.ID, uploadURL, RoomID(v.ID), nil
}

func (uc *VideoUsecase) CompleteUpload(ctx context.Context, userID, id uint64) error {
	v, err := uc.repo.Get(ctx, id)
	if err != nil {
		return errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	if v.UserID != userID {
		return errors.Forbidden("NOT_OWNER", "只能操作自己的稿件")
	}
	ok, err := uc.storage.Exists(ctx, v.ObjectKey)
	if err != nil {
		return err
	}
	if !ok {
		return errors.BadRequest("FILE_NOT_UPLOADED", "文件尚未上传")
	}
	if err := uc.repo.MarkProcessing(ctx, id); err != nil {
		return err
	}
	payload, _ := json.Marshal(task.ProcessVideoPayload{VideoID: id, ObjectKey: v.ObjectKey})
	_, err = uc.queue.EnqueueContext(ctx, asynq.NewTask(task.TypeProcessVideo, payload))
	return err
}

// GetDetail 详情：媒体 URL + 互动计数（viewerID=0 表示匿名）。
func (uc *VideoUsecase) GetDetail(ctx context.Context, id, viewerID uint64) (v *Video, author, coverURL, playlistURL, roomID, danmuWS string, stats InteractStats, err error) {
	v, err = uc.repo.Get(ctx, id)
	if err != nil {
		return nil, "", "", "", "", "", stats, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	_ = uc.repo.IncrPlay(ctx, id)
	if u, uerr := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: v.UserID}); uerr == nil {
		author = u.Username
	}
	roomID = RoomID(v.ID)
	danmuWS = uc.danmuWS
	if v.Status == "ready" {
		if v.CoverKey != "" {
			if u := uc.storage.PublicURL(v.CoverKey); u != "" {
				coverURL = u
			} else {
				coverURL, _ = uc.storage.PresignGet(ctx, v.CoverKey, time.Hour)
			}
		}
		if v.PlaylistKey != "" {
			if u := uc.storage.PublicURL(v.PlaylistKey); u != "" {
				playlistURL = u
			} else {
				playlistURL, _ = uc.storage.PresignGet(ctx, v.PlaylistKey, time.Hour)
			}
		}
	}
	stats, _ = uc.GetInteractStats(ctx, id, viewerID)
	return v, author, coverURL, playlistURL, roomID, danmuWS, stats, nil
}

func (uc *VideoUsecase) Get(ctx context.Context, id uint64) (v *Video, author, coverURL, playlistURL, roomID, danmuWS string, err error) {
	v, author, coverURL, playlistURL, roomID, danmuWS, _, err = uc.GetDetail(ctx, id, 0)
	return
}

// List 公开稿件列表:category/userID 过滤,sort=hot 按 play_count+3*like 加权,默认最新。
func (uc *VideoUsecase) List(ctx context.Context, category string, userID uint64, sort string, page, size int) ([]*Video, int64, error) {
	page, size = normalizePage(page, size)
	return uc.repo.ListReady(ctx, category, userID, sort, (page-1)*size, size)
}

// ListRelated 同分区按热度推荐,排除自身(播放页侧栏)。
func (uc *VideoUsecase) ListRelated(ctx context.Context, id uint64, limit int) ([]*Video, error) {
	if limit <= 0 || limit > 30 {
		limit = 12
	}
	v, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	return uc.repo.ListRelated(ctx, id, v.Category, limit)
}

// CardMeta 补齐列表卡片所需的封面 URL 与 UP 名(作者按 userID 去重后调 user 服务)。
func (uc *VideoUsecase) CardMeta(ctx context.Context, list []*Video) (covers, authors []string) {
	covers = make([]string, len(list))
	authors = make([]string, len(list))
	names := map[uint64]string{}
	for i, v := range list {
		if v.CoverKey != "" {
			if u := uc.storage.PublicURL(v.CoverKey); u != "" {
				covers[i] = u
			} else {
				covers[i], _ = uc.storage.PresignGet(ctx, v.CoverKey, time.Hour)
			}
		}
		name, ok := names[v.UserID]
		if !ok {
			if u, uerr := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: v.UserID}); uerr == nil {
				name = u.Username
			} else {
				name = strconv.FormatUint(v.UserID, 10)
			}
			names[v.UserID] = name
		}
		authors[i] = name
	}
	return covers, authors
}

func (uc *VideoUsecase) ListMine(ctx context.Context, userID uint64, page, size int) ([]*Video, int64, error) {
	if userID == 0 {
		return nil, 0, errors.Unauthorized("NO_USER", "未登录")
	}
	page, size = normalizePage(page, size)
	return uc.repo.ListByUser(ctx, userID, (page-1)*size, size)
}

func (uc *VideoUsecase) ReportProcessed(ctx context.Context, id uint64, success bool, durationSec float64, playlistKey, coverKey, errMsg string) error {
	return uc.repo.UpdateProcessed(ctx, id, success, durationSec, playlistKey, coverKey, errMsg)
}

// ListDanmu 按播放进度区间拉历史弹幕（匿名可读）。
func (uc *VideoUsecase) ListDanmu(ctx context.Context, videoID uint64, fromMS, toMS int64, limit int) ([]*Danmu, error) {
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return nil, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	if fromMS < 0 {
		fromMS = 0
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	return uc.repo.ListDanmu(ctx, videoID, fromMS, toMS, limit)
}

func (uc *VideoUsecase) ListComments(ctx context.Context, videoID uint64, page, size int) ([]*Comment, []string, int64, error) {
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return nil, nil, 0, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	page, size = normalizePage(page, size)
	list, total, err := uc.repo.ListComments(ctx, videoID, (page-1)*size, size)
	if err != nil {
		return nil, nil, 0, err
	}
	authors := make([]string, len(list))
	for i, c := range list {
		if u, uerr := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: c.UserID}); uerr == nil {
			authors[i] = u.Username
		} else {
			authors[i] = strconv.FormatUint(c.UserID, 10)
		}
	}
	return list, authors, total, nil
}

func (uc *VideoUsecase) PostComment(ctx context.Context, userID, videoID uint64, content string) (uint64, error) {
	if userID == 0 {
		return 0, errors.Unauthorized("NO_USER", "未登录")
	}
	content = strings.TrimSpace(content)
	if content == "" {
		return 0, errors.BadRequest("INVALID_PARAM", "内容不能为空")
	}
	if len([]rune(content)) > 500 {
		return 0, errors.BadRequest("INVALID_PARAM", "内容过长")
	}
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return 0, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	return uc.repo.InsertComment(ctx, videoID, userID, content)
}

func (uc *VideoUsecase) ToggleLike(ctx context.Context, userID, videoID uint64) (bool, int64, error) {
	if userID == 0 {
		return false, 0, errors.Unauthorized("NO_USER", "未登录")
	}
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return false, 0, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	return uc.repo.ToggleLike(ctx, videoID, userID)
}

func (uc *VideoUsecase) ToggleFavorite(ctx context.Context, userID, videoID uint64) (bool, int64, error) {
	if userID == 0 {
		return false, 0, errors.Unauthorized("NO_USER", "未登录")
	}
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return false, 0, errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	return uc.repo.ToggleFavorite(ctx, videoID, userID)
}

func (uc *VideoUsecase) GetInteractStats(ctx context.Context, videoID, userID uint64) (InteractStats, error) {
	var s InteractStats
	s.LikeCount, _ = uc.repo.CountLikes(ctx, videoID)
	s.CommentCount, _ = uc.repo.CountComments(ctx, videoID)
	s.FavoriteCount, _ = uc.repo.CountFavorites(ctx, videoID)
	s.Liked, _ = uc.repo.HasLike(ctx, videoID, userID)
	s.Favorited, _ = uc.repo.HasFavorite(ctx, videoID, userID)
	return s, nil
}

// PostDanmu 登录用户落库一条点播弹幕（实时广播由客户端另走 comet WS）。
func (uc *VideoUsecase) PostDanmu(ctx context.Context, userID, videoID uint64, content string, offsetMS int64) (msgID string, err error) {
	if userID == 0 {
		return "", errors.Unauthorized("NO_USER", "未登录")
	}
	content = strings.TrimSpace(content)
	if content == "" {
		return "", errors.BadRequest("INVALID_PARAM", "内容不能为空")
	}
	if len([]rune(content)) > 200 {
		return "", errors.BadRequest("INVALID_PARAM", "内容过长")
	}
	if offsetMS < 0 {
		offsetMS = 0
	}
	if _, err := uc.repo.Get(ctx, videoID); err != nil {
		return "", errors.NotFound("VIDEO_NOT_FOUND", "视频不存在")
	}
	msgID = fmt.Sprintf("v%d-%d-%d", videoID, userID, time.Now().UnixNano())
	if err := uc.repo.InsertDanmu(ctx, videoID, userID, msgID, content, offsetMS); err != nil {
		return "", err
	}
	return msgID, nil
}

func normalizePage(page, size int) (int, int) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return page, size
}
