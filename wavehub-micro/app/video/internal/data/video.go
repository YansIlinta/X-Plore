package data

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub-micro/app/video/internal/biz"
)

type videoModel struct {
	ID          uint64 `gorm:"primaryKey"`
	UserID      uint64 `gorm:"index;not null"`
	Title       string `gorm:"size:200;not null"`
	Description string `gorm:"type:text"`
	Category    string `gorm:"size:32;index;default:general"`
	Status      string `gorm:"size:16;default:uploading;index"`
	ObjectKey   string `gorm:"size:512"`
	PlaylistKey string `gorm:"size:512"`
	CoverKey    string `gorm:"size:512"`
	DurationSec float64
	PlayCount   int64 `gorm:"default:0"`
	ErrorMsg    string `gorm:"size:512"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (videoModel) TableName() string { return "videos" }

func NewDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	if err := db.AutoMigrate(&videoModel{}, &danmuModel{}, &commentModel{}, &likeModel{}, &favoriteModel{}); err != nil {
		log.Fatalf("建表失败: %v", err)
	}
	return db
}

// 点播历史弹幕（PG）。直播海量走 ClickHouse consumer；点播按稿件量级用 PG 更简单可运营。
type danmuModel struct {
	ID        uint64 `gorm:"primaryKey"`
	VideoID   uint64 `gorm:"index:idx_video_offset,priority:1;not null"`
	UserID    uint64 `gorm:"index;not null"`
	MsgID     string `gorm:"size:64;uniqueIndex;not null"`
	Content   string `gorm:"size:500;not null"`
	OffsetMS  int64  `gorm:"index:idx_video_offset,priority:2;not null"`
	CreatedAt time.Time
}

func (danmuModel) TableName() string { return "video_danmus" }

type commentModel struct {
	ID        uint64 `gorm:"primaryKey"`
	VideoID   uint64 `gorm:"index;not null"`
	UserID    uint64 `gorm:"index;not null"`
	Content   string `gorm:"size:1000;not null"`
	CreatedAt time.Time
}

func (commentModel) TableName() string { return "video_comments" }

// 用户-视频 点赞唯一
type likeModel struct {
	ID        uint64 `gorm:"primaryKey"`
	VideoID   uint64 `gorm:"uniqueIndex:uk_like_vu,priority:1;not null"`
	UserID    uint64 `gorm:"uniqueIndex:uk_like_vu,priority:2;not null"`
	CreatedAt time.Time
}

func (likeModel) TableName() string { return "video_likes" }

type favoriteModel struct {
	ID        uint64 `gorm:"primaryKey"`
	VideoID   uint64 `gorm:"uniqueIndex:uk_fav_vu,priority:1;not null"`
	UserID    uint64 `gorm:"uniqueIndex:uk_fav_vu,priority:2;not null"`
	CreatedAt time.Time
}

func (favoriteModel) TableName() string { return "video_favorites" }

type videoRepo struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewVideoRepo(db *gorm.DB, rdb *redis.Client) biz.VideoRepo {
	return &videoRepo{db: db, rdb: rdb}
}

func (r *videoRepo) Create(ctx context.Context, v *biz.Video) error {
	m := videoModel{
		UserID: v.UserID, Title: v.Title, Description: v.Description,
		Category: v.Category, Status: v.Status,
	}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return err
	}
	v.ID = m.ID
	return nil
}

func (r *videoRepo) Get(ctx context.Context, id uint64) (*biz.Video, error) {
	var m videoModel
	if err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return toBiz(&m), nil
}

// hotScore 热度加权:播放 + 3*点赞(相关子查询,MVP 量级足够;大表演进为物化列/离线算分)
const hotScore = "play_count + 3*(SELECT COUNT(*) FROM video_likes WHERE video_likes.video_id = videos.id)"

func (r *videoRepo) ListReady(ctx context.Context, category string, userID uint64, sort string, offset, limit int) ([]*biz.Video, int64, error) {
	q := r.db.WithContext(ctx).Model(&videoModel{}).Where("status = ?", "ready")
	if category != "" {
		q = q.Where("category = ?", category)
	}
	if userID != 0 {
		q = q.Where("user_id = ?", userID)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	order := "created_at DESC"
	if sort == "hot" {
		order = hotScore + " DESC, created_at DESC"
	}
	var ms []videoModel
	err := q.Order(order).Offset(offset).Limit(limit).Find(&ms).Error
	if err != nil {
		return nil, 0, err
	}
	return toBizList(ms), total, nil
}

func (r *videoRepo) ListRelated(ctx context.Context, id uint64, category string, limit int) ([]*biz.Video, error) {
	var ms []videoModel
	err := r.db.WithContext(ctx).Model(&videoModel{}).
		Where("status = ? AND category = ? AND id <> ?", "ready", category, id).
		Order(hotScore + " DESC, created_at DESC").Limit(limit).Find(&ms).Error
	if err != nil {
		return nil, err
	}
	return toBizList(ms), nil
}

func (r *videoRepo) ListByUser(ctx context.Context, userID uint64, offset, limit int) ([]*biz.Video, int64, error) {
	q := r.db.WithContext(ctx).Model(&videoModel{}).Where("user_id = ?", userID)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var ms []videoModel
	err := q.Order("created_at DESC").Offset(offset).Limit(limit).Find(&ms).Error
	if err != nil {
		return nil, 0, err
	}
	return toBizList(ms), total, nil
}

func (r *videoRepo) UpdateUpload(ctx context.Context, id uint64, objectKey string) error {
	res := r.db.WithContext(ctx).Model(&videoModel{}).Where("id = ?", id).
		Updates(map[string]any{"object_key": objectKey, "status": "uploading"})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *videoRepo) MarkProcessing(ctx context.Context, id uint64) error {
	return r.db.WithContext(ctx).Model(&videoModel{}).Where("id = ?", id).
		Update("status", "processing").Error
}

func (r *videoRepo) UpdateProcessed(ctx context.Context, id uint64, success bool, durationSec float64, playlistKey, coverKey, errMsg string) error {
	if !success {
		return r.db.WithContext(ctx).Model(&videoModel{}).Where("id = ?", id).
			Updates(map[string]any{"status": "failed", "error_msg": truncate(errMsg, 500)}).Error
	}
	return r.db.WithContext(ctx).Model(&videoModel{}).Where("id = ?", id).
		Updates(map[string]any{
			"status":        "ready",
			"duration_sec":  durationSec,
			"playlist_key":  playlistKey,
			"cover_key":     coverKey,
			"error_msg":     "",
		}).Error
}

func (r *videoRepo) IncrPlay(ctx context.Context, id uint64) error {
	// PG 计数为准(列表/热度排序读它);Redis 计数保留作实时热榜缓存位
	if err := r.db.WithContext(ctx).Model(&videoModel{}).Where("id = ?", id).
		UpdateColumn("play_count", gorm.Expr("play_count + 1")).Error; err != nil {
		return err
	}
	return r.rdb.Incr(ctx, fmt.Sprintf("video:play:%d", id)).Err()
}

func (r *videoRepo) InsertDanmu(ctx context.Context, videoID, userID uint64, msgID, content string, offsetMS int64) error {
	m := danmuModel{
		VideoID: videoID, UserID: userID, MsgID: msgID,
		Content: content, OffsetMS: offsetMS,
	}
	return r.db.WithContext(ctx).Create(&m).Error
}

func (r *videoRepo) ListDanmu(ctx context.Context, videoID uint64, fromMS, toMS int64, limit int) ([]*biz.Danmu, error) {
	q := r.db.WithContext(ctx).Model(&danmuModel{}).
		Where("video_id = ? AND offset_ms >= ?", videoID, fromMS)
	if toMS > 0 {
		q = q.Where("offset_ms <= ?", toMS)
	}
	var ms []danmuModel
	err := q.Order("offset_ms ASC, id ASC").Limit(limit).Find(&ms).Error
	if err != nil {
		return nil, err
	}
	out := make([]*biz.Danmu, 0, len(ms))
	for _, m := range ms {
		out = append(out, &biz.Danmu{
			MsgID: m.MsgID, UserID: m.UserID, Content: m.Content,
			OffsetMS: m.OffsetMS, CreatedAt: m.CreatedAt.UnixMilli(),
		})
	}
	return out, nil
}

func (r *videoRepo) InsertComment(ctx context.Context, videoID, userID uint64, content string) (uint64, error) {
	m := commentModel{VideoID: videoID, UserID: userID, Content: content}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return 0, err
	}
	return m.ID, nil
}

func (r *videoRepo) ListComments(ctx context.Context, videoID uint64, offset, limit int) ([]*biz.Comment, int64, error) {
	q := r.db.WithContext(ctx).Model(&commentModel{}).Where("video_id = ?", videoID)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var ms []commentModel
	if err := q.Order("id DESC").Offset(offset).Limit(limit).Find(&ms).Error; err != nil {
		return nil, 0, err
	}
	out := make([]*biz.Comment, 0, len(ms))
	for _, m := range ms {
		out = append(out, &biz.Comment{
			ID: m.ID, UserID: m.UserID, Content: m.Content, CreatedAt: m.CreatedAt.UnixMilli(),
		})
	}
	return out, total, nil
}

func (r *videoRepo) CountComments(ctx context.Context, videoID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&commentModel{}).Where("video_id = ?", videoID).Count(&n).Error
	return n, err
}

func (r *videoRepo) ToggleLike(ctx context.Context, videoID, userID uint64) (liked bool, count int64, err error) {
	var existing likeModel
	err = r.db.WithContext(ctx).Where("video_id = ? AND user_id = ?", videoID, userID).First(&existing).Error
	if err == nil {
		if err := r.db.WithContext(ctx).Delete(&existing).Error; err != nil {
			return false, 0, err
		}
		liked = false
	} else if err == gorm.ErrRecordNotFound {
		if err := r.db.WithContext(ctx).Create(&likeModel{VideoID: videoID, UserID: userID}).Error; err != nil {
			return false, 0, err
		}
		liked = true
	} else {
		return false, 0, err
	}
	count, err = r.CountLikes(ctx, videoID)
	return liked, count, err
}

func (r *videoRepo) CountLikes(ctx context.Context, videoID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&likeModel{}).Where("video_id = ?", videoID).Count(&n).Error
	return n, err
}

func (r *videoRepo) HasLike(ctx context.Context, videoID, userID uint64) (bool, error) {
	if userID == 0 {
		return false, nil
	}
	var n int64
	err := r.db.WithContext(ctx).Model(&likeModel{}).Where("video_id = ? AND user_id = ?", videoID, userID).Count(&n).Error
	return n > 0, err
}

func (r *videoRepo) ToggleFavorite(ctx context.Context, videoID, userID uint64) (favorited bool, count int64, err error) {
	var existing favoriteModel
	err = r.db.WithContext(ctx).Where("video_id = ? AND user_id = ?", videoID, userID).First(&existing).Error
	if err == nil {
		if err := r.db.WithContext(ctx).Delete(&existing).Error; err != nil {
			return false, 0, err
		}
		favorited = false
	} else if err == gorm.ErrRecordNotFound {
		if err := r.db.WithContext(ctx).Create(&favoriteModel{VideoID: videoID, UserID: userID}).Error; err != nil {
			return false, 0, err
		}
		favorited = true
	} else {
		return false, 0, err
	}
	count, err = r.CountFavorites(ctx, videoID)
	return favorited, count, err
}

func (r *videoRepo) CountFavorites(ctx context.Context, videoID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&favoriteModel{}).Where("video_id = ?", videoID).Count(&n).Error
	return n, err
}

func (r *videoRepo) HasFavorite(ctx context.Context, videoID, userID uint64) (bool, error) {
	if userID == 0 {
		return false, nil
	}
	var n int64
	err := r.db.WithContext(ctx).Model(&favoriteModel{}).Where("video_id = ? AND user_id = ?", videoID, userID).Count(&n).Error
	return n > 0, err
}

func toBiz(m *videoModel) *biz.Video {
	return &biz.Video{
		ID: m.ID, UserID: m.UserID, Title: m.Title, Description: m.Description,
		Category: m.Category, Status: m.Status, ObjectKey: m.ObjectKey,
		PlaylistKey: m.PlaylistKey, CoverKey: m.CoverKey,
		DurationSec: m.DurationSec, PlayCount: m.PlayCount,
	}
}

func toBizList(ms []videoModel) []*biz.Video {
	out := make([]*biz.Video, 0, len(ms))
	for i := range ms {
		out = append(out, toBiz(&ms[i]))
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
