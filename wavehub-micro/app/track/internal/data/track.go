package data

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub-micro/app/track/internal/biz"
)

// 注意：这张表只有 track 服务能读写。user 服务的表长什么样，这里不知道也不关心 ——
// 需要用户数据就走 gRPC。生产环境会进一步做到"每个服务一个独立数据库实例"。
type trackModel struct {
	ID          uint64 `gorm:"primaryKey"`
	UserID      uint64 `gorm:"index;not null"`
	Title       string `gorm:"size:120;not null"`
	Status      string `gorm:"size:16;default:processing;index"`
	ObjectKey   string `gorm:"size:256"`
	DurationSec float64
	Peaks       datatypes.JSON `gorm:"type:jsonb"`
	PlayCount   int64          `gorm:"default:0"`
	CreatedAt   time.Time
}

func (trackModel) TableName() string { return "tracks" }

func NewDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	if err := db.AutoMigrate(&trackModel{}); err != nil {
		log.Fatalf("建表失败: %v", err)
	}
	return db
}

type trackRepo struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewTrackRepo(db *gorm.DB, rdb *redis.Client) biz.TrackRepo {
	return &trackRepo{db: db, rdb: rdb}
}

func (r *trackRepo) Create(ctx context.Context, t *biz.Track) error {
	m := trackModel{UserID: t.UserID, Title: t.Title, Status: t.Status}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return err
	}
	t.ID = m.ID
	return nil
}

func (r *trackRepo) Get(ctx context.Context, id uint64) (*biz.Track, error) {
	var m trackModel
	if err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return toBiz(&m), nil
}

func (r *trackRepo) List(ctx context.Context, offset, limit int) ([]*biz.Track, error) {
	var ms []trackModel
	err := r.db.WithContext(ctx).
		Select("id, user_id, title, status, duration_sec, play_count, created_at"). // 列表不取 peaks 大字段
		Where("status = ?", "ready").
		Order("created_at DESC").
		Offset(offset).Limit(limit).
		Find(&ms).Error
	if err != nil {
		return nil, err
	}
	out := make([]*biz.Track, 0, len(ms))
	for i := range ms {
		out = append(out, toBiz(&ms[i]))
	}
	return out, nil
}

func (r *trackRepo) UpdateUpload(ctx context.Context, id uint64, objectKey string) error {
	res := r.db.WithContext(ctx).Model(&trackModel{}).Where("id = ?", id).
		Update("object_key", objectKey)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *trackRepo) UpdateProcessed(ctx context.Context, id uint64, success bool, durationSec float64, peaks []float32) error {
	if !success {
		return r.db.WithContext(ctx).Model(&trackModel{}).Where("id = ?", id).
			Update("status", "failed").Error
	}
	peaksJSON, _ := json.Marshal(peaks)
	return r.db.WithContext(ctx).Model(&trackModel{}).Where("id = ?", id).
		Updates(map[string]any{
			"status":       "ready",
			"duration_sec": durationSec,
			"peaks":        datatypes.JSON(peaksJSON),
		}).Error
}

func (r *trackRepo) IncrPlay(ctx context.Context, id uint64) error {
	// 热数据进 Redis，由定时任务批量刷回 PostgreSQL(骨架未实现，见文档"下一步")
	return r.rdb.Incr(ctx, fmt.Sprintf("play:%d", id)).Err()
}

func toBiz(m *trackModel) *biz.Track {
	t := &biz.Track{
		ID: m.ID, UserID: m.UserID, Title: m.Title, Status: m.Status,
		ObjectKey: m.ObjectKey, DurationSec: m.DurationSec, PlayCount: m.PlayCount,
	}
	if len(m.Peaks) > 0 {
		_ = json.Unmarshal(m.Peaks, &t.Peaks)
	}
	return t
}
