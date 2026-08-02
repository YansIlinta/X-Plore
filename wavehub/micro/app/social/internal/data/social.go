package data

import (
	"context"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub-micro/app/social/internal/biz"
)

// user_follows:关注关系,(follower, followee) 唯一
type followModel struct {
	ID         uint64 `gorm:"primaryKey"`
	FollowerID uint64 `gorm:"uniqueIndex:uk_follow,priority:1;index;not null"`
	FolloweeID uint64 `gorm:"uniqueIndex:uk_follow,priority:2;index;not null"`
	CreatedAt  time.Time
}

func (followModel) TableName() string { return "user_follows" }

func NewDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	if err := db.AutoMigrate(&followModel{}); err != nil {
		log.Fatalf("建表失败: %v", err)
	}
	return db
}

type followRepo struct {
	db *gorm.DB
}

func NewFollowRepo(db *gorm.DB) biz.FollowRepo {
	return &followRepo{db: db}
}

func (r *followRepo) Toggle(ctx context.Context, followerID, followeeID uint64) (bool, error) {
	var existing followModel
	err := r.db.WithContext(ctx).Where("follower_id = ? AND followee_id = ?", followerID, followeeID).First(&existing).Error
	if err == nil {
		return false, r.db.WithContext(ctx).Delete(&existing).Error
	}
	if err != gorm.ErrRecordNotFound {
		return false, err
	}
	return true, r.db.WithContext(ctx).Create(&followModel{FollowerID: followerID, FolloweeID: followeeID}).Error
}

func (r *followRepo) IsFollowing(ctx context.Context, followerID, followeeID uint64) (bool, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&followModel{}).
		Where("follower_id = ? AND followee_id = ?", followerID, followeeID).Count(&n).Error
	return n > 0, err
}

func (r *followRepo) CountFollowers(ctx context.Context, userID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&followModel{}).Where("followee_id = ?", userID).Count(&n).Error
	return n, err
}

func (r *followRepo) CountFollowings(ctx context.Context, userID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&followModel{}).Where("follower_id = ?", userID).Count(&n).Error
	return n, err
}

func (r *followRepo) listEdges(ctx context.Context, where string, userID uint64, pick func(m *followModel) uint64, offset, limit int) ([]*biz.FollowEdge, int64, error) {
	q := r.db.WithContext(ctx).Model(&followModel{}).Where(where, userID)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var ms []followModel
	if err := q.Order("id DESC").Offset(offset).Limit(limit).Find(&ms).Error; err != nil {
		return nil, 0, err
	}
	out := make([]*biz.FollowEdge, 0, len(ms))
	for i := range ms {
		out = append(out, &biz.FollowEdge{UserID: pick(&ms[i]), CreatedAt: ms[i].CreatedAt.UnixMilli()})
	}
	return out, total, nil
}

func (r *followRepo) ListFollowings(ctx context.Context, userID uint64, offset, limit int) ([]*biz.FollowEdge, int64, error) {
	return r.listEdges(ctx, "follower_id = ?", userID, func(m *followModel) uint64 { return m.FolloweeID }, offset, limit)
}

func (r *followRepo) ListFollowers(ctx context.Context, userID uint64, offset, limit int) ([]*biz.FollowEdge, int64, error) {
	return r.listEdges(ctx, "followee_id = ?", userID, func(m *followModel) uint64 { return m.FollowerID }, offset, limit)
}
