// data 层 = 数据访问（对应单体里的 model + 数据库操作）。
// 表结构(userModel)是本层私有的，对外只暴露 biz.UserRepo 接口的实现。
package data

import (
	"context"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub-micro/app/user/internal/biz"
)

type userModel struct {
	ID           uint64 `gorm:"primaryKey"`
	Username     string `gorm:"uniqueIndex;size:32;not null"`
	PasswordHash string `gorm:"size:128;not null"`
	CreatedAt    time.Time
}

func (userModel) TableName() string { return "users" }

func NewDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	if err := db.AutoMigrate(&userModel{}); err != nil {
		log.Fatalf("建表失败: %v", err)
	}
	return db
}

type userRepo struct{ db *gorm.DB }

func NewUserRepo(db *gorm.DB) biz.UserRepo { return &userRepo{db: db} }

func (r *userRepo) Create(ctx context.Context, u *biz.User) error {
	m := userModel{Username: u.Username, PasswordHash: u.PasswordHash}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return err
	}
	u.ID = m.ID
	return nil
}

func (r *userRepo) GetByUsername(ctx context.Context, username string) (*biz.User, error) {
	var m userModel
	if err := r.db.WithContext(ctx).First(&m, "username = ?", username).Error; err != nil {
		return nil, err
	}
	return toBiz(&m), nil
}

func (r *userRepo) GetByID(ctx context.Context, id uint64) (*biz.User, error) {
	var m userModel
	if err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return toBiz(&m), nil
}

func toBiz(m *userModel) *biz.User {
	return &biz.User{ID: m.ID, Username: m.Username, PasswordHash: m.PasswordHash}
}
