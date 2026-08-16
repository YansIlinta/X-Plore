// model 层 = 数据库表结构。GORM 通过 struct tag 映射列。
package model

import (
	"time"

	"gorm.io/datatypes"
)

type User struct {
	ID           uint64 `gorm:"primaryKey"`
	Username     string `gorm:"uniqueIndex;size:32;not null"`
	PasswordHash string `gorm:"size:128;not null"` // 存 bcrypt 哈希，绝不存明文
	CreatedAt    time.Time
}

// Track 是一个音乐作品。设计要点：
//   - 音频文件本体在 MinIO，这里只存 object key（见 ARCHITECTURE.md 第 4 节）
//   - Peaks 存波形峰值数组(JSONB)，是前端可视化的数据源
//   - ProjectData 存编辑器工程文件(轨道/剪辑点)，JSONB 的典型用法
//   - PlayCount 平时在 Redis 累加，定时刷回来，所以这里的值允许轻微滞后
type Track struct {
	ID          uint64 `gorm:"primaryKey"`
	UserID      uint64 `gorm:"index;not null"`
	User        User   `gorm:"foreignKey:UserID"`
	Title       string `gorm:"size:120;not null"`
	Status      string `gorm:"size:16;default:processing;index"` // processing / ready / failed
	ObjectKey   string `gorm:"size:256"`                         // MinIO 中的原始文件
	StreamKey   string `gorm:"size:256"`                         // 转码后的播放文件
	DurationSec float64
	Peaks       datatypes.JSON `gorm:"type:jsonb"` // [0.12, 0.87, ...] 归一化振幅
	ProjectData datatypes.JSON `gorm:"type:jsonb"` // 编辑器工程(可为空)
	PlayCount   int64          `gorm:"default:0"`
	CreatedAt   time.Time
}
