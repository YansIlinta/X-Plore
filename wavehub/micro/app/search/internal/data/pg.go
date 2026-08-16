// PG ILIKE 检索实现:共库只读 videos 表(读旁路,不写)。
// 演进:换 ES 实现 + outbox/binlog 同步,biz.Searcher 契约不变。
package data

import (
	"context"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/YansIlinta/wavehub-micro/app/search/internal/biz"
)

// orderExpr 带参数的排序表达式。注意 gorm 的 Order() 只认 clause.OrderBy/string,
// 直接传 gorm.Expr 会被静默忽略。
func orderExpr(sql string, vars ...any) clause.OrderBy {
	return clause.OrderBy{Expression: clause.Expr{SQL: sql, Vars: vars, WithoutParentheses: true}}
}

// videoRow 只映射检索需要的列;表归 video 服务,这里不做 AutoMigrate
type videoRow struct {
	ID          uint64
	Title       string
	Description string
	Category    string
	UserID      uint64
	DurationSec float64
	PlayCount   int64
	CoverKey    string
	CreatedAt   time.Time
}

func (videoRow) TableName() string { return "videos" }

func NewDB(dsn string) *gorm.DB {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	return db
}

type pgSearcher struct {
	db *gorm.DB
}

func NewPGSearcher(db *gorm.DB) biz.Searcher {
	return &pgSearcher{db: db}
}

func (s *pgSearcher) SearchVideos(ctx context.Context, q, category string, offset, limit int) ([]*biz.Hit, int64, error) {
	pattern := "%" + escapeLike(q) + "%"
	base := s.db.WithContext(ctx).Model(&videoRow{}).
		Where("status = ?", "ready").
		Where("(title ILIKE ? OR description ILIKE ?)", pattern, pattern)
	if category != "" {
		base = base.Where("category = ?", category)
	}
	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []videoRow
	// 标题命中优先于简介命中,再按热度
	err := base.
		Order(orderExpr("(title ILIKE ?) DESC, play_count DESC, created_at DESC", pattern)).
		Offset(offset).Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, 0, err
	}
	out := make([]*biz.Hit, 0, len(rows))
	for i := range rows {
		r := &rows[i]
		out = append(out, &biz.Hit{
			ID: r.ID, Title: r.Title, Description: r.Description, Category: r.Category,
			UserID: r.UserID, DurationSec: r.DurationSec, PlayCount: r.PlayCount,
			CoverKey: r.CoverKey, CreatedAt: r.CreatedAt.UnixMilli(),
		})
	}
	return out, total, nil
}

func (s *pgSearcher) Suggest(ctx context.Context, q string, limit int) ([]string, error) {
	pattern := "%" + escapeLike(q) + "%"
	prefix := escapeLike(q) + "%"
	var titles []string
	// 前缀命中排前,子串命中兜底;DISTINCT 与表达式排序在 PG 冲突,改 Go 侧去重
	err := s.db.WithContext(ctx).Model(&videoRow{}).
		Where("status = ? AND title ILIKE ?", "ready", pattern).
		Order(orderExpr("(title ILIKE ?) DESC, title", prefix)).
		Limit(limit * 2).Pluck("title", &titles).Error
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(titles))
	out := make([]string, 0, limit)
	for _, t := range titles {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// escapeLike 转义 LIKE 元字符,防止用户输入 % _ \ 干扰匹配
func escapeLike(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '%' || r == '_' || r == '\\' {
			out = append(out, '\\')
		}
		out = append(out, r)
	}
	return string(out)
}
