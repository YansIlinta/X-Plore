package biz

import (
	"context"
	"strconv"
	"strings"

	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
)

// Hit 搜索命中(列表卡片所需字段)
type Hit struct {
	ID          uint64
	Title       string
	Description string
	Category    string
	UserID      uint64
	Author      string
	DurationSec float64
	PlayCount   int64
	CoverKey    string
	CoverURL    string
	CreatedAt   int64 // unix ms
}

// Searcher 检索抽象:V2 默认 PG ILIKE 读旁路;ES 实现替换此接口即可,契约不变。
type Searcher interface {
	SearchVideos(ctx context.Context, q, category string, offset, limit int) ([]*Hit, int64, error)
	Suggest(ctx context.Context, q string, limit int) ([]string, error)
}

type SearchUsecase struct {
	searcher Searcher
	userCli  userv1.UserClient
	// coverBase MinIO 公共读前缀;拼封面 URL 无需对象存储凭证
	coverBase string
}

func NewSearchUsecase(searcher Searcher, userCli userv1.UserClient, coverBase string) *SearchUsecase {
	return &SearchUsecase{searcher: searcher, userCli: userCli, coverBase: strings.TrimRight(coverBase, "/")}
}

func (uc *SearchUsecase) SearchVideos(ctx context.Context, q, category string, page, size int) ([]*Hit, int64, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, 0, nil
	}
	if len([]rune(q)) > 100 {
		q = string([]rune(q)[:100])
	}
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 50 {
		size = 20
	}
	hits, total, err := uc.searcher.SearchVideos(ctx, q, category, (page-1)*size, size)
	if err != nil {
		return nil, 0, err
	}
	names := map[uint64]string{}
	for _, h := range hits {
		if h.CoverKey != "" && uc.coverBase != "" {
			h.CoverURL = uc.coverBase + "/" + strings.TrimLeft(h.CoverKey, "/")
		}
		name, ok := names[h.UserID]
		if !ok {
			if u, uerr := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: h.UserID}); uerr == nil {
				name = u.Username
			} else {
				name = strconv.FormatUint(h.UserID, 10)
			}
			names[h.UserID] = name
		}
		h.Author = name
	}
	return hits, total, nil
}

func (uc *SearchUsecase) Suggest(ctx context.Context, q string) ([]string, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, nil
	}
	if len([]rune(q)) > 50 {
		q = string([]rune(q)[:50])
	}
	return uc.searcher.Suggest(ctx, q, 10)
}
