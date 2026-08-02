package service

import (
	"context"

	v1 "github.com/YansIlinta/wavehub-micro/api/search/v1"
	"github.com/YansIlinta/wavehub-micro/app/search/internal/biz"
)

type SearchService struct {
	v1.UnimplementedSearchServer
	uc *biz.SearchUsecase
}

func NewSearchService(uc *biz.SearchUsecase) *SearchService {
	return &SearchService{uc: uc}
}

func (s *SearchService) SearchVideos(ctx context.Context, req *v1.SearchVideosRequest) (*v1.SearchVideosReply, error) {
	hits, total, err := s.uc.SearchVideos(ctx, req.Q, req.Category, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	out := make([]*v1.SearchHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, &v1.SearchHit{
			Id: h.ID, Title: h.Title, Description: h.Description, Category: h.Category,
			UserId: h.UserID, Author: h.Author, DurationSec: h.DurationSec,
			PlayCount: h.PlayCount, CoverUrl: h.CoverURL, CreatedAt: h.CreatedAt,
		})
	}
	return &v1.SearchVideosReply{List: out, Total: int32(total)}, nil
}

func (s *SearchService) Suggest(ctx context.Context, req *v1.SuggestRequest) (*v1.SuggestReply, error) {
	list, err := s.uc.Suggest(ctx, req.Q)
	if err != nil {
		return nil, err
	}
	return &v1.SuggestReply{List: list}, nil
}
