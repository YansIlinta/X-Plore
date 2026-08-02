package service

import (
	"context"
	"strconv"

	v1 "github.com/YansIlinta/wavehub-micro/api/video/v1"
	"github.com/YansIlinta/wavehub-micro/app/video/internal/biz"
	mw "github.com/YansIlinta/wavehub-micro/pkg/authmw"
)

type VideoService struct {
	v1.UnimplementedVideoServer
	uc *biz.VideoUsecase
}

func NewVideoService(uc *biz.VideoUsecase) *VideoService {
	return &VideoService{uc: uc}
}

func (s *VideoService) CreateVideo(ctx context.Context, req *v1.CreateVideoRequest) (*v1.CreateVideoReply, error) {
	id, uploadURL, roomID, err := s.uc.Create(ctx, mw.UserIDFromContext(ctx), req.Title, req.Description, req.Category)
	if err != nil {
		return nil, err
	}
	return &v1.CreateVideoReply{Id: id, UploadUrl: uploadURL, RoomId: roomID}, nil
}

func (s *VideoService) CompleteUpload(ctx context.Context, req *v1.CompleteUploadRequest) (*v1.CompleteUploadReply, error) {
	if err := s.uc.CompleteUpload(ctx, mw.UserIDFromContext(ctx), req.Id); err != nil {
		return nil, err
	}
	return &v1.CompleteUploadReply{Status: "processing"}, nil
}

// cards 列表统一出参:补封面 + UP 名(卡片流必需)
func (s *VideoService) cards(ctx context.Context, list []*biz.Video) []*v1.VideoInfo {
	covers, authors := s.uc.CardMeta(ctx, list)
	out := make([]*v1.VideoInfo, 0, len(list))
	for i, v := range list {
		out = append(out, toProto(v, authors[i], covers[i], "", biz.RoomID(v.ID), "", biz.InteractStats{}))
	}
	return out
}

func (s *VideoService) ListVideos(ctx context.Context, req *v1.ListVideosRequest) (*v1.ListVideosReply, error) {
	list, total, err := s.uc.List(ctx, req.Category, req.UserId, req.Sort, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	return &v1.ListVideosReply{List: s.cards(ctx, list), Total: int32(total)}, nil
}

func (s *VideoService) ListRelated(ctx context.Context, req *v1.ListRelatedRequest) (*v1.ListVideosReply, error) {
	list, err := s.uc.ListRelated(ctx, req.Id, int(req.Limit))
	if err != nil {
		return nil, err
	}
	return &v1.ListVideosReply{List: s.cards(ctx, list), Total: int32(len(list))}, nil
}

func (s *VideoService) ListMyVideos(ctx context.Context, req *v1.ListMyVideosRequest) (*v1.ListVideosReply, error) {
	list, total, err := s.uc.ListMine(ctx, mw.UserIDFromContext(ctx), int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	return &v1.ListVideosReply{List: s.cards(ctx, list), Total: int32(total)}, nil
}

func (s *VideoService) GetVideo(ctx context.Context, req *v1.GetVideoRequest) (*v1.VideoInfo, error) {
	// 可选 JWT：有 token 时中间件会注入；匿名则 viewer=0
	viewer := mw.UserIDFromContext(ctx)
	v, author, cover, playlist, roomID, danmuWS, stats, err := s.uc.GetDetail(ctx, req.Id, viewer)
	if err != nil {
		return nil, err
	}
	return toProto(v, author, cover, playlist, roomID, danmuWS, stats), nil
}

func (s *VideoService) ReportProcessed(ctx context.Context, req *v1.ReportProcessedRequest) (*v1.ReportProcessedReply, error) {
	if err := s.uc.ReportProcessed(ctx, req.Id, req.Success, req.DurationSec, req.PlaylistKey, req.CoverKey, req.ErrorMessage); err != nil {
		return nil, err
	}
	return &v1.ReportProcessedReply{}, nil
}

func (s *VideoService) ListDanmu(ctx context.Context, req *v1.ListDanmuRequest) (*v1.ListDanmuReply, error) {
	list, err := s.uc.ListDanmu(ctx, req.Id, req.FromMs, req.ToMs, int(req.Limit))
	if err != nil {
		return nil, err
	}
	out := make([]*v1.DanmuItem, 0, len(list))
	for _, d := range list {
		out = append(out, &v1.DanmuItem{
			MsgId: d.MsgID, Uid: formatUID(d.UserID), Content: d.Content,
			OffsetMs: d.OffsetMS, CreatedAt: d.CreatedAt,
		})
	}
	return &v1.ListDanmuReply{List: out}, nil
}

func (s *VideoService) PostDanmu(ctx context.Context, req *v1.PostDanmuRequest) (*v1.PostDanmuReply, error) {
	msgID, err := s.uc.PostDanmu(ctx, mw.UserIDFromContext(ctx), req.Id, req.Content, req.OffsetMs)
	if err != nil {
		return nil, err
	}
	return &v1.PostDanmuReply{MsgId: msgID, OffsetMs: req.OffsetMs}, nil
}

func (s *VideoService) ListComments(ctx context.Context, req *v1.ListCommentsRequest) (*v1.ListCommentsReply, error) {
	list, authors, total, err := s.uc.ListComments(ctx, req.Id, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	out := make([]*v1.CommentItem, 0, len(list))
	for i, c := range list {
		out = append(out, &v1.CommentItem{
			Id: c.ID, UserId: c.UserID, Author: authors[i],
			Content: c.Content, CreatedAt: c.CreatedAt,
		})
	}
	return &v1.ListCommentsReply{List: out, Total: int32(total)}, nil
}

func (s *VideoService) PostComment(ctx context.Context, req *v1.PostCommentRequest) (*v1.PostCommentReply, error) {
	id, err := s.uc.PostComment(ctx, mw.UserIDFromContext(ctx), req.Id, req.Content)
	if err != nil {
		return nil, err
	}
	return &v1.PostCommentReply{Id: id}, nil
}

func (s *VideoService) ToggleLike(ctx context.Context, req *v1.ToggleLikeRequest) (*v1.ToggleLikeReply, error) {
	liked, count, err := s.uc.ToggleLike(ctx, mw.UserIDFromContext(ctx), req.Id)
	if err != nil {
		return nil, err
	}
	return &v1.ToggleLikeReply{Liked: liked, LikeCount: count}, nil
}

func (s *VideoService) ToggleFavorite(ctx context.Context, req *v1.ToggleFavoriteRequest) (*v1.ToggleFavoriteReply, error) {
	fav, count, err := s.uc.ToggleFavorite(ctx, mw.UserIDFromContext(ctx), req.Id)
	if err != nil {
		return nil, err
	}
	return &v1.ToggleFavoriteReply{Favorited: fav, FavoriteCount: count}, nil
}

func (s *VideoService) GetInteractStats(ctx context.Context, req *v1.GetInteractStatsRequest) (*v1.InteractStats, error) {
	st, err := s.uc.GetInteractStats(ctx, req.Id, mw.UserIDFromContext(ctx))
	if err != nil {
		return nil, err
	}
	return &v1.InteractStats{
		LikeCount: st.LikeCount, CommentCount: st.CommentCount, FavoriteCount: st.FavoriteCount,
		Liked: st.Liked, Favorited: st.Favorited,
	}, nil
}

func formatUID(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func toProto(v *biz.Video, author, cover, playlist, roomID, danmuWS string, st biz.InteractStats) *v1.VideoInfo {
	return &v1.VideoInfo{
		Id: v.ID, Title: v.Title, Description: v.Description, Category: v.Category,
		UserId: v.UserID, Author: author, Status: v.Status, DurationSec: v.DurationSec,
		PlayCount: v.PlayCount, CoverUrl: cover, PlaylistUrl: playlist,
		RoomId: roomID, DanmuWsHint: danmuWS,
		LikeCount: st.LikeCount, CommentCount: st.CommentCount, FavoriteCount: st.FavoriteCount,
		Liked: st.Liked, Favorited: st.Favorited,
	}
}
