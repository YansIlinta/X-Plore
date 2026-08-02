package service

import (
	"context"

	v1 "github.com/YansIlinta/wavehub-micro/api/track/v1"
	"github.com/YansIlinta/wavehub-micro/app/track/internal/biz"
	mw "github.com/YansIlinta/wavehub-micro/app/track/internal/middleware"
)

type TrackService struct {
	v1.UnimplementedTrackServer
	uc *biz.TrackUsecase
}

func NewTrackService(uc *biz.TrackUsecase) *TrackService { return &TrackService{uc: uc} }

func (s *TrackService) CreateTrack(ctx context.Context, req *v1.CreateTrackRequest) (*v1.CreateTrackReply, error) {
	id, uploadURL, err := s.uc.Create(ctx, mw.UserIDFromContext(ctx), req.Title)
	if err != nil {
		return nil, err
	}
	return &v1.CreateTrackReply{Id: id, UploadUrl: uploadURL}, nil
}

func (s *TrackService) CompleteUpload(ctx context.Context, req *v1.CompleteUploadRequest) (*v1.CompleteUploadReply, error) {
	if err := s.uc.CompleteUpload(ctx, mw.UserIDFromContext(ctx), req.Id); err != nil {
		return nil, err
	}
	return &v1.CompleteUploadReply{Status: "processing"}, nil
}

func (s *TrackService) ListTracks(ctx context.Context, req *v1.ListTracksRequest) (*v1.ListTracksReply, error) {
	tracks, err := s.uc.List(ctx, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	list := make([]*v1.TrackInfo, 0, len(tracks))
	for _, t := range tracks {
		list = append(list, toProto(t, "")) // 列表页不逐条查作者(避免 N+1 次 RPC)，批量方案见文档
	}
	return &v1.ListTracksReply{List: list}, nil
}

func (s *TrackService) GetTrack(ctx context.Context, req *v1.GetTrackRequest) (*v1.TrackInfo, error) {
	t, author, streamURL, err := s.uc.Get(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	info := toProto(t, author)
	info.Peaks = t.Peaks         // 只有详情页返回波形
	info.StreamUrl = streamURL   // 前端 <audio src=streamURL> 直连 MinIO 播放
	return info, nil
}

func (s *TrackService) ReportProcessed(ctx context.Context, req *v1.ReportProcessedRequest) (*v1.ReportProcessedReply, error) {
	if err := s.uc.ReportProcessed(ctx, req.Id, req.Success, req.DurationSec, req.Peaks); err != nil {
		return nil, err
	}
	return &v1.ReportProcessedReply{}, nil
}

func toProto(t *biz.Track, author string) *v1.TrackInfo {
	return &v1.TrackInfo{
		Id: t.ID, Title: t.Title, UserId: t.UserID, Author: author,
		Status: t.Status, DurationSec: t.DurationSec, PlayCount: t.PlayCount,
	}
}
