package service

import (
	"context"

	v1 "github.com/YansIlinta/wavehub-micro/api/social/v1"
	"github.com/YansIlinta/wavehub-micro/app/social/internal/biz"
	mw "github.com/YansIlinta/wavehub-micro/pkg/authmw"
)

type SocialService struct {
	v1.UnimplementedSocialServer
	uc *biz.SocialUsecase
}

func NewSocialService(uc *biz.SocialUsecase) *SocialService {
	return &SocialService{uc: uc}
}

func (s *SocialService) ToggleFollow(ctx context.Context, req *v1.ToggleFollowRequest) (*v1.ToggleFollowReply, error) {
	following, count, err := s.uc.ToggleFollow(ctx, mw.UserIDFromContext(ctx), req.Id)
	if err != nil {
		return nil, err
	}
	return &v1.ToggleFollowReply{Following: following, FollowerCount: count}, nil
}

func (s *SocialService) GetProfile(ctx context.Context, req *v1.GetProfileRequest) (*v1.Profile, error) {
	p, err := s.uc.Profile(ctx, req.Id, mw.UserIDFromContext(ctx))
	if err != nil {
		return nil, err
	}
	return &v1.Profile{
		Id: p.ID, Username: p.Username,
		FollowerCount: p.FollowerCount, FollowingCount: p.FollowingCount,
		Following: p.Following,
	}, nil
}

func toFollowList(list []*biz.FollowUser, total int64) *v1.ListFollowReply {
	out := make([]*v1.FollowUser, 0, len(list))
	for _, u := range list {
		out = append(out, &v1.FollowUser{Id: u.ID, Username: u.Username, CreatedAt: u.CreatedAt})
	}
	return &v1.ListFollowReply{List: out, Total: int32(total)}
}

func (s *SocialService) ListFollowings(ctx context.Context, req *v1.ListFollowRequest) (*v1.ListFollowReply, error) {
	list, total, err := s.uc.ListFollowings(ctx, req.Id, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	return toFollowList(list, total), nil
}

func (s *SocialService) ListFollowers(ctx context.Context, req *v1.ListFollowRequest) (*v1.ListFollowReply, error) {
	list, total, err := s.uc.ListFollowers(ctx, req.Id, int(req.Page), int(req.Size))
	if err != nil {
		return nil, err
	}
	return toFollowList(list, total), nil
}
