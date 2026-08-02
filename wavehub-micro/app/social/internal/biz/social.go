package biz

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/v2/errors"

	userv1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
)

// FollowEdge 一条关注关系边(只存 id,用户名按需查 user 服务)
type FollowEdge struct {
	UserID    uint64
	CreatedAt int64 // unix ms
}

type FollowRepo interface {
	Toggle(ctx context.Context, followerID, followeeID uint64) (following bool, err error)
	IsFollowing(ctx context.Context, followerID, followeeID uint64) (bool, error)
	CountFollowers(ctx context.Context, userID uint64) (int64, error)
	CountFollowings(ctx context.Context, userID uint64) (int64, error)
	ListFollowings(ctx context.Context, userID uint64, offset, limit int) ([]*FollowEdge, int64, error)
	ListFollowers(ctx context.Context, userID uint64, offset, limit int) ([]*FollowEdge, int64, error)
}

type Profile struct {
	ID             uint64
	Username       string
	FollowerCount  int64
	FollowingCount int64
	Following      bool
}

type SocialUsecase struct {
	repo    FollowRepo
	userCli userv1.UserClient
}

func NewSocialUsecase(repo FollowRepo, userCli userv1.UserClient) *SocialUsecase {
	return &SocialUsecase{repo: repo, userCli: userCli}
}

func (uc *SocialUsecase) username(ctx context.Context, id uint64) string {
	if u, err := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: id}); err == nil {
		return u.Username
	}
	return strconv.FormatUint(id, 10)
}

func (uc *SocialUsecase) ToggleFollow(ctx context.Context, followerID, followeeID uint64) (following bool, followerCount int64, err error) {
	if followerID == 0 {
		return false, 0, errors.Unauthorized("NO_USER", "未登录")
	}
	if followerID == followeeID {
		return false, 0, errors.BadRequest("SELF_FOLLOW", "不能关注自己")
	}
	if _, err := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: followeeID}); err != nil {
		return false, 0, errors.NotFound("USER_NOT_FOUND", "用户不存在")
	}
	following, err = uc.repo.Toggle(ctx, followerID, followeeID)
	if err != nil {
		return false, 0, err
	}
	followerCount, err = uc.repo.CountFollowers(ctx, followeeID)
	return following, followerCount, err
}

// Profile UP 主公开信息;viewerID!=0 时返回其是否已关注。
func (uc *SocialUsecase) Profile(ctx context.Context, id, viewerID uint64) (*Profile, error) {
	u, err := uc.userCli.GetUser(ctx, &userv1.GetUserRequest{Id: id})
	if err != nil {
		return nil, errors.NotFound("USER_NOT_FOUND", "用户不存在")
	}
	p := &Profile{ID: id, Username: u.Username}
	p.FollowerCount, _ = uc.repo.CountFollowers(ctx, id)
	p.FollowingCount, _ = uc.repo.CountFollowings(ctx, id)
	if viewerID != 0 && viewerID != id {
		p.Following, _ = uc.repo.IsFollowing(ctx, viewerID, id)
	}
	return p, nil
}

type FollowUser struct {
	ID        uint64
	Username  string
	CreatedAt int64
}

func (uc *SocialUsecase) list(ctx context.Context, edges []*FollowEdge) []*FollowUser {
	out := make([]*FollowUser, 0, len(edges))
	for _, e := range edges {
		out = append(out, &FollowUser{ID: e.UserID, Username: uc.username(ctx, e.UserID), CreatedAt: e.CreatedAt})
	}
	return out
}

func (uc *SocialUsecase) ListFollowings(ctx context.Context, id uint64, page, size int) ([]*FollowUser, int64, error) {
	page, size = normalizePage(page, size)
	edges, total, err := uc.repo.ListFollowings(ctx, id, (page-1)*size, size)
	if err != nil {
		return nil, 0, err
	}
	return uc.list(ctx, edges), total, nil
}

func (uc *SocialUsecase) ListFollowers(ctx context.Context, id uint64, page, size int) ([]*FollowUser, int64, error) {
	page, size = normalizePage(page, size)
	edges, total, err := uc.repo.ListFollowers(ctx, id, (page-1)*size, size)
	if err != nil {
		return nil, 0, err
	}
	return uc.list(ctx, edges), total, nil
}

func normalizePage(page, size int) (int, int) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return page, size
}
