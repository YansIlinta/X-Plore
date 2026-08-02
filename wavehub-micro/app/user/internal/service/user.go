// service 层 = 协议适配（对应单体里的 handler）：把 proto 请求翻译给 biz，再把结果翻译回去。
// 这一层必须"薄"——不写任何业务规则，同一份实现同时服务 gRPC 和 HTTP。
package service

import (
	"context"

	v1 "github.com/YansIlinta/wavehub-micro/api/user/v1"
	"github.com/YansIlinta/wavehub-micro/app/user/internal/biz"
)

type UserService struct {
	v1.UnimplementedUserServer // 前向兼容：proto 加新方法时旧代码不编译失败
	uc *biz.UserUsecase
}

func NewUserService(uc *biz.UserUsecase) *UserService { return &UserService{uc: uc} }

func (s *UserService) Register(ctx context.Context, req *v1.RegisterRequest) (*v1.AuthReply, error) {
	token, id, err := s.uc.Register(ctx, req.Username, req.Password)
	if err != nil {
		return nil, err
	}
	return &v1.AuthReply{Token: token, UserId: id}, nil
}

func (s *UserService) Login(ctx context.Context, req *v1.LoginRequest) (*v1.AuthReply, error) {
	token, id, err := s.uc.Login(ctx, req.Username, req.Password)
	if err != nil {
		return nil, err
	}
	return &v1.AuthReply{Token: token, UserId: id}, nil
}

func (s *UserService) GetUser(ctx context.Context, req *v1.GetUserRequest) (*v1.UserInfo, error) {
	u, err := s.uc.Get(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return &v1.UserInfo{Id: u.ID, Username: u.Username}, nil
}
