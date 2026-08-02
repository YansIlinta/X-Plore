// biz 层 = 业务逻辑（对应单体里的 service 层）。
// Kratos 分层的关键规矩：biz 不知道数据库是什么，只依赖自己定义的 UserRepo 接口，
// 由 data 层来实现它 —— 这叫依赖倒置，换数据库/写单元测试(mock repo)都不用动业务代码。
package biz

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// User 是业务实体，不带任何 gorm tag —— 表结构是 data 层的私事
type User struct {
	ID           uint64
	Username     string
	PasswordHash string
}

type UserRepo interface {
	Create(ctx context.Context, u *User) error
	GetByUsername(ctx context.Context, username string) (*User, error)
	GetByID(ctx context.Context, id uint64) (*User, error)
}

type UserUsecase struct {
	repo      UserRepo
	jwtSecret []byte
}

func NewUserUsecase(repo UserRepo, jwtSecret string) *UserUsecase {
	return &UserUsecase{repo: repo, jwtSecret: []byte(jwtSecret)}
}

func (uc *UserUsecase) Register(ctx context.Context, username, password string) (token string, id uint64, err error) {
	if len(username) < 3 || len(password) < 6 {
		return "", 0, errors.BadRequest("INVALID_PARAM", "用户名至少3位，密码至少6位")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost) // 绝不存明文
	if err != nil {
		return "", 0, err
	}
	u := &User{Username: username, PasswordHash: string(hash)}
	if err := uc.repo.Create(ctx, u); err != nil {
		// 用 kratos errors 包返回业务错误：HTTP 侧自动变成对应状态码，gRPC 侧变成对应 code
		return "", 0, errors.Conflict("USER_EXISTS", "用户名已被占用")
	}
	token, err = uc.sign(u.ID)
	return token, u.ID, err
}

func (uc *UserUsecase) Login(ctx context.Context, username, password string) (token string, id uint64, err error) {
	u, err := uc.repo.GetByUsername(ctx, username)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		// 故意不区分"用户不存在"和"密码错误"，防止撞库探测
		return "", 0, errors.Unauthorized("AUTH_FAILED", "用户名或密码错误")
	}
	token, err = uc.sign(u.ID)
	return token, u.ID, err
}

func (uc *UserUsecase) Get(ctx context.Context, id uint64) (*User, error) {
	u, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, errors.NotFound("USER_NOT_FOUND", "用户不存在")
	}
	return u, nil
}

func (uc *UserUsecase) sign(uid uint64) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid": uid,
		"exp": time.Now().Add(72 * time.Hour).Unix(),
	})
	return t.SignedString(uc.jwtSecret)
}
