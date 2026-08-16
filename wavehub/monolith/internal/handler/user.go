package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub/internal/model"
)

type UserHandler struct {
	db        *gorm.DB
	jwtSecret string // 签发 token 要用的"印章"，和中间件验章用的是同一个
}

func NewUserHandler(db *gorm.DB, jwtSecret string) *UserHandler {
	return &UserHandler{db: db, jwtSecret: jwtSecret}
}

func (h *UserHandler) Register(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required,min=3,max=32"`
		Password string `json:"password" binding:"required,min=6"`
	}
	if err := c.ShouldBindJSON(&req); err != nil { // 数据来自 body 的 JSON
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "加密失败"})
		return
	}

	user := model.User{Username: req.Username, PasswordHash: string(hash)}
	if err := h.db.Create(&user).Error; err != nil { // 真正写库，ID 在这之后自动有了
		c.JSON(http.StatusConflict, gin.H{"error": "用户名已被占用"})
		return
	}

	token, err := h.sign(user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "签发 token 失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": user.ID, "token": token})
}

// Login 是你的作业。四步，每步用到的工具你都见过：
//  1. 和 Register 开头一样，用 ShouldBindJSON 拿到 username/password（校验规则可以简单些）
//  2. 按用户名查库：var user model.User + h.db.First(&user, "username = ?", req.Username)
//     —— 写法参考 track.go 的 Detail
//  3. 核对密码：bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password))
//     返回 nil 说明密码正确。注意：查不到用户、密码不对，都返回同一句
//     "用户名或密码错误"（想想为什么不能提示得更具体 —— 撞库探测）
//  4. h.sign(user.ID) 签发 token，c.JSON 返回，格式和 Register 一样
func (h *UserHandler) Login(c *gin.Context) {
	// ① 准备容器，一次性接住整个 JSON body
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// ② 按用户名查库。注意"查不到"和"密码错"必须返回同一句话，不给撞库者线索
	var user model.User
	if err := h.db.First(&user, "username = ?", req.Username).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "用户名或密码错误"})
		return
	}

	// ③ 核对密码：(库里的哈希, 用户这次输入的明文)，返回 nil 才是匹配
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "用户名或密码错误"})
		return
	}

	// ④ 盖章放行
	token, err := h.sign(user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "签发 token 失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": user.ID, "token": token})
}

// sign 生成 JWT —— 就是"盖章"：uid 和过期时间写进 payload，用 secret 算出签名
func (h *UserHandler) sign(uid uint64) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid": uid,
		"exp": time.Now().Add(72 * time.Hour).Unix(), // 72小时后过期
	})
	return t.SignedString([]byte(h.jwtSecret))
}
