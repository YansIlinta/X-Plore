// handler 层只做三件事：解析请求 → 调 service → 拼响应。业务规则不要写在这里。
package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/YansIlinta/wavehub/internal/model"
	"github.com/YansIlinta/wavehub/internal/service"
)

type TrackHandler struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewTrackHandler(db *gorm.DB, rdb *redis.Client) *TrackHandler {
	return &TrackHandler{db: db, rdb: rdb}
}

// Create: 登记作品元数据，返回该去哪上传（真实项目返回 MinIO 预签名 URL，见 ARCHITECTURE.md 第 4 节）
func (h *TrackHandler) Create(c *gin.Context) {
	var req struct {
		Title string `json:"title" binding:"required,max=120"` // binding tag 自动校验
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	track := model.Track{
		UserID: c.GetUint64("userID"), // 由 JWT 中间件塞入
		Title:  req.Title,
		Status: "processing",
	}
	if err := h.db.Create(&track).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": track.ID /*, "upload_url": presignedURL */})
}

// CompleteUpload: 客户端传完文件后调用，触发异步转码。
// 学习期先用 go func() 体验流程，之后换成 asynq.Client.Enqueue（见 LEARNING.md 第 5 步）
func (h *TrackHandler) CompleteUpload(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	go service.ProcessAudio(h.db, id) // TODO: 换 asynq，进程重启不丢任务
	c.JSON(http.StatusAccepted, gin.H{"status": "processing"})
}
// List: 分页列表。注意 select 排除了 peaks/project_data 大字段——列表页不需要它们
func (h *TrackHandler) List(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	if size > 100 {
		size = 100 // 永远限制分页上限，防止一次拖库
	}

	var tracks []model.Track
	h.db.Select("id, user_id, title, status, duration_sec, play_count, created_at").
		Where("status = ?", "ready").
		Order("created_at DESC").
		Offset((page - 1) * size).Limit(size).
		Find(&tracks)
	c.JSON(http.StatusOK, gin.H{"list": tracks})
}

// Detail: 详情页返回 peaks，前端拿去画波形；播放量在 Redis 累加，不直接写库
func (h *TrackHandler) Detail(c *gin.Context) {
	var track model.Track
	if err := h.db.First(&track, "id = ?", c.Param("id")).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "作品不存在"})
		return
	}
	h.rdb.Incr(c.Request.Context(), "play:"+c.Param("id")) // 定时任务批量刷回 PostgreSQL
	c.JSON(http.StatusOK, track)
}
