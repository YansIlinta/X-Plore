// 异步任务契约。投递方与消费方必须共享同一份定义。
package task

const (
	TypeProcessAudio = "media:process_audio"
	TypeProcessVideo = "media:process_video"
)

type ProcessAudioPayload struct {
	TrackID   uint64 `json:"track_id"`
	ObjectKey string `json:"object_key"`
}

// ProcessVideoPayload 点播转码任务：原片 → HLS + 封面。
type ProcessVideoPayload struct {
	VideoID   uint64 `json:"video_id"`
	ObjectKey string `json:"object_key"` // 原片在 MinIO 中的 key
}
