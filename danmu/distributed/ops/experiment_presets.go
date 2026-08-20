package ops

import "fmt"

// Preset 只是参数模板（模板套用到真实 loadtest，绝不写第二套压测引擎）。
// 每个 preset 对应一个有明确系统问题的场景，workload 全部落在
// loadtest 已支持的参数空间内（connections / rooms / message_rate / duration / target）。
type Preset struct {
	Name         string         `json:"name"`
	Label        string         `json:"label"`
	Description  string         `json:"description"`
	Question     string         `json:"question"`     // 这个 preset 想回答的系统问题
	Architecture string         `json:"architecture"` // 默认架构（可在页面改为另一种）
	Workload     WorkloadConfig `json:"workload"`
}

// ExperimentPresets 是有内置问题的预设集。Target 是环境相关的默认值，页面可改。
var ExperimentPresets = []Preset{
	{
		Name:         "low-fanout",
		Label:        "Low Fan-out",
		Architecture: ArchMonolith,
		Description:  "大量房间、每个房间少量连接 —— 验证基础 WebSocket + broadcast 主链在低扇出下是否闭环。",
		Question:     "基础 WS + 广播路径在低扇出下是否闭环、延迟是否可接受？",
		Workload: WorkloadConfig{
			Connections: 2000, Rooms: 1000, MessageRate: 1,
			Duration: "60s", Target: "ws://localhost:8081",
		},
	},
	{
		Name:         "hot-room",
		Label:        "Hot Room",
		Architecture: ArchMonolith,
		Description:  "更少房间、更高扇出 —— 观察延迟、send queue 压力、广播放大（一条消息复制到房间内所有连接）。",
		Question:     "热门房间高扇出下，广播放大是否造成延迟劣化 / 队列压力 / 丢消息？",
		Workload: WorkloadConfig{
			Connections: 1000, Rooms: 10, MessageRate: 2,
			Duration: "60s", Target: "ws://localhost:8081",
		},
	},
	{
		Name:         "custom",
		Label:        "Custom",
		Architecture: ArchMonolith,
		Description:  "用户自定义 workload：connections / rooms / rate / duration / target 全由自己设置。",
		Question:     "用户指定的任意 workload。",
		Workload: WorkloadConfig{
			Connections: 100, Rooms: 10, MessageRate: 1,
			Duration: "30s", Target: "ws://localhost:8081",
		},
	},
}

// PresetByName 按 name 查预设；不存在返回 (nil, error)。
func PresetByName(name string) (*Preset, error) {
	for i := range ExperimentPresets {
		if ExperimentPresets[i].Name == name {
			return &ExperimentPresets[i], nil
		}
	}
	return nil, fmt.Errorf("unknown preset %q (available: low-fanout, hot-room, custom)", name)
}
