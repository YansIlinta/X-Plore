package main

import (
	"fmt"
	"runtime"

	"github.com/grafana/pyroscope-go"
)

// startPyroscope 接入 Grafana Pyroscope 持续剖析。
// addr 形如 http://localhost:4040；失败返回 error（调用方打 WARN，不阻断启动）。
func startPyroscope(addr, serverID string) error {
	_, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: "danmu-monolith",
		ServerAddress:   addr,
		Tags: map[string]string{
			"server_id": serverID,
			"go_arch":   runtime.GOARCH,
		},
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
		},
	})
	if err != nil {
		return fmt.Errorf("pyroscope.Start: %w", err)
	}
	return nil
}
