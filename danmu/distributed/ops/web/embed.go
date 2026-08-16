// Package web 内嵌 Ops Console 前端构建产物。
// 构建前需先在本目录执行 npm install && npm run build（vite 输出到 dist/）。
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed all:dist
var dist embed.FS

// Assets 返回前端静态资源的 FileSystem。
func Assets() http.FileSystem {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return http.FS(sub)
}
