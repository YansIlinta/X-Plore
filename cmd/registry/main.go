// registry 是 goim 式架构的服务发现：复用 minirpc/registry（HTTP + 内存 + TTL 租约）。
// comet/logic 启动时 POST /register 续租，job/comet 通过 GET /services?service=X 发现对端。
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"minirpc/registry"
)

func main() {
	addr := flag.String("addr", ":7350", "registry HTTP listen address")
	ttl := flag.Duration("ttl", 10*time.Second, "服务租约有效期")
	flag.Parse()

	reg := registry.New(*ttl)
	log.Printf("[registry] listening on %s (ttl=%s)", *addr, *ttl)
	if err := http.ListenAndServe(*addr, reg); err != nil {
		log.Fatalf("[registry] %v", err)
	}
}
