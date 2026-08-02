# 在 Linux/WSL2 下使用。Windows 下等价命令见 README 或直接执行 make 里的 protoc 命令。
API_PROTOS := $(shell find api -name "*.proto")

.PHONY: api
api: # 由 proto 生成 pb / grpc / kratos-http 代码
	protoc --proto_path=. \
	       --proto_path=./third_party \
	       --go_out=paths=source_relative:. \
	       --go-grpc_out=paths=source_relative:. \
	       --go-http_out=paths=source_relative:. \
	       $(API_PROTOS)

.PHONY: build
build:
	go build ./...

.PHONY: run-user run-track run-video run-media run-social run-search run-gateway
run-user:
	go run ./app/user/cmd
run-track:
	go run ./app/track/cmd
run-video:
	go run ./app/video/cmd
run-media:
	go run ./app/media/cmd
run-social:
	go run ./app/social/cmd
run-search:
	go run ./app/search/cmd
run-gateway:
	go run ./app/gateway/cmd
