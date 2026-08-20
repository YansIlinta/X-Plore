#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

command -v protoc >/dev/null 2>&1 || {
  echo "error: protoc is required" >&2
  exit 1
}
command -v protoc-gen-go >/dev/null 2>&1 || {
  echo "error: protoc-gen-go is required" >&2
  exit 1
}
command -v protoc-gen-go-grpc >/dev/null 2>&1 || {
  echo "error: protoc-gen-go-grpc is required" >&2
  exit 1
}

protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  pb/danmu.proto pb/realtime.proto

echo "protobuf Go sources regenerated"
