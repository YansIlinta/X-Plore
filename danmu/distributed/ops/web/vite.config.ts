import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 开发时 vite dev server 把 /api 代理到本机 ops 后端（:7900）。
// 生产构建产物由 ops 二进制 go:embed 内嵌，同端口服务，无需 CORS。
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:7900",
    },
  },
  build: {
    outDir: "dist",
  },
});
