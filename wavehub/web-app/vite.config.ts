import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 开发期默认拆到各服务；若已起 gateway(:8088)，可把 target 统一改成 8088。
const gateway = process.env.VITE_PROXY_GATEWAY || ''
const userTarget = gateway || 'http://localhost:8001'
const videoTarget = gateway || 'http://localhost:8003'
const socialTarget = gateway || 'http://localhost:8004'
const searchTarget = gateway || 'http://localhost:8005'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/v1/register': { target: userTarget, changeOrigin: true },
      '/v1/login': { target: userTarget, changeOrigin: true },
      '/v1/videos': { target: videoTarget, changeOrigin: true },
      '/v1/me': { target: videoTarget, changeOrigin: true },
      '/v1/users': { target: socialTarget, changeOrigin: true },
      '/v1/search': { target: searchTarget, changeOrigin: true },
      // 可选：同源 /ws 代理到 comet（或 gateway）
      '/ws': {
        target: gateway || 'http://localhost:8080',
        changeOrigin: true,
        ws: true,
      },
    },
  },
})
