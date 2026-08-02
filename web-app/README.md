# X-Plore Web（React + TypeScript + Vite）

产品前端：发现 / 登录 / 投稿 / 播放（HLS + 飘字弹幕）/ 我的。

## 开发

```powershell
# 依赖（网络不稳用 npmmirror）
npm install --registry=https://registry.npmmirror.com
npm run dev
# http://localhost:5173
```

仓库根目录也可：

```powershell
.\scripts\dev-platform.ps1 frontend
# 走 gateway：
$env:USE_GATEWAY='1'; .\scripts\dev-platform.ps1 frontend
```

## 环境变量

| 变量 | 说明 |
|------|------|
| `VITE_API_BASE` | 如 `http://localhost:8088`，空则走 Vite 代理 |
| `VITE_DANMU_WS` | 如 `ws://localhost:8088/ws` |
| `USE_GATEWAY=1` | 由 `dev-platform.ps1` 设置上述变量 |

默认代理：`/v1/login|register` → `:8001`，`/v1/videos|/v1/me` → `:8003`。

## 构建

```powershell
npm run build
```

完整演示步骤见仓库根目录 [DEMO.md](../DEMO.md)。
