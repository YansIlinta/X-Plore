# SoundCanvas — 本地音乐可视化应用设计文档

> 状态：设计阶段，未开工。
> 关系：独立于 WaveHub（平台）的本地应用；后端复用 WaveHub 学到的 Gin 技能，
> 未来微服务化时直接套用 X-Plore danmu/distributed 的既有模式。

---

## 1. 产品定义与边界

**一句话**：把本地音乐文件变成一块可定制的视觉画布——频谱/波形驱动的动态背景 + 同步歌词。

**先划清一条法律边界**（决定"在线搜索"能做什么）：

- ✅ 播放**用户自己的本地文件**——无版权问题
- ✅ 在线搜索**元数据**（歌名/歌手/封面）和**歌词**——有免费合法 API
- ❌ 在线搜索并播放/下载歌曲本体——盗版，不做，微服务版也不做

所以"在线搜索"的真实定义：**为本地文件匹配元数据和滚动歌词**。用户导入 `周杰倫-晴天.mp3`，
应用自动搜出封面、正确标题和逐行时间轴歌词（LRC）。这个定义合法、有用、可实现。

## 2. 功能清单（按里程碑排序）

| 里程碑 | 功能 | 对应你的需求 |
|---|---|---|
| **M1 最小可用** | 本地文件导入(拖拽+选择器)、播放控制、频谱/波形两种可视化、**黑白两套背景** | 需求 1、5 |
| **M2 歌词** | LRC 文件导入与解析、逐行滚动高亮、无 LRC 时的纯视觉模式 | 需求 3 |
| **M3 在线搜索** | 按文件名/手动关键词搜元数据+歌词（走自建 Gin 代理） | 需求 2 |
| **M4 背景系统** | 多背景预设库、用户导入图片/视频做背景、背景与可视化叠加规则 | 需求 3、4 |
| **M5 后端/微服务** | 账号、云端收藏歌词库、设置同步（复用 danmu/distributed 模式） | 你说的"可能" |

**M1 刻意最小**：能拖一个 mp3 进来、看到黑底白线的频谱在动，就是第一个里程碑完成。

## 3. 技术选型（每项说清为什么）

### 3.1 形态：浏览器应用（PWA），不是桌面客户端

- 候选：网页 / Electron / Tauri
- **选网页**：Web Audio API 就是为这个场景生的，浏览器里全套能力齐备（解码、频谱分析、Canvas 渲染）；
  Electron 打包 100MB+ 且要学一套新东西，两天冲刺后你的技能栈里没有它的位置。
- 本地文件不需要后端：`<input type="file">` / 拖拽拿到 File 对象直接喂给 AudioContext，**文件不离开电脑**。
- 想要"双击图标打开"的桌面感 → 做成 PWA（一个 manifest.json 的事），以后不满意再考虑 Tauri。

### 3.2 前端框架：Vue 3 + Vite

- 候选：Vue 3 / React / 原生 JS
- **选 Vue 3**：中文文档官方且一流（cn.vuejs.org），响应式模型直觉（播放状态→UI 自动更新），
  国内生态和岗位多——和你选 Gin 的理由同构。
- 不选 React：JSX + hooks 心智负担对新手更重，中文资料质量参差。
- 不选原生 JS：播放器状态（当前曲目/进度/歌词行/背景选择）联动多，手动管理 DOM 会写成泥潭。
- **但可视化渲染层不走 Vue**：Canvas 每帧 60 次重绘，走响应式系统是性能自杀。
  规则：**Vue 管 UI 状态，Canvas 用原生 requestAnimationFrame 自己跑**，两个世界只通过少量事件通信。

### 3.3 音频与可视化：Web Audio API + Canvas 2D（不引库）

- 核心链路只有四步，全是平台原生能力，**不需要任何 npm 音频库**：
  ```
  File → AudioContext.decodeAudioData → AnalyserNode → 每帧 getByteFrequencyData()
       → Canvas 画出来
  ```
- AnalyserNode 免费送你两样东西：**频域**（频谱柱/径向光环用）和**时域**（波形线用）。
- 不选 WebGL/Three.js 起步：Canvas 2D 画几百个矩形/线条 60fps 毫无压力，先把美术做对；
  M4 之后想上粒子/着色器效果再迁 WebGL，渲染器接口先留好（见 5.2）。
- 不选 wavesurfer.js：它是"波形播放器控件"，我们要的是自由画布，引它反而束缚。

### 3.4 歌词：LRC 格式

- LRC 是事实标准：`[01:23.45]歌词一行`，一个 50 行的解析函数就能搞定，不引库。
- 数据结构：`{ time: 秒, text: string }[]`，播放时二分查找当前行。
- 在线歌词源：**LRCLIB**（lrclib.net，免费开放 API，有逐行时间轴，无需鉴权）为主，
  元数据/封面用 **iTunes Search API**（免费无鉴权）。都从 Gin 代理走（见 3.5）。

### 3.5 后端：一个极小的 Gin 代理（M3 才需要）

浏览器直连第三方 API 会撞 CORS，且将来加缓存/换源都要有个中间层。这正好是你的主场：

```
浏览器 → GET /api/search?q=晴天 → Gin：查 Redis 缓存 → 未命中则请求 LRCLIB/iTunes
       → 缓存 24h → 返回统一格式 JSON
```

- 一个 main.go + 一个 handler，比 X-Plore 现有服务还小，半天工作量；
- **微服务版（M5）**：这个代理天然就是第一个服务（search 服务）；账号/收藏起来后按
  danmu/distributed 的模式拆 user / library 服务，proto 契约、JWT、数据归属那套纪律原样平移。

### 3.6 本地数据：IndexedDB（通过 localForage）

- 设置、背景选择、歌词缓存、最近播放 → IndexedDB（localStorage 只有 5MB 且同步阻塞，存不了背景图）。
- localForage 是唯一建议引的工具库：把 IndexedDB 包成 `getItem/setItem` 的简单接口。

## 4. 设计规范（硬约束，评审时逐条对照）

### 4.1 字体

- **禁**：Inter、Roboto、Arial、Helvetica、system-ui 兜底里的默认无衬线全家桶
- **用**：
  - 界面/数字：**Geist**（Vercel 出品，OFL 开源，可自托管 woff2）
  - 展示型大字（歌名、时间大数字）：**Clash Display**（Fontshare，免费可商用）
  - 编辑感衬线（歌词的可选风格）：PP Editorial New（注意：**个人免费、商用需授权**，
    商用替代：Fontshare 的 **Sentient** 或 **Gambetta**）
- **中文歌词必须配中文字体**（上述三个都不含 CJK，这是你需求里没提但躲不开的）：
  - 歌词正文：**霞鹜文楷 / LXGW WenKai**（开源，文艺气质与本产品匹配）
  - 界面中文：**思源黑体 / Noto Sans SC**（可变字重，细字重符合整体气质）
  - font-family 顺序：`Geist, 'Noto Sans SC', sans-serif`（英文命中 Geist，中文落到思源）
- 全部**自托管 woff2**，禁用 Google Fonts CDN（国内加载失败=字体全崩回宋体）

### 4.2 图标

- **禁**：Lucide、FontAwesome、Material Icons 等 1.5px+ 描边的通用图标集
- **用**：**Phosphor Thin/Light**（1px 描边档位）；不够用时自绘 SVG，统一 `stroke-width: 1`
- 图标永远单色、继承文字颜色，禁双色/填充式图标

### 4.3 边框、阴影、分隔

- **禁**：1px 实线灰边框（`border: 1px solid #e5e5e5` 这类）、生硬黑投影（Tailwind shadow-md 式的
  `0 4px 6px rgba(0,0,0,.1)`）、以及**任何区块之间的分隔线**（你的需求 6）
- 区块靠什么区分？三种手段按优先级：
  1. **留白**（间距差就是分组：组内 12px，组间 48px+）
  2. **明度差**（背景 #0A0A0A 上放 #111 的面板，无边框）
  3. **模糊层**（`backdrop-filter: blur` 的悬浮层，用于播放控制条）
- 阴影只允许两种：大半径低透明度的环境影（`0 24px 80px rgba(0,0,0,.35)`，营造悬浮）、
  同色系辉光（黑白主题下即白色低透明辉光，用于当前歌词行/激活态）

### 4.4 布局

- **禁**：贴顶 sticky 导航栏、Bootstrap 式三列对称栅格
- 本应用天然不需要导航栏：**可视化画布全屏铺满，一切 UI 悬浮其上**——
  播放控制是底部悬浮胶囊（不贴边，距底 24px+），曲库/设置是侧滑面板或整页覆盖层
- 排版基调：不对称、大留白、歌名等展示字可以出血到边缘；网格用 CSS Grid 自由划分，
  比例避免均分（如 2fr/1fr、黄金比），不用等宽三列

### 4.5 动效

- **禁**：`linear`、`ease-in-out`、以及任何无过渡的瞬时状态切换（display:none 直切）
- 缓动 token（全站只用这三个）：
  ```css
  --ease-out-expo: cubic-bezier(0.16, 1, 0.3, 1);      /* 入场、展开——快起缓收 */
  --ease-in-out-quart: cubic-bezier(0.76, 0, 0.24, 1); /* 位移、换页 */
  --ease-spring: cubic-bezier(0.34, 1.56, 0.64, 1);    /* 小元素弹性(播放按钮) */
  ```
- 时长 token：微交互 150–250ms，面板/覆盖层 400–600ms，背景切换 800–1200ms 交叉淡入
- 歌词行切换：上一行淡出+轻微上移，当前行放大+提亮，禁跳变
- 尊重 `prefers-reduced-motion`：动效降级为纯淡入淡出

### 4.6 黑白主题（M1 先做这两套，即需求 5）

| token | 黑背景「Void」 | 白背景「Paper」 |
|---|---|---|
| 画布底 | #050505 | #FAFAF8（暖白，不用纯白） |
| 面板 | #111111 | #F1F1EE |
| 主文字 | #F5F5F5 | #141414 |
| 次文字 | rgba(245,245,245,.45) | rgba(20,20,20,.45) |
| 可视化主线 | #FFFFFF | #000000 |
| 可视化余晖 | rgba(255,255,255,.18) | rgba(0,0,0,.14) |

- 黑白主题下可视化**不允许出现彩色**——克制本身就是风格；彩色留给 M4 的预设背景
- 用户导入背景（M4）时：图片上自动压一层可调透明度的黑/白纱罩，保证歌词对比度 ≥ 4.5:1

## 5. 架构与数据

### 5.1 M1–M4 结构（纯前端 + 可选代理）

```
浏览器
├── UI 层 (Vue3)：曲库列表 / 播放控制 / 设置 / 背景选择
├── 播放核心 (原生 TS)：AudioContext + AnalyserNode，单例
├── 渲染层 (Canvas)：requestAnimationFrame 循环
│     └── Visualizer 接口（见 5.2），背景层 + 可视化层 + 歌词层三层合成
└── 存储 (IndexedDB)：设置 / 歌词缓存 / 自定义背景
          │ (仅 M3 的搜索请求)
          ▼
     Gin 代理 (:8080) ── LRCLIB / iTunes Search ── Redis 缓存
```

### 5.2 两个关键接口（现在定好，M4/WebGL 迁移才不用重写）

```ts
interface Visualizer {                    // 每种可视化实现它
  init(canvas: HTMLCanvasElement): void
  render(frame: AudioFrame, t: number): void   // 每帧调用
  destroy(): void
}
interface AudioFrame {
  freq: Uint8Array   // 频域 (AnalyserNode.getByteFrequencyData)
  wave: Uint8Array   // 时域 (getByteTimeDomainData)
  rms: number        // 响度，驱动"随节奏呼吸"的全局效果
}
```

M1 实现两个：`BarsVisualizer`（频谱柱，含镜像模式）、`WaveVisualizer`（时域连线）。
背景（纯色/图片/视频）同样走 `Background` 接口，与 Visualizer 层叠加。

### 5.3 数据模型

```ts
Track      { id, fileName, title?, artist?, album?, coverURL?, duration, addedAt }
LyricLine  { time: number, text: string }          // LRC 解析结果，随 track 缓存
Settings   { theme: 'void'|'paper'|presetId, visualizer: string, lyricFont, overlayOpacity }
BackgroundPreset { id, kind: 'solid'|'gradient'|'image'|'video', src?, monochrome: boolean }
```

## 6. 风险与注意

1. **浏览器自动播放策略**：AudioContext 必须由用户手势启动——首屏设计一个"点击开始"仪式感入口，把限制变成设计。
2. **中文字体体积**：思源黑体全量 8MB+，必须子集化（fonttools/子集化服务），界面字先只打包常用 3500 字，歌词字体懒加载。
3. **视频背景性能**：`<video>` + Canvas 同帧合成对低端机吃力，M4 提供"静帧降级"开关。
4. **文件名匹配搜索**：`晴天 (Live) [320k].mp3` 这类脏文件名要先清洗（去括号/码率/序号）再搜，命中率决定 M3 体验。
5. **PP Editorial 授权**：上线/商用前换 Sentient 或购买授权，别带着侥幸发布。

## 7. 开工顺序（M1 拆解，预估 3–4 个工作日）

1. Vite + Vue3 脚手架，设计 token（4.6 表格）落成 CSS variables
2. 文件导入 → AudioContext 播放通（先不管 UI 好不好看）
3. AnalyserNode → BarsVisualizer 黑底白线跑起来
4. 播放控制悬浮胶囊（进度/播放暂停/音量），按 4.3–4.5 规范做
5. WaveVisualizer + 黑白主题切换 + 设置持久化
