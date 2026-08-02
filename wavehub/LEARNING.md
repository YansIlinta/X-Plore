# 学习路线：Git / Linux / Gin / Go

> 原则：不要先啃完再动手。每项学 20% 的核心，剩下的在做 WaveHub 的过程中按需查。

---

## 1. Git（1~2 天入门，长期精进）

**必读文档：**
- Pro Git 中文版（官方书，免费）：https://git-scm.com/book/zh/v2 —— 只读第 1~3 章 + 第 6 章 GitHub 部分
- 交互式练习分支：https://learngitbranching.js.org/?locale=zh_CN （强烈推荐，游戏化练 rebase/merge）

**日常够用的核心命令（占日常使用 95%）：**

```bash
git status / git diff          # 永远先看状态再操作
git add -p                     # 按块暂存，比 git add . 好的习惯
git commit -m "feat: xxx"      # 用 Conventional Commits 格式(feat/fix/docs/refactor)
git log --oneline --graph      # 看历史
git switch -c feature/upload   # 开分支做功能，做完合回 main
git rebase main                # 保持分支基于最新 main（历史更干净，学习期就养成）
git push / git pull --rebase
git stash / git stash pop      # 临时切走时保存现场
```

**练习方式**：WaveHub 每个功能开一个分支，自己给自己提 PR（GitHub 上练 code review 流程）。

---

## 2. Linux（1 周入门，建议方式：WSL2）

你在 Windows 上，**装 WSL2 + Ubuntu 是最优解**：`wsl --install` 一条命令，既是真实 Linux 环境，又不用双系统/虚拟机。Go 服务上线跑的就是 Linux，开发环境和生产一致。

**必学清单（按顺序）：**

1. 文件与目录：`cd ls cp mv rm mkdir`、绝对/相对路径、`~` 与 `/`
2. 查看文件：`cat less tail -f`（`tail -f` 看日志是服务端日常）
3. 权限：`chmod chown`、为什么脚本要 `chmod +x`、`sudo` 是什么
4. 进程：`ps aux | grep`、`kill`、`top/htop`、前后台 `&` 与 `nohup`
5. 网络排查四件套：`curl`（手动测自己的 API）、`ss -tlnp`（谁占用了端口）、`ping`、`scp`
6. 管道与文本三剑客初步：`|`、`grep`、`awk '{print $1}'`、`wc -l` —— 分析访问日志用
7. systemd：写一个 `.service` 文件把你的 Go 二进制变成开机自启服务（部署 WaveHub 时练）

**文档：**
- 《The Linux Command Line》中文版：http://billie66.github.io/TLCL/
- 快速手册型：https://wangchujiang.com/linux-command/

**练习方式**：WaveHub 全程在 WSL2 里开发（Go、Docker、FFmpeg 都装在 WSL 里），逼自己用命令行。

---

## 3. Go 语言本身（贯穿始终）

**文档（按学习顺序）：**
1. Go 官方 Tour（有中文）：https://tour.go-zh.org/ —— 2 天过完语法
2. Go by Example（中文）：https://gobyexample-cn.github.io/ —— 当字典查
3. 《Effective Go》：https://go.dev/doc/effective_go —— 写出地道 Go 的必读
4. 进阶看《Go 语言设计与实现》：https://draveness.me/golang/ —— 面试深度（GMP 调度、GC、channel 底层）

**这个项目里你会实际用到的 Go 核心概念**：goroutine + channel（转码 worker）、`context.Context`（超时与取消，Gin 每个请求都带）、`defer`（关文件/解锁）、error 处理惯例（`if err != nil` 与 `errors.Is/As`）、interface（service 层解耦）、`os/exec`（调 FFmpeg）。

---

## 4. Gin（2~3 天上手）

**文档：**
- 官方文档（中文）：https://gin-gonic.com/zh-cn/docs/
- 官方 examples 仓库：https://github.com/gin-gonic/examples

**必须掌握的 6 个点（本项目骨架里全部有示例）：**

1. 路由与分组 `r.Group("/api/v1")`
2. 参数绑定三兄弟：`ShouldBindJSON`（body）/ `ShouldBindQuery`（?page=1）/ `Param`（/:id）+ binding tag 校验
3. 中间件：`c.Next()` / `c.Abort()` 的洋葱模型，自己写一个 JWT 中间件（见 `internal/middleware/auth.go`）
4. `c.Set / c.Get`：中间件向 handler 传当前用户 ID
5. 统一响应结构与统一错误处理
6. 文件上传 `c.FormFile` 与大文件的流式处理

**配套库文档：**
- GORM 中文：https://gorm.io/zh_CN/docs/
- go-redis：https://redis.uptrace.dev/zh/
- asynq（任务队列）：https://github.com/hibiken/asynq

## 4.5 微服务 / Kratos（进阶，配合 ../wavehub-micro）

- Kratos 官方文档（中文）：https://go-kratos.dev/docs/
- Protocol Buffers 官方指南：https://protobuf.dev/programming-guides/proto3/
- gRPC 官方 Go 教程：https://grpc.io/docs/languages/go/basics/
- 学习顺序：先把本单体版跑通，再读 `../wavehub-micro/MICROSERVICES.md` 对比两种写法——
  proto 契约 → service/biz/data 分层 → 服务间 gRPC → 异步队列 → 服务发现

---

## 5. 建议的做事顺序（每步都有产出）

1. **第 1~2 周**：Go Tour + 在 WSL2 里把 `docker-compose up` 环境跑起来，写通「用户注册/登录（JWT）」
2. **第 3~4 周**：作品 CRUD + 分页列表 + PostgreSQL 索引初体验（EXPLAIN 看查询计划）
3. **第 5~6 周**：文件上传到 MinIO + asynq 队列 + FFmpeg 转码与波形峰值提取（本项目的灵魂）
4. **第 7~8 周**：前端用 wavesurfer.js 把峰值画出来；Redis 做播放计数和排行榜
5. **之后**：评论区、关注流、然后再考虑音乐编辑器（Tone.js，工程量最大，放最后）

每完成一步：开 Git 分支 → 提交 → 合并，把 Git 练习揉进日常。
