# X-Plore VOD platform helper (Windows PowerShell)
# Usage: .\scripts\dev-platform.ps1 deps|frontend|gateway|build|help

param(
  [Parameter(Position = 0)]
  [ValidateSet("deps", "frontend", "gateway", "build", "help")]
  [string]$Cmd = "help"
)

# $Root 指向 wavehub/（本脚本在 wavehub/scripts/ 下）；$RepoRoot 指向仓库根 X-Plore/
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
if (-not (Test-Path (Join-Path $Root "EVOLUTION.md"))) {
  $Root = "D:\X-Plore\wavehub"
}
$RepoRoot = Split-Path -Parent $Root

function Test-Cmd([string]$Name) {
  return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Show-Deps {
  Write-Host "=== tools ===" -ForegroundColor Cyan
  foreach ($c in @("go", "node", "npm", "docker", "ffmpeg", "ffprobe")) {
    if (Test-Cmd $c) {
      Write-Host "  OK  $c" -ForegroundColor Green
    } else {
      Write-Host "  --  $c (not in PATH)" -ForegroundColor Yellow
    }
  }

  Write-Host "=== ports ===" -ForegroundColor Cyan
  foreach ($p in @(5432, 6379, 9000, 8001, 8003, 8004, 8005, 8080, 8088, 5173)) {
    $listen = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listen) {
      Write-Host "  LISTEN  :$p" -ForegroundColor Green
    } else {
      Write-Host "  closed  :$p" -ForegroundColor DarkGray
    }
  }

  Write-Host "=== binaries ===" -ForegroundColor Cyan
  foreach ($b in @(
      "micro\bin\user.exe",
      "micro\bin\video.exe",
      "micro\bin\media.exe",
      "micro\bin\social.exe",
      "micro\bin\search.exe",
      "micro\bin\gateway.exe"
    )) {
    $path = Join-Path $Root $b
    if (Test-Path $path) {
      Write-Host "  OK  $b" -ForegroundColor Green
    } else {
      Write-Host "  --  $b" -ForegroundColor Yellow
    }
  }

  Write-Host ""
  Write-Host "Need Docker Desktop for PG/Redis/MinIO, and ffmpeg for HLS." -ForegroundColor Yellow
  Write-Host "Compose: cd deploy\platform ; docker compose up -d" -ForegroundColor Yellow
}

function Build-Bins {
  $micro = Join-Path $Root "micro"
  Set-Location $micro
  New-Item -ItemType Directory -Force -Path "bin" | Out-Null
  Write-Host "building user/video/media/social/search/gateway..." -ForegroundColor Cyan
  go build -o bin/user.exe ./app/user/cmd
  go build -o bin/video.exe ./app/video/cmd
  go build -o bin/media.exe ./app/media/cmd
  go build -o bin/social.exe ./app/social/cmd
  go build -o bin/search.exe ./app/search/cmd
  go build -o bin/gateway.exe ./app/gateway/cmd
  # comet 属弹幕分布式版（独立 module），产物仍放到 micro\bin 便于统一启动
  Set-Location (Join-Path $RepoRoot "danmu\distributed")
  go build -o (Join-Path $micro "bin\comet.exe") ./comet
  Set-Location $Root
  Write-Host "done -> micro\bin\" -ForegroundColor Green
}

function Start-Gateway {
  $micro = Join-Path $Root "micro"
  $exe = Join-Path $micro "bin\gateway.exe"
  if (-not (Test-Path $exe)) {
    Write-Host "building gateway..." -ForegroundColor Cyan
    Set-Location $micro
    New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    go build -o bin/gateway.exe ./app/gateway/cmd
  }
  Write-Host "gateway http://localhost:8088  (user/video/ws)" -ForegroundColor Green
  Set-Location $micro
  & ".\bin\gateway.exe"
}

function Start-Frontend {
  $web = Join-Path $Root "web-app"
  Set-Location $web
  if (-not (Test-Path "node_modules\react-router-dom")) {
    Write-Host "Installing frontend deps via npmmirror..." -ForegroundColor Cyan
    npm install --registry=https://registry.npmmirror.com
  }
  # 默认经 Vite 代理直连后端；若 gateway 已在 8088，可设 USE_GATEWAY=1
  if ($env:USE_GATEWAY -eq "1") {
    $env:VITE_API_BASE = "http://localhost:8088"
    $env:VITE_DANMU_WS = "ws://localhost:8088/ws"
    Write-Host "frontend via gateway :8088" -ForegroundColor Green
  } else {
    Write-Host "frontend via Vite proxy (user:8001 video:8003)" -ForegroundColor Green
  }
  Write-Host "Starting Vite http://localhost:5173" -ForegroundColor Green
  npm run dev
}

function Show-Help {
  Write-Host "X-Plore VOD - startup order"
  Write-Host ""
  Write-Host "[1] Infra (Docker Desktop required)"
  Write-Host "    cd $Root\deploy\platform"
  Write-Host "    docker compose up -d"
  Write-Host ""
  Write-Host "[2] Backend (separate terminals under wavehub\micro)"
  Write-Host "    go run ./app/user/cmd"
  Write-Host "    go run ./app/video/cmd"
  Write-Host "    `$env:ENABLE_TRACK_WORKER='false'; go run ./app/media/cmd"
  Write-Host "    # danmu comet (under danmu\distributed):"
  Write-Host "    `$env:JWT_SECRET='dev-only-change-me'; go run ./comet -ws-addr=:8080"
  Write-Host ""
  Write-Host "[3] Gateway (unified :8088)"
  Write-Host "    .\scripts\dev-platform.ps1 gateway"
  Write-Host "    curl http://localhost:8088/health"
  Write-Host ""
  Write-Host "[4] Frontend"
  Write-Host "    .\scripts\dev-platform.ps1 frontend"
  Write-Host "    # or via gateway:"
  Write-Host "    `$env:USE_GATEWAY='1'; .\scripts\dev-platform.ps1 frontend"
  Write-Host ""
  Write-Host "[5] Smoke"
  Write-Host "    .\scripts\smoke-vod.ps1 -SampleMp4 path\to\short.mp4"
  Write-Host ""
  Write-Host "[6] Production checklist"
  Write-Host "    deploy\platform\PRODUCTION.md"
  Write-Host "    deploy\platform\.env.example"
  Write-Host ""
  Write-Host "[7] Demo / health"
  Write-Host "    ..\docs\DEMO.md"
  Write-Host "    .\scripts\check-platform.ps1"
  Write-Host ""
  Write-Host "Subcommands: deps | build | gateway | frontend | help"
}

switch ($Cmd) {
  "deps" { Show-Deps }
  "build" { Build-Bins }
  "gateway" { Start-Gateway }
  "frontend" { Start-Frontend }
  default { Show-Help }
}
