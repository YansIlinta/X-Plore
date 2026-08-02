# X-Plore platform health check (no side effects)
# Usage: .\scripts\check-platform.ps1

$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
if (-not (Test-Path (Join-Path $Root "EVOLUTION.md"))) { $Root = "D:\X-Plore" }

function Ok($m) { Write-Host "  [OK]  $m" -ForegroundColor Green }
function Bad($m) { Write-Host "  [!!]  $m" -ForegroundColor Yellow }
function Fail($m) { Write-Host "  [XX]  $m" -ForegroundColor Red }

function HasCmd($n) { return [bool](Get-Command $n -ErrorAction SilentlyContinue) }

function PortOpen($p) {
  return [bool](Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1)
}

function HttpOk($url) {
  try {
    $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3
    return $r.StatusCode -ge 200 -and $r.StatusCode -lt 500
  } catch {
    return $false
  }
}

Write-Host "=== X-Plore check-platform ===" -ForegroundColor Cyan
Write-Host "root: $Root"
Write-Host ""

Write-Host "Tools" -ForegroundColor Cyan
if (HasCmd go) { Ok "go $(go version 2>$null | ForEach-Object { $_ })" } else { Fail "go missing" }
if (HasCmd node) { Ok "node $(node -v)" } else { Fail "node missing" }
if (HasCmd npm) { Ok "npm $(npm -v)" } else { Fail "npm missing" }
if (HasCmd docker) { Ok "docker" } else { Bad "docker missing (need for PG/Redis/MinIO)" }
if (HasCmd ffmpeg) { Ok "ffmpeg" } else { Bad "ffmpeg missing (need for HLS)" }
if (HasCmd ffprobe) { Ok "ffprobe" } else { Bad "ffprobe missing" }

Write-Host ""
Write-Host "Ports (listen)" -ForegroundColor Cyan
$map = @{
  5432 = "postgres"
  6379 = "redis"
  9000 = "minio"
  8001 = "user"
  8003 = "video"
  8080 = "comet"
  8088 = "gateway"
  5173 = "web-app"
}
foreach ($p in ($map.Keys | Sort-Object)) {
  $name = $map[$p]
  if (PortOpen $p) { Ok ":$p $name" } else { Bad ":$p $name closed" }
}

Write-Host ""
Write-Host "HTTP probes" -ForegroundColor Cyan
$probes = @(
  @{ u = "http://127.0.0.1:8088/health"; n = "gateway /health" },
  @{ u = "http://127.0.0.1:8088/metrics"; n = "gateway /metrics" },
  @{ u = "http://127.0.0.1:8001/v1/login"; n = "user (may 405/400)" },
  @{ u = "http://127.0.0.1:8003/v1/videos"; n = "video list" },
  @{ u = "http://127.0.0.1:5173/"; n = "vite" }
)
foreach ($p in $probes) {
  if (HttpOk $p.u) { Ok $p.n } else { Bad "$($p.n) unreachable" }
}

Write-Host ""
Write-Host "Binaries" -ForegroundColor Cyan
foreach ($b in @(
    "wavehub-micro\bin\user.exe",
    "wavehub-micro\bin\video.exe",
    "wavehub-micro\bin\media.exe",
    "wavehub-micro\bin\gateway.exe",
    "wavehub-micro\bin\comet.exe"
  )) {
  $path = Join-Path $Root $b
  if (Test-Path $path) { Ok $b } else { Bad "$b not built (run: .\scripts\dev-platform.ps1 build)" }
}

Write-Host ""
Write-Host "Docs" -ForegroundColor Cyan
foreach ($d in @("DEMO.md", "EVOLUTION.md", "PROJECT.md", "deploy\platform\PRODUCTION.md")) {
  if (Test-Path (Join-Path $Root $d)) { Ok $d } else { Fail "$d missing" }
}

Write-Host ""
Write-Host "Next: see DEMO.md for full startup & demo script." -ForegroundColor Cyan
Write-Host "     .\scripts\dev-platform.ps1 help"
