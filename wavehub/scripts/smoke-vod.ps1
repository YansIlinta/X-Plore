# X-Plore VOD smoke: register -> create -> put sample -> complete -> poll ready
# Prerequisites: user:8001 video:8003 media worker + PG/Redis/MinIO + ffmpeg
# Usage:
#   .\scripts\smoke-vod.ps1
#   .\scripts\smoke-vod.ps1 -SampleMp4 C:\path\to\short.mp4

param(
  [string]$UserBase = "http://localhost:8001",
  [string]$VideoBase = "http://localhost:8003",
  [string]$SampleMp4 = "",
  [int]$TimeoutSec = 180
)

$ErrorActionPreference = "Stop"
$user = "smoke_" + [guid]::NewGuid().ToString("N").Substring(0, 8)
$pass = "pass1234"

function Invoke-Json($Method, $Url, $Body, $Token) {
  $headers = @{ "Content-Type" = "application/json" }
  if ($Token) { $headers["Authorization"] = "Bearer $Token" }
  $json = if ($null -ne $Body) { $Body | ConvertTo-Json -Compress } else { $null }
  if ($json) {
    return Invoke-RestMethod -Method $Method -Uri $Url -Headers $headers -Body $json
  }
  return Invoke-RestMethod -Method $Method -Uri $Url -Headers $headers
}

Write-Host "== register $user ==" -ForegroundColor Cyan
$auth = Invoke-Json POST "$UserBase/v1/register" @{ username = $user; password = $pass } $null
$token = $auth.token
if (-not $token) { $token = $auth.Token }
if (-not $token) { throw "no token in register reply: $($auth | ConvertTo-Json)" }
Write-Host "token ok userId=$($auth.userId)$($auth.user_id)"

Write-Host "== create video ==" -ForegroundColor Cyan
$created = Invoke-Json POST "$VideoBase/v1/videos" @{
  title = "smoke-test"
  description = "auto"
  category = "tech"
} $token
$vid = $created.id
$upload = $created.uploadUrl
if (-not $upload) { $upload = $created.upload_url }
if (-not $vid -or -not $upload) { throw "create failed: $($created | ConvertTo-Json)" }
Write-Host "video id=$vid"
Write-Host "uploadUrl=$upload"

# sample file
$tmp = $null
if ($SampleMp4 -and (Test-Path $SampleMp4)) {
  $filePath = $SampleMp4
} else {
  Write-Host "No sample mp4 given; writing tiny placeholder (may fail transcode)" -ForegroundColor Yellow
  $tmp = Join-Path $env:TEMP "xplore-smoke.bin"
  [IO.File]::WriteAllBytes($tmp, [byte[]](0..255))
  $filePath = $tmp
}

Write-Host "== PUT original ==" -ForegroundColor Cyan
Invoke-WebRequest -Method PUT -Uri $upload -InFile $filePath -ContentType "video/mp4" | Out-Null
Write-Host "put ok"

Write-Host "== complete ==" -ForegroundColor Cyan
$done = Invoke-Json POST "$VideoBase/v1/videos/$vid/complete" @{} $token
Write-Host "status=$($done.status)"

Write-Host "== poll ready (timeout ${TimeoutSec}s) ==" -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds($TimeoutSec)
$info = $null
while ((Get-Date) -lt $deadline) {
  Start-Sleep -Seconds 2
  $info = Invoke-Json GET "$VideoBase/v1/videos/$vid" $null $null
  $st = $info.status
  Write-Host "  status=$st"
  if ($st -eq "ready" -or $st -eq "failed") { break }
}
if (-not $info) { throw "no detail" }
$playlist = $info.playlistUrl
if (-not $playlist) { $playlist = $info.playlist_url }
$room = $info.roomId
if (-not $room) { $room = $info.room_id }

Write-Host ""
Write-Host "=== RESULT ===" -ForegroundColor Green
Write-Host "id=$vid status=$($info.status) room=$room"
Write-Host "playlist=$playlist"
if ($info.status -ne "ready") {
  Write-Host "NOT ready (need media worker + ffmpeg + valid mp4)" -ForegroundColor Yellow
  exit 2
}
Write-Host "SMOKE OK" -ForegroundColor Green
if ($tmp) { Remove-Item $tmp -ErrorAction SilentlyContinue }
