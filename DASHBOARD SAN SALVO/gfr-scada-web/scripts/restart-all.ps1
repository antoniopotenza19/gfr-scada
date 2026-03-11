Param(
  [switch]$NoBuild,
  [switch]$NoFrontend,
  [switch]$NoSeed
)

$ErrorActionPreference = "Stop"

function Invoke-CommandStrict {
  param(
    [string]$Label,
    [scriptblock]$Command
  )

  Write-Host "==> $Label" -ForegroundColor Cyan
  & $Command
  if ($LASTEXITCODE -ne 0) {
    throw "Step failed: $Label"
  }
}

function Assert-DockerDaemonAvailable {
  Write-Host "==> Checking Docker daemon" -ForegroundColor Cyan
  docker info | Out-Null
  if ($LASTEXITCODE -ne 0) {
    throw @"
Docker Desktop risulta non avviato oppure il motore Linux non e disponibile.

Apri Docker Desktop, aspetta che mostri lo stato 'Engine running', poi rilancia:
  .\start-dev.cmd
"@
  }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  throw "Docker CLI not found in PATH."
}

Assert-DockerDaemonAvailable

if (-not (Test-Path ".\frontend\package.json")) {
  throw "frontend/package.json not found. Run this script from the repository."
}

Invoke-CommandStrict "Stopping existing containers" { docker compose down }

if ($NoBuild) {
  Invoke-CommandStrict "Starting db + backend" { docker compose up -d db backend }
} else {
  Invoke-CommandStrict "Starting db + backend (build)" { docker compose up -d --build db backend }
}

Write-Host "==> Waiting for database healthcheck" -ForegroundColor Cyan
$dbReady = $false
for ($i = 1; $i -le 60; $i++) {
  $status = (docker inspect -f "{{.State.Health.Status}}" gfr_scada_db 2>$null)
  if ($status -eq "healthy") {
    $dbReady = $true
    break
  }
  Start-Sleep -Seconds 2
}

if (-not $dbReady) {
  throw "Database did not become healthy in time."
}

$migrationOk = $false
for ($attempt = 1; $attempt -le 20; $attempt++) {
  Write-Host "==> Running migrations (attempt $attempt/20)" -ForegroundColor Cyan
  docker compose exec backend alembic upgrade head
  if ($LASTEXITCODE -eq 0) {
    $migrationOk = $true
    break
  }
  Start-Sleep -Seconds 2
}

if (-not $migrationOk) {
  throw "Migration failed after 20 attempts."
}

if (-not $NoSeed) {
  Invoke-CommandStrict "Seeding dev data" { docker compose exec backend python app/scripts/seed.py }
}

if (-not $NoFrontend) {
  $frontendPath = Join-Path $root "frontend"
  $cmd = "Set-Location '$frontendPath'; npm run dev"
  Write-Host "==> Starting frontend dev server in a new PowerShell window" -ForegroundColor Cyan
  Start-Process powershell -ArgumentList @("-NoExit", "-Command", $cmd) | Out-Null
}

Write-Host ""
Write-Host "Restart complete." -ForegroundColor Green
Write-Host "Backend:  http://127.0.0.1:8000"
Write-Host "Frontend: http://localhost:5173"
