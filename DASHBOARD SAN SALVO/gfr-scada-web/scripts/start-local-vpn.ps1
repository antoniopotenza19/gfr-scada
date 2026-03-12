Param(
  [string]$EnvFile = ".env.vpn",
  [switch]$RunMigrations,
  [switch]$RunSeed,
  [switch]$NoFrontend
)

$ErrorActionPreference = "Stop"

function Import-EnvFile {
  param([string]$Path)

  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith('#')) {
      return
    }

    $idx = $line.IndexOf('=')
    if ($idx -lt 1) {
      return
    }

    $name = $line.Substring(0, $idx).Trim()
    $value = $line.Substring($idx + 1).Trim()

    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
      $value = $value.Substring(1, $value.Length - 2)
    }

    [Environment]::SetEnvironmentVariable($name, $value, "Process")
  }
}

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

function Resolve-PythonCommand {
  param([string]$Root)

  $candidates = @()

  $candidates += @(
    (Join-Path $Root "venv\Scripts\python.exe")
  )

  if ($env:VIRTUAL_ENV) {
    $activePython = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if ($activePython -ne (Join-Path $Root "venv\Scripts\python.exe")) {
      $candidates += $activePython
    }
  }

  foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path $candidate)) {
      $importCheck = & $candidate -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('sqlalchemy') and importlib.util.find_spec('pymysql') else 1)" 2>$null
      if ($LASTEXITCODE -eq 0) {
        return $candidate
      }
    }
  }

  throw "Python environment not found with required packages. Use the root venv (venv\\Scripts\\python.exe) with sqlalchemy and pymysql installed."
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root

$envPath = Join-Path $root $EnvFile
if (-not (Test-Path $envPath)) {
  throw "Env file not found: $EnvFile. Create it from .env.vpn.example."
}

$backendPython = Resolve-PythonCommand -Root $root
Write-Host "==> Using Python: $backendPython" -ForegroundColor Cyan

if (-not (Get-Command npm.cmd -ErrorAction SilentlyContinue)) {
  throw "npm.cmd not found in PATH."
}

Import-EnvFile -Path $envPath

if (-not $env:DATABASE_URL) {
  throw "DATABASE_URL is required in $EnvFile."
}

if (-not $env:JWT_SECRET) {
  throw "JWT_SECRET is required in $EnvFile."
}

if (-not $env:GFR_ENERGYSAVING_DATABASE_URL -and -not $env:SCADA_ENERGY_DATABASE_URL -and -not $env:MYSQL_DATABASE_URL) {
  throw "One of GFR_ENERGYSAVING_DATABASE_URL, SCADA_ENERGY_DATABASE_URL or MYSQL_DATABASE_URL is required in $EnvFile."
}

if ($RunMigrations) {
  Invoke-CommandStrict "Running migrations against DATABASE_URL from $EnvFile" {
    Set-Location (Join-Path $root "backend")
    & $backendPython -m alembic upgrade head
  }

  if ($RunSeed) {
    Invoke-CommandStrict "Running seed against DATABASE_URL from $EnvFile" {
      Set-Location (Join-Path $root "backend")
      & $backendPython app/scripts/seed.py
    }
  }

  Set-Location $root
}

$backendPath = Join-Path $root "backend"
$frontendPath = Join-Path $root "frontend"
$backendCmd = "Set-Location '$backendPath'; & '$backendPython' -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload"

Write-Host "==> Starting backend locally in a new PowerShell window" -ForegroundColor Cyan
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $backendCmd) | Out-Null

if (-not $NoFrontend) {
  $frontendCmd = "Set-Location '$frontendPath'; npm.cmd run dev"
  Write-Host "==> Starting frontend locally in a new PowerShell window" -ForegroundColor Cyan
  Start-Process powershell -ArgumentList @("-NoExit", "-Command", $frontendCmd) | Out-Null
}

Write-Host ""
Write-Host "Local VPN start command completed." -ForegroundColor Green
Write-Host "Backend:  http://127.0.0.1:8000"
Write-Host "Frontend: http://localhost:5173"
Write-Host "Env file: $EnvFile"
if (-not $RunMigrations) {
  Write-Host "Migrations: skipped (use -RunMigrations if needed)"
}
if (-not $RunSeed) {
  Write-Host "Seed: skipped (use -RunSeed together with -RunMigrations only on local/dev DB)"
}
