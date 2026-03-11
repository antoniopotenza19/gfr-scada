Param(
  [switch]$RunMigrations,
  [switch]$RunSeed,
  [switch]$NoFrontend
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "start-local-vpn.ps1"
$arguments = @("-ExecutionPolicy", "Bypass", "-File", $scriptPath, "-EnvFile", ".env")

if ($RunMigrations) {
  $arguments += "-RunMigrations"
}

if ($RunSeed) {
  $arguments += "-RunSeed"
}

if ($NoFrontend) {
  $arguments += "-NoFrontend"
}

& powershell @arguments
exit $LASTEXITCODE
