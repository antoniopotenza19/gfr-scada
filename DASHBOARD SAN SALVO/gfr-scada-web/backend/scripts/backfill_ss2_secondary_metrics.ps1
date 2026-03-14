param(
    [string]$PythonPath = ".\\venv\\Scripts\\python.exe",
    [string]$RecentFrom = "2026-02-01",
    [string]$RecentTo = "2026-03-15",
    [string]$HistoryFrom = "2025-11-01",
    [string]$HistoryTo = "2026-04-01"
)

$scriptPath = "backend\\scripts\\backfill_aggregates.py"

$runs = @(
    @("--dataset", "sale", "--sale-ids", "2", "--granularity", "1min", "--from", $RecentFrom, "--to", $RecentTo, "--truncate-target-range", "--chunk-unit", "day"),
    @("--dataset", "sale", "--sale-ids", "2", "--granularity", "15min", "--from", $RecentFrom, "--to", $RecentTo, "--truncate-target-range", "--chunk-unit", "day"),
    @("--dataset", "sale", "--sale-ids", "2", "--granularity", "1h", "--from", $RecentFrom, "--to", $RecentTo, "--truncate-target-range", "--chunk-unit", "week"),
    @("--dataset", "sale", "--sale-ids", "2", "--granularity", "1d", "--from", $HistoryFrom, "--to", $RecentTo, "--truncate-target-range", "--chunk-unit", "month"),
    @("--dataset", "sale", "--sale-ids", "2", "--granularity", "1month", "--from", $HistoryFrom, "--to", $HistoryTo, "--truncate-target-range", "--chunk-unit", "month")
)

foreach ($args in $runs) {
    Write-Host ""
    Write-Host "Running backfill:" $args
    & $PythonPath $scriptPath @args
    if ($LASTEXITCODE -ne 0) {
        throw "Backfill failed with exit code $LASTEXITCODE for args: $($args -join ' ')"
    }
}

Write-Host ""
Write-Host "SS2 secondary metrics backfill completed."
