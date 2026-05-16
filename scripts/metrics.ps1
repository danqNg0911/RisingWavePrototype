$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Test-Path ".env")) {
    Write-Host "Missing .env. Copy .env.example to .env before running metrics."
    exit 1
}

$dsnLine = Get-Content ".env" | Where-Object { $_ -match "^RISINGWAVE_DSN_HOST=" } | Select-Object -First 1
$dsn = if ($dsnLine) { $dsnLine.Substring($dsnLine.IndexOf("=") + 1) } else { "postgresql://root@localhost:4566/dev" }

psql $dsn -v ON_ERROR_STOP=1 -f "sql/99_metrics_queries.sql"
