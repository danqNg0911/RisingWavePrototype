$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Test-Path ".env")) {
    Write-Host "Missing .env. Copy .env.example to .env before starting."
    exit 1
}

docker compose up -d redpanda risingwave

$dsnLine = Get-Content ".env" | Where-Object { $_ -match "^RISINGWAVE_DSN_HOST=" } | Select-Object -First 1
$dsn = if ($dsnLine) { $dsnLine.Substring($dsnLine.IndexOf("=") + 1) } else { "postgresql://root@localhost:4566/dev" }

for ($i = 0; $i -lt 30; $i++) {
    try {
        psql $dsn -c "select 1;" | Out-Null
        Write-Host "RisingWave is reachable at $dsn"
        exit 0
    } catch {
        Start-Sleep -Seconds 3
    }
}

Write-Host "RisingWave did not become reachable in time."
exit 1
