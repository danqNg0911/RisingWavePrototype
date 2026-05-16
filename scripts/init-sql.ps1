$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Test-Path ".env")) {
    Write-Host "Missing .env. Copy .env.example to .env before initializing."
    exit 1
}

$dsnLine = Get-Content ".env" | Where-Object { $_ -match "^RISINGWAVE_DSN_HOST=" } | Select-Object -First 1
$dsn = if ($dsnLine) { $dsnLine.Substring($dsnLine.IndexOf("=") + 1) } else { "postgresql://root@localhost:4566/dev" }

$topics = @(
    "nexmark_persons",
    "nexmark_auctions",
    "nexmark_bids",
    "risk_candidates",
    "rpa_decisions"
)

foreach ($topic in $topics) {
    $topicOutput = docker compose exec -T redpanda rpk topic create $topic 2>&1
    if ($LASTEXITCODE -ne 0) {
        if ($topicOutput -match "TOPIC_ALREADY_EXISTS" -or $topicOutput -match "already exists") {
            Write-Host "Topic $topic already exists."
        } else {
            Write-Host $topicOutput
            throw "Failed to create topic $topic"
        }
    }
}

$sqlFiles = @(
    "sql/00_create_tables.sql",
    "sql/01_create_streaming_views.sql",
    "sql/02_create_ai_tables.sql",
    "sql/03_create_rpa_decisions.sql",
    "sql/04_create_sinks.sql"
)

foreach ($file in $sqlFiles) {
    Write-Host "Applying $file"
    psql $dsn -v ON_ERROR_STOP=1 -f $file
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to apply $file"
    }
}

Write-Host "SQL initialization complete."
