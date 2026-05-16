# RisingWave AI/RPA Local Prototype

This repository contains a Windows-first local prototype that extends a Nexmark-style streaming pipeline with an AI scoring layer and an RPA decision worker.

The prototype flow is:

```text
generator -> Kafka/Redpanda -> RisingWave -> materialized views -> Kafka sinks
         -> AI scorer -> ai_scored_events -> rpa_decisions -> RPA worker -> workflow_audit_log
```

This is an integration demo, not a formal benchmark. Formal streaming benchmark evidence belongs to RisingWave's official Nexmark benchmark track.

## Stack

- Redpanda for Kafka-compatible topics
- RisingWave `single_node` for local stream processing
- Python generator for Nexmark-style `person`, `auction`, `bid` events
- Python AI scorer worker consuming `risk_candidates`
- Python RPA worker consuming `rpa_decisions`
- Streamlit dashboard backed by RisingWave SQL

## Prerequisites

- Windows PowerShell
- Docker Desktop with Linux containers enabled and daemon running
- `docker compose`
- `psql`

## Quick start

1. Copy `.env.example` to `.env` and adjust values only if needed.
   The repository already includes a local `.env` with safe defaults for the prototype.
2. Start infrastructure:

```powershell
.\scripts\up.ps1
```

3. Initialize Kafka topics and RisingWave SQL:

```powershell
.\scripts\init-sql.ps1
```

4. Start the generator:

```powershell
.\scripts\run-generator.ps1
```

5. Start the AI scorer and RPA worker:

```powershell
.\scripts\run-ai.ps1
.\scripts\run-rpa.ps1
```

6. Start the dashboard:

```powershell
.\scripts\run-dashboard.ps1
```

7. Query metrics:

```powershell
.\scripts\metrics.ps1
```

8. Tear everything down when finished:

```powershell
.\scripts\down.ps1
```

## Repository layout

```text
sql/                    RisingWave tables, views, sinks, and metrics SQL
services/generator/     Nexmark-style event generator
services/ai_scorer/     Kafka consumer + RisingWave writer for AI scoring
services/rpa_worker/    Kafka consumer + RisingWave writer for workflow audit
services/dashboard/     Streamlit UI
scripts/                Windows-first PowerShell entrypoints
docs/                   Architecture, methodology, benchmark positioning, demo flow
```

## Runtime defaults

- `REDPANDA_IMAGE=redpandadata/redpanda:v25.1.2`
- `RISINGWAVE_IMAGE=risingwavelabs/risingwave:v2.8.3-aa2cf138`
- `EVENTS_PER_SECOND=20`
- `RUN_SECONDS=180`
- `AI_MODE=mock`

## Notes

- The included PowerShell scripts are the primary operational path for this repository.
- The dashboard is intentionally read-only and queries only RisingWave.
- The worker layer uses deterministic scoring and idempotent audit logging to keep the demo reproducible.
- A full usage-and-results report is available in [docs/prototype_report.md](./docs/prototype_report.md).

## Benchmark positioning

Use this wording in reports and demos:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.
