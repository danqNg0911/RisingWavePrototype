# RisingWave AI/RPA Local Prototype

This repository contains a Windows-first local prototype that extends a Nexmark-style streaming pipeline with an AI scoring layer and an RPA decision worker.

The prototype flow is:

```text
generator -> Kafka/Redpanda -> RisingWave -> materialized views -> Kafka sinks
         -> AI scorer -> ai_scored_events -> rpa_decisions -> RPA worker/bridge -> workflow_dispatch_log
```

This is an integration demo, not a formal benchmark. Formal streaming benchmark evidence belongs to RisingWave's official Nexmark benchmark track.

## Stack

- Redpanda for Kafka-compatible topics
- RisingWave `single_node` for local stream processing
- Python generator for Nexmark-style `person`, `auction`, `bid` events
- Python AI scorer worker consuming `risk_candidates`
- Python RPA bridge consuming `rpa_decisions` and dispatching OpenFlow workitems
- Streamlit dashboard backed by RisingWave SQL

## Prerequisites

- Windows PowerShell
- Docker Desktop with Linux containers enabled and daemon running
- `docker compose`
- `psql`

## Quick start

1. Copy `.env.example` to `.env` and adjust values only if needed.
   The repository already includes a local `.env` with safe defaults for the prototype.
   `.env.example` now contains the recommended Hugging Face and OpenFlow/OpenRPA variables for the real integration path.
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
services/rpa_worker/    Kafka consumer + OpenFlow/OpenRPA dispatch bridge
services/dashboard/     Streamlit UI
scripts/                Windows-first PowerShell entrypoints
docs/                   Architecture, methodology, benchmark positioning, demo flow
```

## Runtime defaults

- `REDPANDA_IMAGE=redpandadata/redpanda:v25.1.2`
- `RISINGWAVE_IMAGE=risingwavelabs/risingwave:v2.8.3-aa2cf138`
- `EVENTS_PER_SECOND=20`
- `RUN_SECONDS=180`
- Safe local `.env`: `AI_MODE=mock`, `RPA_MODE=mock`
- Recommended real `.env.example`: `AI_MODE=hf_api`, `RPA_MODE=openflow_queue`

## Real Integration Config

Recommended variables for the upgraded path:

- `AI_MODE=hf_api`
- `HF_TOKEN`
- `HF_MODEL_ID=facebook/bart-large-mnli`
- `HF_PROVIDER_POLICY=preferred`
- `RPA_MODE=openflow_queue`
- `OPENFLOW_URL=grpc://grpc.app.openiap.io:443`
- `OPENFLOW_USERNAME` and `OPENFLOW_PASSWORD`, or `OPENFLOW_TOKEN`
- `OPENRPA_QUEUE_NAME`
- `OPENRPA_ENTRY_WORKFLOW`
- `OPENFLOW_MAX_DISPATCH_RETRIES=6`
- `OPENFLOW_BACKLOG_BATCH_SIZE=3`

## Switching To Real Mode

The repository keeps `.env` in a safe local configuration:

- `AI_MODE=mock`
- `RPA_MODE=mock`

To cut over to real integrations:

1. Open `.env`.
2. Change `AI_MODE=hf_api`.
3. Set `HF_TOKEN`.
4. Keep `HF_MODEL_ID=facebook/bart-large-mnli` or replace it with your chosen zero-shot model.
5. Change `RPA_MODE=openflow_queue`.
6. Set `OPENFLOW_URL` to a real SDK endpoint such as `grpc://grpc.app.openiap.io:443`.
7. Set either `OPENFLOW_TOKEN` or `OPENFLOW_USERNAME` with `OPENFLOW_PASSWORD`.
8. Set `OPENRPA_QUEUE_NAME`.
9. Recreate the workers:

```powershell
.\scripts\run-ai.ps1
.\scripts\run-rpa.ps1
```

If you want a production-like template instead of the safe local one, start from `.env.example`.

Expected verification after cutover:

- `docker compose logs -f ai_scorer` shows `mode=hf_api` or `hf_api_fallback_rule`
- `docker compose logs -f rpa_worker` shows `mode=openflow_queue` and non-null `external_workitem_id`
- `SELECT dispatch_mode, status, COUNT(*) FROM workflow_dispatch_log GROUP BY dispatch_mode, status;`
- `SELECT model_version, COUNT(*) FROM ai_scored_events GROUP BY model_version;`
- If you accidentally paste the OpenIAP UI URL such as `https://app.openiap.io/ui`, the worker now normalizes it internally, but using the gRPC endpoint directly is more reliable.

## Notes

- The included PowerShell scripts are the primary operational path for this repository.
- The dashboard is intentionally read-only and queries only RisingWave.
- The AI layer supports `mock`, `hf_api`, and a best-effort `hf_local` mode with rule fallback.
- The RPA layer writes `workflow_dispatch_log` and can run in `mock`, `openflow_queue`, or `openrpa_full` mode.
- A full usage-and-results report is available in [docs/prototype_report.md](./docs/prototype_report.md).
- OpenFlow/OpenRPA integration details are documented in [docs/openflow_openrpa_integration.md](./docs/openflow_openrpa_integration.md).

## Benchmark positioning

Use this wording in reports and demos:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.
