# RisingWave AI/RPA Local Prototype

This repository is a Windows-first local prototype that runs a real Hugging Face scoring path on top of a RisingWave + Redpanda streaming core, then dispatches actionable results to OpenFlow/OpenRPA through `openflow_queue`.

```text
generator -> Kafka/Redpanda -> RisingWave -> materialized views -> Kafka sinks
         -> AI scorer -> ai_scored_events -> rpa_decisions -> RPA worker/bridge -> workflow_dispatch_log
```

This is an integration demo, not a formal benchmark. Formal streaming performance evidence belongs to RisingWave's official Nexmark benchmark track.

## Stack

- Redpanda for Kafka-compatible topics
- RisingWave `single_node` for local stream processing
- Python generator for Nexmark-style `person`, `auction`, `bid` events
- Python AI scorer calling Hugging Face Inference API
- Python RPA bridge dispatching OpenFlow workitems
- Streamlit dashboard backed by RisingWave SQL

## Prerequisites

- Windows PowerShell
- Docker Desktop with Linux containers enabled and daemon running
- `docker compose`
- `psql`

## Quick start

1. Review `.env` and make sure the Hugging Face and OpenFlow credentials are valid for your environment.
2. Start infrastructure:

```powershell
.\scripts\up.ps1
```

3. Initialize Kafka topics and RisingWave SQL:

```powershell
.\scripts\init-sql.ps1
```

4. Start the long-running services in separate terminals:

```powershell
.\scripts\run-ai.ps1
.\scripts\run-rpa.ps1
.\scripts\run-dashboard.ps1
```

5. Run the generator:

```powershell
.\scripts\run-generator.ps1
```

6. Query metrics:

```powershell
.\scripts\metrics.ps1
```

7. Tear everything down when finished:

```powershell
.\scripts\down.ps1
```

## Operational URLs

- Dashboard: `http://localhost:8501`
- RisingWave SQL endpoint: `localhost:4566`
- Kafka-compatible broker: `localhost:9092`

## Repository layout

```text
sql/                    RisingWave tables, views, sinks, and metrics SQL
services/generator/     Nexmark-style event generator
services/ai_scorer/     Kafka consumer + Hugging Face scoring worker
services/rpa_worker/    Kafka consumer + OpenFlow/OpenRPA dispatch bridge
services/dashboard/     Streamlit UI
scripts/                Windows-first PowerShell entrypoints
docs/                   Architecture, report, demo flow, integration notes
```

## Current Runtime Profile

The current repository configuration is aligned to the real integration path:

- `AI_MODE=hf_api`
- `HF_MODEL_ID=facebook/bart-large-mnli`
- `RPA_MODE=openflow_queue`
- `OPENRPA_QUEUE_NAME=risingwave_rpa_decisions`

`.env.example` keeps the same variable set with blank secrets so a fresh environment can be configured without copying live credentials.

## Runtime Notes

- The dashboard is read-only and queries RisingWave directly.
- `rpa_decisions` are created from `ai_decision_context`, so decisions can appear before Hugging Face scoring has fully drained because the view falls back to rule-based risk when inference is still pending.
- With the current `EVENTS_PER_SECOND=20` and `RUN_SECONDS=180`, the generator produces data much faster than a single Hugging Face API consumer can score it. Expect `risk_candidates`, `ai_scored_events`, and `workflow_dispatch_log` to diverge while the run is in flight.
- If `OPENFLOW_URL` is set to the OpenIAP UI URL such as `https://app.openiap.io/ui`, the worker normalizes it to the gRPC endpoint pattern internally. Using `grpc://grpc.app.openiap.io:443` directly is still the cleaner configuration.

## Docs

- Full report: [docs/prototype_report.md](./docs/prototype_report.md)
- Integration details: [docs/openflow_openrpa_integration.md](./docs/openflow_openrpa_integration.md)
- Architecture: [docs/architecture.md](./docs/architecture.md)
- Demo flow: [docs/demo_script.md](./docs/demo_script.md)

## Benchmark Positioning

Use this wording in reports and demos:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.
