# Architecture

## Pipeline

```text
nexmark_generator
  -> nexmark_persons / nexmark_auctions / nexmark_bids
  -> RisingWave connector tables
  -> clean_transactions / user_10min_features / enriched_transactions / risk_candidates
  -> risk_candidates_sink
  -> ai_scorer
  -> ai_scored_events
  -> ai_decision_context / rpa_decisions
  -> rpa_decisions_sink
  -> rpa_worker / OpenFlow bridge
  -> workflow_dispatch_log
  -> Streamlit dashboard + metrics SQL
```

## Components

- `redpanda`: Kafka-compatible broker for ingestion and downstream sinks.
- `risingwave`: Local single-node streaming database for tables, materialized views, and sinks.
- `generator`: Emits deterministic Nexmark-style events with built-in risky patterns.
- `ai_scorer`: Consumes `risk_candidates`, calls the Hugging Face Inference API, and writes `ai_scored_events`.
- `rpa_worker`: Consumes `rpa_decisions`, pushes OpenFlow workitems through `openflow_queue`, and writes `workflow_dispatch_log`.
- `dashboard`: Queries RisingWave directly for KPIs and recent workflow activity.

## Design Choices

- AI scoring is asynchronous through Kafka to keep the database path simple and to avoid embedding model execution inside RisingWave.
- `ai_decision_context` joins `risk_candidates` to `ai_scored_events` and falls back to rule risk while inference is still pending. This is why `rpa_decisions` can outpace `ai_scored_events` during a live run.
- Dispatch status stays inside RisingWave even when the downstream execution is delegated to OpenFlow/OpenRPA.
- Sink topics use upsert JSON to make consumer reprocessing more predictable.
- PowerShell scripts are the primary operational interface for the Windows local environment.
- For demos, start `ai_scorer`, `rpa_worker`, and `dashboard` before the generator so the real external integrations begin draining immediately.
