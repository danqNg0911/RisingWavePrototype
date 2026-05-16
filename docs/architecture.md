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
  -> rpa_worker
  -> workflow_audit_log
  -> Streamlit dashboard + metrics SQL
```

## Components

- `redpanda`: Kafka-compatible broker for ingestion and downstream sinks.
- `risingwave`: Local single-node streaming database for tables, materialized views, and sinks.
- `generator`: Emits deterministic Nexmark-style events with built-in risky patterns.
- `ai_scorer`: Consumes `risk_candidates`, applies deterministic scoring, writes `ai_scored_events`.
- `rpa_worker`: Consumes `rpa_decisions`, simulates actions, writes `workflow_audit_log`.
- `dashboard`: Queries RisingWave directly for KPIs and recent workflow activity.

## Design choices

- AI scoring is asynchronous through Kafka to keep setup simpler than external UDFs.
- Audit and scored-event state stay inside RisingWave to avoid adding another operational database.
- Sink topics use upsert JSON to make consumer reprocessing more predictable.
- PowerShell scripts are the primary operational interface for the Windows local environment.

