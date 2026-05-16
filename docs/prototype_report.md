# RisingWave AI/RPA Prototype Report

## 1. Executive Summary

This prototype demonstrates a local, Windows-first streaming pipeline that extends a RisingWave core with:

- Nexmark-style event generation
- Kafka-compatible ingestion through Redpanda
- Streaming transformations and risk filtering in RisingWave
- An asynchronous AI scoring worker
- An RPA dispatch worker with OpenFlow/OpenRPA-ready status logging
- A Streamlit dashboard for read-only monitoring

The prototype is an integration demo, not a formal benchmark. Its purpose is to prove that the end-to-end flow works locally:

```text
generator -> Redpanda -> RisingWave -> risk_candidates -> AI scorer
         -> ai_scored_events -> rpa_decisions -> RPA worker -> workflow_dispatch_log
```

## 2. Objectives

### 2.1 Primary objectives

- Prove that streaming data can move from generator to Kafka to RisingWave continuously.
- Prove that RisingWave materialized views can maintain risk-oriented derived state.
- Prove that AI scoring can be attached asynchronously without embedding a heavy model in the database.
- Prove that the AI layer can run in either deterministic local mode or Hugging Face-backed mode.
- Prove that scored events can be translated into RPA-style operational actions with an auditable dispatch trail.
- Prove that the RPA layer can progress from local mock dispatch to OpenFlow/OpenRPA workitem dispatch without changing the RisingWave core.

### 2.2 Prototype KPI targets

These are demo targets, not production SLAs:

| KPI | Target |
|---|---:|
| End-to-end ingestion completion | 100% of generated `bid` events should appear in `clean_transactions` |
| AI scoring coverage | >= 95% of `risk_candidates` should receive an `ai_scored_events` record |
| Decision coverage | >= 90% of `risk_candidates` should map to an actionable `rpa_decisions` record |
| Worker execution success | >= 95% after the queue drains |
| Demo dashboard availability | 100% during the demo session |
| Average decision latency | < 300 seconds in local single-node demo mode |

## 3. How To Use The Prototype

### 3.1 Prerequisites

- Windows PowerShell
- Docker Desktop with Linux containers enabled
- `docker compose`
- `psql`

### 3.2 Standard run flow

1. Start infrastructure:

```powershell
.\scripts\up.ps1
```

2. Initialize Kafka topics and RisingWave SQL objects:

```powershell
.\scripts\init-sql.ps1
```

3. Start the AI scorer:

```powershell
.\scripts\run-ai.ps1
```

4. Start the RPA worker:

```powershell
.\scripts\run-rpa.ps1
```

5. Start the dashboard:

```powershell
.\scripts\run-dashboard.ps1
```

6. Run the generator:

```powershell
.\scripts\run-generator.ps1
```

7. Inspect metrics:

```powershell
.\scripts\metrics.ps1
```

### 3.3 Operational URLs and checks

- Dashboard: `http://localhost:8501`
- RisingWave SQL endpoint: `localhost:4566`
- Kafka-compatible broker: `localhost:9092`

Useful checks:

```powershell
docker compose ps
docker compose logs -f ai_scorer
docker compose logs -f rpa_worker
psql -h localhost -p 4566 -U root -d dev -c "select count(*) from clean_transactions;"
```

### 3.4 Reset and rerun

To destroy all containers, network, and volumes:

```powershell
.\scripts\down.ps1
```

Then rerun the steps above from `up.ps1`.

### 3.5 Cutover to real integrations

Safe local defaults keep the stack in mock mode:

- `AI_MODE=mock`
- `RPA_MODE=mock`

The recommended real-ready template is in `.env.example`:

- `AI_MODE=hf_api`
- `HF_TOKEN`
- `HF_MODEL_ID=facebook/bart-large-mnli`
- `RPA_MODE=openflow_queue`
- `OPENFLOW_URL=grpc://grpc.app.openiap.io:443`
- `OPENFLOW_TOKEN` or `OPENFLOW_USERNAME` and `OPENFLOW_PASSWORD`
- `OPENRPA_QUEUE_NAME`
- `OPENRPA_ENTRY_WORKFLOW`
- `OPENFLOW_MAX_DISPATCH_RETRIES=6`
- `OPENFLOW_BACKLOG_BATCH_SIZE=3`

After editing `.env`, recreate the worker services:

```powershell
.\scripts\run-ai.ps1
.\scripts\run-rpa.ps1
```

Expected runtime evidence:

- `ai_scorer` logs show `mode=hf_api` or `hf_api_fallback_rule`
- `rpa_worker` logs show `mode=openflow_queue`
- `workflow_dispatch_log.external_workitem_id` is populated for successful external dispatches
- `ai_scored_events.model_version` reflects either the Hugging Face model or the fallback rule path
- If the OpenIAP UI URL is used by mistake, the worker now normalizes it internally to the gRPC endpoint pattern, but explicit `grpc://...` is preferred.

## 4. Data Design

### 4.1 Source entities

The generator emits three Nexmark-style entities:

| Entity | Topic | Meaning in this prototype |
|---|---|---|
| `person` | `nexmark_persons` | customer or user profile |
| `auction` | `nexmark_auctions` | merchant, product, or transaction context |
| `bid` | `nexmark_bids` | transaction-like event |

### 4.2 Example risk mapping

- High `price` => high-risk transaction amount
- Repeated `bid` activity by one user => burst behavior
- `channel = unknown` => suspicious channel
- `extra.message` containing `urgent` or `suspicious` => suspicious text pattern

### 4.3 Derived streaming objects

Key RisingWave objects:

- `clean_transactions`
- `user_10min_features`
- `enriched_transactions`
- `risk_candidates`
- `ai_scored_events`
- `ai_decision_context`
- `rpa_decisions`
- `workflow_dispatch_log`

### 4.4 Risk scoring logic

The prototype uses two layers:

1. Rule-based candidate generation inside RisingWave.
2. Asynchronous rescoring in Python, running either deterministic local logic or Hugging Face-backed zero-shot classification.

The final score is:

```text
final_risk_score = GREATEST(rule_risk_score, ai_score)
```

This keeps the prototype stable, reproducible, and easy to explain.

## 5. Infrastructure Architecture

### 5.1 Runtime topology

| Component | Role |
|---|---|
| Redpanda | Kafka-compatible broker for ingress and downstream sinks |
| RisingWave `single_node` | stream processing engine, tables, MVs, sinks, internal state |
| Generator service | emits Nexmark-style JSON events |
| AI scorer worker | consumes `risk_candidates`, writes `ai_scored_events` |
| RPA worker | consumes `rpa_decisions`, dispatches workitems, writes `workflow_dispatch_log` |
| Streamlit dashboard | read-only observability UI |

### 5.2 Message and state flow

```text
nexmark_persons / nexmark_auctions / nexmark_bids
  -> connector tables in RisingWave
  -> streaming views
  -> risk_candidates_sink
  -> ai_scorer
  -> ai_scored_events
  -> ai_decision_context / rpa_decisions
  -> rpa_decisions_sink
  -> rpa_worker
  -> workflow_dispatch_log
```

### 5.3 Local deployment characteristics

- Windows-first operation via PowerShell scripts
- Docker Compose deployment
- Single-node RisingWave
- In-memory mode for local simplicity
- No external database for audit storage
- Optional external AI provider through Hugging Face
- Optional external OpenFlow/OpenRPA queue and bot runtime

## 6. Result Snapshot

Snapshot time:

- UTC: `2026-05-16 07:04:56`
- ICT (+07:00): `2026-05-16 14:04:56`

### 6.1 Core counts

| Metric | Value |
|---|---:|
| Total transactions in `clean_transactions` | 400 |
| Total risk candidates | 361 |
| Total AI-scored events | 361 |
| Total actionable RPA decisions | 328 |
| Total rows in `workflow_dispatch_log` | 309 |

### 6.2 Coverage percentages

| Metric | Formula | Result |
|---|---|---:|
| Risk candidate rate | `361 / 400` | 90.25% |
| AI scoring coverage | `361 / 361` | 100.00% |
| Decision coverage | `328 / 361` | 90.86% |

### 6.3 Action distribution

| Action | Count | Share |
|---|---:|---:|
| `BLOCK_AND_CREATE_TICKET` | 198 | 60.37% |
| `CREATE_TICKET_AND_NOTIFY` | 125 | 38.11% |
| `SEND_WARNING` | 5 | 1.52% |

### 6.4 Dispatch execution snapshot

At the snapshot time the worker was still draining backlog:

| Status | Count |
|---|---:|
| `DISPATCHED` | 204 |
| `QUEUED` | 105 |

Derived snapshot values:

| Metric | Result |
|---|---:|
| Dispatch success rate at snapshot | 66.02% |
| Average decision latency | 31.12 seconds |
| Average dispatch latency | 51.63 seconds |
| Maximum observed final risk score | 0.95 |

### 6.5 Top risky users

| User ID | Risky events | Average score | Max score |
|---|---:|---:|---:|
| 103 | 67 | 0.8978 | 0.90 |
| 102 | 56 | 0.8982 | 0.90 |
| 101 | 43 | 0.9233 | 0.95 |

### 6.6 Real integration validation snapshot

Second validation snapshot:

- UTC: `2026-05-16 08:23:50`
- ICT (+07:00): `2026-05-16 15:23:50`

This snapshot was taken after switching `.env` to:

- `AI_MODE=hf_api`
- `RPA_MODE=openflow_queue`

Observed Hugging Face scoring state:

| Model version | Count |
|---|---:|
| `hf_api:facebook/bart-large-mnli` | 1088 |
| `hf_api_fallback_rule` | 99 |

Derived Hugging Face validation:

| Metric | Formula | Result |
|---|---|---:|
| Real HF scoring share | `1088 / (1088 + 99)` | 91.66% |
| Fallback share | `99 / (1088 + 99)` | 8.34% |

Observed OpenFlow dispatch state:

| Status | Count |
|---|---:|
| `DISPATCHED` | 1455 |
| `FAILED_DISPATCH` | 39 |
| `QUEUED` | 303 |

Derived OpenFlow validation:

| Metric | Formula | Result |
|---|---|---:|
| OpenFlow dispatch success rate | `1455 / 1797` | 80.97% |
| Rows with external workitem id | direct count | 1455 |

Representative successful OpenFlow workitems:

| Transaction ID | Action | External workitem id | External state |
|---|---|---|---|
| `ad702a6724e4abe0decf7de8a0734680` | `CREATE_TICKET_AND_NOTIFY` | `6a082658058b25cd1d4f03c9` | `new` |
| `842764f24fc71b6bf61fd9c56a04198b` | `BLOCK_AND_CREATE_TICKET` | `6a082658058b25cd1d4f03c8` | `new` |
| `cc81f7b49b3d45f15ef9a4622bc1bc06` | `BLOCK_AND_CREATE_TICKET` | `6a082657058b25cd1d4f03c7` | `new` |

Interpretation of this second snapshot:

- Hugging Face real inference is confirmed by successful `POST` calls and persisted `hf_api:facebook/bart-large-mnli` rows.
- OpenFlow real dispatch is confirmed by persisted external workitem ids.
- The remaining OpenFlow gap is no longer “can it dispatch at all”, but “how reliably can backlog rows be drained without channel interruptions”.
- The current `rpa_worker` is materially more stable than the first real-mode attempt because it now reuses the OpenFlow client and retries backlog rows from `workflow_dispatch_log`.

## 7. Interpretation Of Results

### 7.1 What worked

- The ingestion path from generator to Kafka to RisingWave worked.
- Materialized views populated continuously.
- `risk_candidates` successfully filtered suspicious events.
- AI scoring reached full coverage over current candidates.
- RPA decisioning produced multiple action levels.
- Dispatch logging worked and the worker proved idempotency by skipping already-dispatched actions on restart.
- The dashboard was reachable over HTTP and the metrics SQL returned meaningful output.
- Hugging Face zero-shot scoring was validated live with persisted `hf_api:facebook/bart-large-mnli` results.
- OpenFlow workitem creation was validated live with non-null `external_workitem_id` values in `workflow_dispatch_log`.

### 7.2 Why risk and action rates are high

The generator intentionally injects suspicious patterns frequently:

- high-value amounts
- repeated bursts by the same user
- suspicious text
- unknown channels

So the current percentages are intentionally demo-heavy. They are suitable for showcasing the pipeline, but they do not represent a normal production fraud distribution.

### 7.3 Why dispatch rows can differ from current decision rows

The `action_id` is based on:

```text
sha256(transaction_id + rpa_action)
```

This means one transaction can legitimately produce more than one dispatch record over time if its recommended action escalates, for example:

- `CREATE_TICKET_AND_NOTIFY`
- later upgraded to `BLOCK_AND_CREATE_TICKET`

That is acceptable for a prototype because it preserves action history instead of overwriting it.

## 8. Gap Analysis Against Targets

| KPI | Target | Current snapshot | Status |
|---|---:|---:|---|
| Ingestion completion | 100% | 400 generated `bid` events reached `clean_transactions` | Met |
| AI scoring coverage | >= 95% | 100.00% | Met |
| Decision coverage | >= 90% | 90.86% | Met |
| Worker execution success | >= 95% after queue drains | 80.97% for current OpenFlow snapshot while backlog still active | In progress |
| Demo dashboard availability | 100% | HTTP 200 on `localhost:8501` | Met |
| Avg decision latency | < 300s | 31.12s | Met |

## 9. Known Limitations

- RisingWave runs in `single_node --in-memory` mode, which is not production-safe.
- The database now contains a mixture of baseline mock rows and later real-mode rows because multiple validation runs were executed in the same local environment.
- The OpenFlow queue path is validated, but channel interruptions still leave part of the backlog in `FAILED_DISPATCH` or `QUEUED`.
- Final downstream completion inside an OpenRPA desktop bot is not yet written back into RisingWave, so the dashboard still measures dispatch status rather than full business-task completion.
- The generator is intentionally biased toward suspicious behavior to make the demo visible quickly.
- Dispatch success rate at snapshot is not final because the worker was still processing backlog.
- This is not an official RisingWave benchmark reproduction and should not be presented as one.

## 10. Recommended Next Steps

### 10.1 For a stronger prototype

- Let `rpa_worker` drain the queue fully before capturing final demo screenshots.
- Add one script that starts all long-running services in separate detached mode for easier demos.
- Add a final reporting script that writes metrics directly into `docs/results_template.md`.
- Add a polling bridge that syncs final OpenFlow/OpenRPA workitem states back into RisingWave for true end-to-end completion metrics.
- Add a clean reset path for “real-mode only” demos so reports do not mix historic mock rows with fresh OpenFlow rows.

### 10.2 For a more realistic v2

- Reduce suspicious-pattern frequency in the generator to produce a more balanced action distribution.
- Add a configurable worker concurrency model for `rpa_worker`.
- Add final state metrics for:
  - queue drain completion time
  - actions per minute
  - success rate after steady state
- Run a live cutover test with a valid Hugging Face token and an OpenFlow queue, then capture a second report snapshot for the real integration path.
- Add OpenRPA host-side execution and state synchronization back into `workflow_dispatch_log` or a follow-up status table.

## 11. Reporting Statement

Use this statement in presentations and written reports:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.
