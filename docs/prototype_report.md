# RisingWave AI/RPA Prototype Report

## 1. Executive Summary

This prototype demonstrates a local, Windows-first streaming pipeline that extends a RisingWave core with:

- Nexmark-style event generation
- Kafka-compatible ingestion through Redpanda
- Streaming transformations and risk filtering in RisingWave
- An asynchronous AI scoring worker
- An RPA simulation worker with audit logging
- A Streamlit dashboard for read-only monitoring

The prototype is an integration demo, not a formal benchmark. Its purpose is to prove that the end-to-end flow works locally:

```text
generator -> Redpanda -> RisingWave -> risk_candidates -> AI scorer
         -> ai_scored_events -> rpa_decisions -> RPA worker -> workflow_audit_log
```

## 2. Objectives

### 2.1 Primary objectives

- Prove that streaming data can move from generator to Kafka to RisingWave continuously.
- Prove that RisingWave materialized views can maintain risk-oriented derived state.
- Prove that AI scoring can be attached asynchronously without embedding a heavy model in the database.
- Prove that scored events can be translated into RPA-style operational actions with an auditable execution trail.

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
- `workflow_audit_log`

### 4.4 Risk scoring logic

The prototype uses two layers:

1. Rule-based candidate generation inside RisingWave.
2. Deterministic AI-style rescoring in Python.

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
| RPA worker | consumes `rpa_decisions`, writes `workflow_audit_log` |
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
  -> workflow_audit_log
```

### 5.3 Local deployment characteristics

- Windows-first operation via PowerShell scripts
- Docker Compose deployment
- Single-node RisingWave
- In-memory mode for local simplicity
- No external database for audit storage
- No external model server

## 6. Result Snapshot

Snapshot time:

- UTC: `2026-05-16 02:00:08`
- ICT (+07:00): `2026-05-16 09:00:08`

### 6.1 Core counts

| Metric | Value |
|---|---:|
| Total transactions in `clean_transactions` | 900 |
| Total risk candidates | 836 |
| Total AI-scored events | 836 |
| Total actionable RPA decisions | 781 |

### 6.2 Coverage percentages

| Metric | Formula | Result |
|---|---|---:|
| Risk candidate rate | `836 / 900` | 92.89% |
| AI scoring coverage | `836 / 836` | 100.00% |
| Decision coverage | `781 / 836` | 93.42% |

### 6.3 Action distribution

| Action | Count | Share |
|---|---:|---:|
| `BLOCK_AND_CREATE_TICKET` | 461 | 59.03% |
| `CREATE_TICKET_AND_NOTIFY` | 313 | 40.08% |
| `SEND_WARNING` | 7 | 0.90% |

### 6.4 Workflow execution snapshot

At the snapshot time the worker was still draining backlog:

| Status | Count |
|---|---:|
| `SUCCEEDED` | 819 |
| `QUEUED` | 172 |

Derived snapshot values:

| Metric | Result |
|---|---:|
| Workflow success rate at snapshot | 82.64% |
| Average decision latency | 194.59 seconds |
| Maximum observed final risk score | 0.95 |

### 6.5 Top risky users

| User ID | Risky events | Average score | Max score |
|---|---:|---:|---:|
| 103 | 149 | 0.8977 | 0.90 |
| 102 | 123 | 0.8984 | 0.90 |
| 101 | 89 | 0.9281 | 0.95 |

## 7. Interpretation Of Results

### 7.1 What worked

- The ingestion path from generator to Kafka to RisingWave worked.
- Materialized views populated continuously.
- `risk_candidates` successfully filtered suspicious events.
- AI scoring reached full coverage over current candidates.
- RPA decisioning produced multiple action levels.
- Audit logging worked and the worker proved idempotency by skipping already-succeeded actions on restart.
- The dashboard was reachable over HTTP and the metrics SQL returned meaningful output.

### 7.2 Why risk and action rates are high

The generator intentionally injects suspicious patterns frequently:

- high-value amounts
- repeated bursts by the same user
- suspicious text
- unknown channels

So the current percentages are intentionally demo-heavy. They are suitable for showcasing the pipeline, but they do not represent a normal production fraud distribution.

### 7.3 Why audit rows can exceed current decision rows

The `action_id` is based on:

```text
sha256(transaction_id + rpa_action)
```

This means one transaction can legitimately produce more than one audit record over time if its recommended action escalates, for example:

- `CREATE_TICKET_AND_NOTIFY`
- later upgraded to `BLOCK_AND_CREATE_TICKET`

That is acceptable for a prototype because it preserves action history instead of overwriting it.

## 8. Gap Analysis Against Targets

| KPI | Target | Current snapshot | Status |
|---|---:|---:|---|
| Ingestion completion | 100% | 900 generated `bid` events reached `clean_transactions` | Met |
| AI scoring coverage | >= 95% | 100.00% | Met |
| Decision coverage | >= 90% | 93.42% | Met |
| Worker execution success | >= 95% after queue drains | 82.64% while backlog still active | In progress |
| Demo dashboard availability | 100% | HTTP 200 on `localhost:8501` | Met |
| Avg decision latency | < 300s | 194.59s | Met |

## 9. Known Limitations

- RisingWave runs in `single_node --in-memory` mode, which is not production-safe.
- The AI layer is deterministic mock scoring, not a trained fraud model.
- The generator is intentionally biased toward suspicious behavior to make the demo visible quickly.
- Audit success rate at snapshot is not final because the worker was still processing backlog.
- This is not an official RisingWave benchmark reproduction and should not be presented as one.

## 10. Recommended Next Steps

### 10.1 For a stronger prototype

- Let `rpa_worker` drain the queue fully before capturing final demo screenshots.
- Add one script that starts all long-running services in separate detached mode for easier demos.
- Add a final reporting script that writes metrics directly into `docs/results_template.md`.

### 10.2 For a more realistic v2

- Reduce suspicious-pattern frequency in the generator to produce a more balanced action distribution.
- Add a configurable worker concurrency model for `rpa_worker`.
- Add final state metrics for:
  - queue drain completion time
  - actions per minute
  - success rate after steady state
- Add optional integration with n8n or OpenRPA-compatible work items.

## 11. Reporting Statement

Use this statement in presentations and written reports:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.

