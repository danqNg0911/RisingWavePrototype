# RisingWave AI/RPA Prototype Report

## 1. Executive Summary

This prototype demonstrates a local, Windows-first streaming pipeline that extends a RisingWave core with real external integrations:

- Nexmark-style event generation
- Kafka-compatible ingestion through Redpanda
- Streaming transformations and risk filtering in RisingWave
- A Hugging Face-backed AI scoring worker
- An OpenFlow-backed RPA dispatch worker
- A Streamlit dashboard for read-only monitoring

```text
generator -> Redpanda -> RisingWave -> risk_candidates -> AI scorer
         -> ai_scored_events -> rpa_decisions -> RPA worker -> workflow_dispatch_log
```

The prototype is an integration demo, not a formal benchmark. Its purpose is to prove the end-to-end path from stream ingestion to AI-assisted RPA dispatch using the current real-mode configuration.

## 2. Runtime Profile

The run documented here used the checked-in real integration profile:

- `AI_MODE=hf_api`
- `HF_MODEL_ID=facebook/bart-large-mnli`
- `RPA_MODE=openflow_queue`
- `OPENRPA_QUEUE_NAME=risingwave_rpa_decisions`
- `EVENTS_PER_SECOND=20`
- `RUN_SECONDS=180`

Important runtime consequence:

- The generator can produce data much faster than a single Hugging Face API consumer can score it.
- `rpa_decisions` are derived from `ai_decision_context`, which falls back to rule risk while inference is still pending.
- Because of that design, `rpa_decisions` and `workflow_dispatch_log` can continue to move even while `ai_scored_events` is still catching up.

## 3. Execution Flow

The run followed this sequence:

1. `.\scripts\down.ps1`
2. `.\scripts\up.ps1`
3. `.\scripts\init-sql.ps1`
4. Start `ai_scorer`, `rpa_worker`, and `dashboard`
5. Run `generator`
6. Capture live metrics from RisingWave and service logs

Observed runtime evidence:

- `ai_scorer` issued live `POST` calls to `https://router.huggingface.co/.../facebook/bart-large-mnli`
- `rpa_worker` normalized the configured OpenIAP UI URL to `grpc://grpc.app.openiap.io:443`
- `rpa_worker` persisted non-null `external_workitem_id` values in `workflow_dispatch_log`
- Dashboard returned HTTP `200` at `http://localhost:8501`

## 4. Snapshot

Snapshot time:

- UTC: `2026-05-16 10:00:03.461`
- ICT (+07:00): `2026-05-16 17:00:03.461`

This snapshot was taken after the OpenFlow dispatch backlog had fully drained so the report would reflect the current steady dispatch state instead of the earlier in-flight backlog.

### 4.1 Source and pipeline counts

| Metric | Value |
|---|---:|
| Generated persons | 420 |
| Generated auctions | 610 |
| Total transactions in `clean_transactions` | 3600 |
| Total risk candidates | 3111 |
| Total AI-scored events | 439 |
| Total actionable RPA decisions | 2501 |
| Total rows in `workflow_dispatch_log` | 2813 |

### 4.2 Coverage percentages

| Metric | Formula | Result |
|---|---|---:|
| Risk candidate rate | `3111 / 3600` | 86.42% |
| AI scoring coverage | `439 / 3111` | 14.11% |
| Decision coverage | `2501 / 3111` | 80.39% |
| Dispatch success rate | `2813 / 2813` | 100.00% |

### 4.3 Action distribution

| Action | Count | Share |
|---|---:|---:|
| `BLOCK_AND_CREATE_TICKET` | 1784 | 71.33% |
| `SEND_WARNING` | 369 | 14.75% |
| `CREATE_TICKET_AND_NOTIFY` | 348 | 13.91% |

### 4.4 Dispatch execution snapshot

| Status | Count |
|---|---:|
| `DISPATCHED` | 2813 |

Derived values:

| Metric | Result |
|---|---:|
| Average decision latency | 362.53 seconds |
| Average dispatch latency | 1162.66 seconds |
| Maximum observed final risk score | 0.99 |

### 4.5 Model-version snapshot

| Model version | Count |
|---|---:|
| `hf_api:facebook/bart-large-mnli` | 439 |

At snapshot time, no fallback rows were present in `ai_scored_events`; all completed AI rows came from the live Hugging Face path.

### 4.6 Top risky users

| User ID | Risky events | Average score | Max score |
|---|---:|---:|---:|
| 103 | 580 | 0.8965 | 0.9514 |
| 102 | 482 | 0.8984 | 0.9900 |
| 101 | 345 | 0.9001 | 0.9874 |
| 106 | 21 | 0.8247 | 0.9334 |
| 131 | 21 | 0.7919 | 0.9499 |

### 4.7 Representative OpenFlow workitems

| External workitem id | External state | Transaction ID | Action |
|---|---|---|---|
| `6a08401a058b25cd1d4f1cb1` | `new` | `1cab9fef1ae98c5dcd96c925e8c9749e` | `CREATE_TICKET_AND_NOTIFY` |
| `6a084019058b25cd1d4f1cb0` | `new` | `a6900fb696a99c1a591fe1b6fa3b3356` | `CREATE_TICKET_AND_NOTIFY` |
| `6a083fd0058b25cd1d4f1cac` | `new` | `33a9888fdde10495f98258f12597ed1e` | `CREATE_TICKET_AND_NOTIFY` |

## 5. Interpretation

### 5.1 What is confirmed

- RisingWave ingested the full generated event set.
- Real Hugging Face inference is active and persisting `hf_api:facebook/bart-large-mnli` rows.
- Real OpenFlow dispatch is active and persisting external workitem ids.
- The OpenFlow queue backlog has fully drained and all `workflow_dispatch_log` rows are now `DISPATCHED`.
- The dashboard is reachable and returns live SQL-backed metrics.

### 5.2 What still lags after queue drain

The current generator configuration produces 3600 transaction rows in three minutes. The AI worker processes roughly one Hugging Face request per second, so the live scoring path cannot keep up with ingestion in this configuration. That is why:

- `risk_candidates` grows much faster than `ai_scored_events`
- `rpa_decisions` exists for many rows before real inference completes
- dispatch completion can reach 100% even though AI scoring coverage is still partial

This is expected behavior for the current single-consumer real-mode prototype. It is not a dashboard bug.

### 5.3 Why `rpa_decisions` exceeds `ai_scored_events`

`ai_decision_context` uses:

```text
COALESCE(a.ai_score, r.rule_risk_score)
```

and

```text
COALESCE(a.inference_time, r.event_time)
```

So the decision layer remains operational even when AI scoring is still pending. This keeps the RPA path live, but it also means a snapshot can show many more decisions than completed AI inference rows.

### 5.4 Why dispatch rows exceed decision rows

`workflow_dispatch_log` stores action history keyed by `action_id`, not a one-row mirror of the latest `rpa_decisions` view. A transaction can therefore contribute more than one dispatch record over time if its recommended action changes, which explains why `2813` dispatch rows coexist with `2501` current decision rows.

## 6. Gap Analysis Against Targets

| KPI | Target | Current snapshot | Status |
|---|---:|---:|---|
| Ingestion completion | 100% | 3600 generated transactions reached `clean_transactions` | Met |
| AI scoring coverage | >= 95% | 14.11% | Not met at snapshot |
| Decision coverage | >= 90% | 80.39% | Not met at snapshot |
| Worker execution success | >= 95% after queue drains | 100.00% after backlog drain | Met |
| Demo dashboard availability | 100% | HTTP 200 on `localhost:8501` | Met |
| Avg decision latency | < 300s | 362.53s | Not met at snapshot |

## 7. Known Limitations

- RisingWave runs in `single_node --in-memory` mode, which is not production-safe.
- A single external Hugging Face consumer is slower than the current generator profile.
- OpenFlow dispatch is working, but backlog drain time is substantial under the current event volume.
- Final OpenRPA bot completion state is not written back into RisingWave, so the dashboard measures dispatch state rather than downstream business completion.
- The generator intentionally biases toward suspicious behavior so that actions are visible quickly in demos.
- This is not an official RisingWave benchmark reproduction and should not be presented as one.

## 8. Recommended Next Steps

- Reduce `RUN_SECONDS` or `EVENTS_PER_SECOND` for a cleaner real-mode demo where AI scoring can catch up before screenshots are taken.
- Add AI worker parallelism or batching if higher-volume real-mode runs are required.
- Point `OPENFLOW_URL` directly to `grpc://grpc.app.openiap.io:443` to avoid relying on runtime normalization.
- Add a final reporting script that captures one SQL snapshot and writes both the dashboard summary and results template from the same timestamp.
- Add downstream state synchronization so OpenRPA completion can be measured inside RisingWave rather than only queue dispatch.

## 9. Reporting Statement

Use this statement in presentations and written reports:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.
