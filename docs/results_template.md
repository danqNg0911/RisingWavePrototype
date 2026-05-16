# Results Template

Snapshot time:

- UTC: `2026-05-16 10:00:03.461`
- ICT (+07:00): `2026-05-16 17:00:03.461`

| Metric | Result | Notes |
|---|---:|---|
| Generated persons | 420 | `bench_persons` count |
| Generated auctions | 610 | `bench_auctions` count |
| Generated bids/transactions | 3600 | `clean_transactions` count |
| Risk candidates | 3111 | `risk_candidates` count |
| AI-scored events | 439 | `ai_scored_events` count at snapshot |
| RPA decisions | 2501 | `rpa_decisions` count |
| Dispatch rows | 2813 | `workflow_dispatch_log` count |
| Dispatch success rate | 100.00% | all `workflow_dispatch_log` rows are `DISPATCHED` |
| Average decision latency | 362.53 seconds | `decision_time - event_time` |
| Average dispatch latency | 1162.66 seconds | `dispatched_at - decision_time` for dispatched rows |
| Dashboard availability | HTTP 200 | `http://localhost:8501` |

> This snapshot reflects a post-drain `hf_api` + `openflow_queue` run where dispatch backlog is fully cleared. `rpa_decisions` still outpace `ai_scored_events` because rule-based fallback remains active while real Hugging Face inference continues to catch up.

> These prototype results demonstrate integration feasibility only. They are not used as a formal performance comparison. Formal streaming performance evidence is based on published RisingWave Nexmark benchmark results.
