# Demo Script

## Steps

1. Start infrastructure:

```powershell
.\scripts\up.ps1
```

2. Initialize topics and SQL objects:

```powershell
.\scripts\init-sql.ps1
```

3. In separate terminals, start the real integration services first:

```powershell
.\scripts\run-ai.ps1
.\scripts\run-rpa.ps1
.\scripts\run-dashboard.ps1
```

4. Start the generator:

```powershell
.\scripts\run-generator.ps1
```

5. Query summary metrics:

```powershell
.\scripts\metrics.ps1
```

## Narrative

1. The generator produces customer, merchant, and transaction-like events in Nexmark-style form.
2. RisingWave ingests those streams and continuously updates risk-oriented materialized views.
3. `risk_candidates` acts as the pre-filter before Hugging Face inference.
4. The AI worker enriches each candidate with `ai_score`, `confidence`, and `decision_reason`.
5. RisingWave maps those scores into `rpa_decisions`.
6. The RPA worker dispatches each decision as a real OpenFlow workitem and records the dispatch trail in `workflow_dispatch_log`.
7. The dashboard presents transaction counts, risk volume, action mix, latency, and dispatch status from RisingWave.

## Demo Notes

- Dashboard URL: `http://localhost:8501`
- With `EVENTS_PER_SECOND=20` and `RUN_SECONDS=180`, Hugging Face scoring will lag behind ingestion. That is expected in the current single-consumer setup.
- `rpa_decisions` can appear earlier than `ai_scored_events` completion because the decision view falls back to rule-based risk while real inference is still pending.
- For a cleaner “all rows fully scored” demo, reduce `RUN_SECONDS`, reduce `EVENTS_PER_SECOND`, or wait for the AI and dispatch backlogs to drain before taking screenshots.
