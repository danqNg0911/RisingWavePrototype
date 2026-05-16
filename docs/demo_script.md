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

3. Start the generator:

```powershell
.\scripts\run-generator.ps1
```

4. In another terminal, start the AI scorer:

```powershell
.\scripts\run-ai.ps1
```

5. In another terminal, start the RPA worker:

```powershell
.\scripts\run-rpa.ps1
```

6. In another terminal, start the dashboard:

```powershell
.\scripts\run-dashboard.ps1
```

7. Query summary metrics:

```powershell
.\scripts\metrics.ps1
```

## Narrative

1. The generator produces customer, merchant, and transaction-like events in Nexmark-style form.
2. RisingWave ingests those streams and continuously updates risk-oriented materialized views.
3. `risk_candidates` acts as the pre-filter before AI scoring.
4. The AI worker enriches each candidate with `ai_score`, `confidence`, and `decision_reason`.
5. RisingWave maps those scores into `rpa_decisions`.
6. The RPA worker dispatches each decision as a local mock action or a real OpenFlow workitem, depending on `RPA_MODE`.
7. The dashboard presents transaction counts, risk volume, action mix, latency, and dispatch status.
