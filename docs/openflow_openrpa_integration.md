# OpenFlow / OpenRPA Integration

This repository is currently aligned to the real external integration path:

- `AI_MODE=hf_api`
- `RPA_MODE=openflow_queue`

## Hugging Face

Recommended configuration:

```env
AI_MODE=hf_api
HF_TOKEN=...
HF_MODEL_ID=facebook/bart-large-mnli
HF_PROVIDER_POLICY=preferred
```

Behavior:

- The AI worker consumes `risk_candidates`.
- It builds a zero-shot classification prompt from transaction fields.
- It calls Hugging Face through `huggingface_hub.InferenceClient`.
- It writes the result into `ai_scored_events`.
- During active ingestion, `rpa_decisions` may still advance ahead of `ai_scored_events` because `ai_decision_context` falls back to rule-based risk until the real score arrives.

## OpenFlow Queue

Recommended configuration:

```env
RPA_MODE=openflow_queue
OPENFLOW_URL=grpc://grpc.app.openiap.io:443
OPENFLOW_USERNAME=...
OPENFLOW_PASSWORD=...
OPENFLOW_TOKEN=...
OPENRPA_QUEUE_NAME=risingwave_rpa_decisions
OPENFLOW_MAX_DISPATCH_RETRIES=6
OPENFLOW_BACKLOG_BATCH_SIZE=3
```

Behavior:

- The RPA worker consumes `rpa_decisions`.
- It maps each decision to a real OpenFlow workitem payload.
- It pushes that workitem to `OPENRPA_QUEUE_NAME`.
- It records local dispatch state in `workflow_dispatch_log`.
- It reuses the OpenFlow client and retries queued rows from `workflow_dispatch_log` while draining backlog.

If you only know the OpenIAP UI URL, for example `https://app.openiap.io/ui`, this repo normalizes it to the gRPC endpoint pattern automatically. The preferred configuration is still the explicit SDK endpoint, for example `grpc://grpc.app.openiap.io:443`.

Important fields written to `workflow_dispatch_log`:

- `status`
- `dispatch_mode`
- `queue_name`
- `external_workitem_id`
- `external_state`
- `payload_json`

## OpenRPA Full Execution

Set:

```env
RPA_MODE=openrpa_full
OPENRPA_ENTRY_WORKFLOW=YourMainWorkflow.xaml
```

In this repository, `openrpa_full` means:

- the dispatch payload includes the entry workflow hint
- work is still pushed through OpenFlow workitems

The desktop bot runtime itself remains a Windows-host deployment concern and is not containerized in this repo.

## Suggested OpenRPA Workflow Structure

Create a main workflow that:

1. Pops the next workitem from `OPENRPA_QUEUE_NAME`
2. Branches by `rpa_action`
3. Runs one of:
   - `BLOCK_AND_CREATE_TICKET`
   - `CREATE_TICKET_AND_NOTIFY`
   - `SEND_WARNING`
4. Updates the workitem to `Successful` or `Retry`

## Verification

Useful checks:

```powershell
docker compose logs -f ai_scorer
docker compose logs -f rpa_worker
.\scripts\metrics.ps1
```

Useful SQL:

```sql
SELECT model_version, COUNT(*) FROM ai_scored_events GROUP BY model_version;
SELECT status, COUNT(*) FROM workflow_dispatch_log GROUP BY status;
SELECT external_workitem_id, external_state FROM workflow_dispatch_log ORDER BY dispatched_at DESC LIMIT 20;
```
