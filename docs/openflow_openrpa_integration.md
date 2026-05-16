# OpenFlow / OpenRPA Integration

This repository now supports two non-mock upgrade paths for the AI/RPA extension layer:

- `AI_MODE=hf_api` for Hugging Face-hosted inference
- `RPA_MODE=openflow_queue` or `RPA_MODE=openrpa_full` for OpenFlow/OpenRPA-backed dispatch

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
- If the API call fails, it falls back to rule scoring and records a fallback model version.

## OpenFlow Queue

Recommended configuration:

```env
RPA_MODE=openflow_queue
OPENFLOW_URL=grpc://your-openflow-host:443
OPENFLOW_USERNAME=...
OPENFLOW_PASSWORD=...
OPENRPA_QUEUE_NAME=risingwave_rpa_decisions
OPENFLOW_MAX_DISPATCH_RETRIES=6
OPENFLOW_BACKLOG_BATCH_SIZE=3
```

Alternative auth:

```env
OPENFLOW_TOKEN=...
```

Behavior:

- The RPA worker consumes `rpa_decisions`.
- It maps each decision to a real OpenFlow workitem payload.
- It pushes that workitem to `OPENRPA_QUEUE_NAME`.
- It records local dispatch state in `workflow_dispatch_log`.
- It reuses the OpenFlow client and retries queued or failed dispatch rows from `workflow_dispatch_log` when idle.

If you only know the OpenIAP UI URL, for example `https://app.openiap.io/ui`, this repo now normalizes it to the gRPC endpoint pattern automatically. The preferred configuration is still the explicit SDK endpoint, for example `grpc://grpc.app.openiap.io:443`.

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

In this repository, `openrpa_full` currently means:

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
SELECT status, COUNT(*) FROM workflow_dispatch_log GROUP BY status;
SELECT dispatch_mode, COUNT(*) FROM workflow_dispatch_log GROUP BY dispatch_mode;
SELECT external_workitem_id, external_state FROM workflow_dispatch_log ORDER BY dispatched_at DESC LIMIT 20;
```
