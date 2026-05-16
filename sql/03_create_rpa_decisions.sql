CREATE MATERIALIZED VIEW IF NOT EXISTS rpa_decisions AS
SELECT
    transaction_id,
    user_id,
    merchant_id,
    merchant_name,
    amount,
    channel,
    location,
    event_time,
    rule_risk_score,
    ai_score,
    confidence,
    model_version,
    final_risk_score,
    decision_reason,
    CASE
        WHEN final_risk_score >= 0.90 THEN 'BLOCK_AND_CREATE_TICKET'
        WHEN final_risk_score >= 0.75 THEN 'CREATE_TICKET_AND_NOTIFY'
        WHEN final_risk_score >= 0.60 THEN 'SEND_WARNING'
        ELSE 'NO_ACTION'
    END AS rpa_action,
    inference_time AS decision_time
FROM ai_decision_context
WHERE final_risk_score >= 0.60;

CREATE TABLE IF NOT EXISTS workflow_dispatch_log (
    action_id VARCHAR PRIMARY KEY,
    transaction_id VARCHAR,
    rpa_action VARCHAR,
    status VARCHAR,
    retry_count INT,
    created_at TIMESTAMPTZ,
    dispatched_at TIMESTAMPTZ,
    decision_time TIMESTAMPTZ,
    processor VARCHAR,
    error_message VARCHAR,
    dispatch_mode VARCHAR,
    queue_name VARCHAR,
    entry_workflow VARCHAR,
    external_workitem_id VARCHAR,
    external_state VARCHAR,
    payload_json VARCHAR
);
