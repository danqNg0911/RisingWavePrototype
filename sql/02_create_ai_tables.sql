CREATE TABLE IF NOT EXISTS ai_scored_events (
    transaction_id VARCHAR PRIMARY KEY,
    user_id BIGINT,
    ai_score DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    model_version VARCHAR,
    decision_reason VARCHAR,
    inference_time TIMESTAMPTZ
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ai_decision_context AS
SELECT
    r.transaction_id,
    r.user_id,
    r.city,
    r.state,
    r.merchant_id,
    r.merchant_name,
    r.category,
    r.amount,
    r.channel,
    r.location,
    r.message,
    r.event_time,
    r.window_start,
    r.window_end,
    r.txn_count,
    r.total_amount,
    r.avg_amount,
    r.max_amount,
    r.rule_risk_score,
    COALESCE(a.ai_score, r.rule_risk_score) AS ai_score,
    COALESCE(a.confidence, 0.50) AS confidence,
    COALESCE(a.model_version, 'rule_only') AS model_version,
    GREATEST(r.rule_risk_score, COALESCE(a.ai_score, 0.0)) AS final_risk_score,
    COALESCE(a.decision_reason, 'Rule-based candidate; AI score pending or unavailable') AS decision_reason,
    COALESCE(a.inference_time, r.event_time) AS inference_time
FROM risk_candidates r
LEFT JOIN ai_scored_events a
    ON r.transaction_id = a.transaction_id;

