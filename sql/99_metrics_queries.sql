SELECT COUNT(*) AS total_transactions FROM clean_transactions;

SELECT COUNT(*) AS total_risk_candidates FROM risk_candidates;

SELECT COUNT(*) AS total_ai_scored FROM ai_scored_events;

SELECT COUNT(*) AS total_dispatch_rows FROM workflow_dispatch_log;

SELECT
    rpa_action,
    COUNT(*) AS total
FROM rpa_decisions
GROUP BY rpa_action
ORDER BY total DESC, rpa_action;

SELECT
    status,
    COUNT(*) AS total
FROM workflow_dispatch_log
GROUP BY status
ORDER BY total DESC, status;

SELECT
    dispatch_mode,
    status,
    COUNT(*) AS total
FROM workflow_dispatch_log
GROUP BY dispatch_mode, status
ORDER BY dispatch_mode, status;

SELECT
    AVG(EXTRACT(EPOCH FROM decision_time) - EXTRACT(EPOCH FROM event_time)) AS avg_decision_latency_seconds
FROM rpa_decisions;

SELECT
    SUM(CASE WHEN status = 'DISPATCHED' THEN 1 ELSE 0 END)::DOUBLE PRECISION
    / NULLIF(COUNT(*), 0) AS workflow_success_rate
FROM workflow_dispatch_log;

SELECT
    AVG(EXTRACT(EPOCH FROM d.dispatched_at) - EXTRACT(EPOCH FROM d.decision_time)) AS avg_dispatch_latency_seconds
FROM workflow_dispatch_log d
WHERE d.dispatched_at IS NOT NULL
  AND d.decision_time IS NOT NULL;

SELECT
    ROUND(
        SUM(CASE WHEN dispatch_mode = 'openflow_queue' AND status = 'DISPATCHED' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN dispatch_mode = 'openflow_queue' THEN 1 ELSE 0 END), 0),
        4
    ) AS openflow_dispatch_success_rate
FROM workflow_dispatch_log;

SELECT
    model_version,
    COUNT(*) AS total
FROM ai_scored_events
GROUP BY model_version
ORDER BY total DESC, model_version;

SELECT
    user_id,
    COUNT(*) AS risky_events,
    AVG(final_risk_score) AS avg_risk_score,
    MAX(final_risk_score) AS max_risk_score
FROM rpa_decisions
GROUP BY user_id
ORDER BY risky_events DESC
LIMIT 10;
