CREATE SINK IF NOT EXISTS risk_candidates_sink
FROM risk_candidates
WITH (
    connector = 'kafka',
    topic = 'risk_candidates',
    properties.bootstrap.server = 'redpanda:9092',
    type = 'upsert',
    primary_key = 'transaction_id'
) FORMAT UPSERT ENCODE JSON;

CREATE SINK IF NOT EXISTS rpa_decisions_sink
FROM rpa_decisions
WITH (
    connector = 'kafka',
    topic = 'rpa_decisions',
    properties.bootstrap.server = 'redpanda:9092',
    type = 'upsert',
    primary_key = 'transaction_id'
) FORMAT UPSERT ENCODE JSON;

