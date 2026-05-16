CREATE MATERIALIZED VIEW IF NOT EXISTS clean_transactions AS
SELECT
    md5(CONCAT(auction::VARCHAR, '-', bidder::VARCHAR, '-', date_time::VARCHAR, '-', price::VARCHAR)) AS transaction_id,
    bidder AS user_id,
    auction AS merchant_id,
    price::DOUBLE PRECISION AS amount,
    channel,
    url,
    date_time AS event_time,
    COALESCE(extra->>'location', 'unknown') AS location,
    COALESCE(extra->>'message', '') AS message,
    CASE WHEN price >= 1000 THEN 1 ELSE 0 END AS high_amount_flag,
    CASE WHEN COALESCE(channel, 'unknown') = 'unknown' THEN 1 ELSE 0 END AS unknown_channel_flag,
    CASE WHEN LOWER(COALESCE(extra->>'message', '')) LIKE '%urgent%' THEN 1 ELSE 0 END AS urgent_message_flag
FROM bench_bids
WHERE bidder IS NOT NULL
  AND auction IS NOT NULL
  AND date_time IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_10min_features AS
SELECT
    user_id,
    window_start,
    window_end,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount
FROM TUMBLE(clean_transactions, event_time, INTERVAL '10 minutes')
GROUP BY user_id, window_start, window_end;

CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_transactions AS
SELECT
    c.transaction_id,
    c.user_id,
    p.city,
    p.state,
    c.merchant_id,
    a.item_name AS merchant_name,
    a.category,
    c.amount,
    c.channel,
    c.location,
    c.message,
    c.event_time,
    c.high_amount_flag,
    c.unknown_channel_flag,
    c.urgent_message_flag
FROM clean_transactions c
LEFT JOIN bench_persons p
    ON c.user_id = p.id
LEFT JOIN bench_auctions a
    ON c.merchant_id = a.id;

CREATE MATERIALIZED VIEW IF NOT EXISTS risk_candidates AS
SELECT
    e.transaction_id,
    e.user_id,
    e.city,
    e.state,
    e.merchant_id,
    e.merchant_name,
    e.category,
    e.amount,
    e.channel,
    e.location,
    e.message,
    e.event_time,
    f.window_start,
    f.window_end,
    f.txn_count,
    f.total_amount,
    f.avg_amount,
    f.max_amount,
    CASE
        WHEN e.high_amount_flag = 1 AND f.txn_count >= 5 THEN 0.90
        WHEN e.high_amount_flag = 1 AND e.urgent_message_flag = 1 THEN 0.85
        WHEN e.high_amount_flag = 1 THEN 0.75
        WHEN f.txn_count >= 10 THEN 0.70
        WHEN e.unknown_channel_flag = 1 THEN 0.60
        ELSE 0.20
    END AS rule_risk_score
FROM enriched_transactions e
JOIN user_10min_features f
  ON e.user_id = f.user_id
 AND e.event_time >= f.window_start
 AND e.event_time < f.window_end
WHERE e.high_amount_flag = 1
   OR f.txn_count >= 5
   OR e.unknown_channel_flag = 1
   OR e.urgent_message_flag = 1;

