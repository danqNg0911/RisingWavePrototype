CREATE TABLE IF NOT EXISTS bench_persons (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMPTZ,
    extra JSONB,
    WATERMARK FOR date_time AS date_time - INTERVAL '30 seconds'
) APPEND ONLY
WITH (
    connector = 'kafka',
    topic = 'nexmark_persons',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE IF NOT EXISTS bench_auctions (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    date_time TIMESTAMPTZ,
    expires TIMESTAMPTZ,
    seller BIGINT,
    category BIGINT,
    extra JSONB,
    WATERMARK FOR date_time AS date_time - INTERVAL '30 seconds'
) APPEND ONLY
WITH (
    connector = 'kafka',
    topic = 'nexmark_auctions',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE IF NOT EXISTS bench_bids (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time TIMESTAMPTZ,
    extra JSONB,
    WATERMARK FOR date_time AS date_time - INTERVAL '30 seconds'
) APPEND ONLY
WITH (
    connector = 'kafka',
    topic = 'nexmark_bids',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
