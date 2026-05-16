# PLAN.md — RisingWave AI/RPA Prototype Plan for Codex Agent

## 0. Purpose

Build a **small local prototype** that follows the same *data/methodology spirit* as RisingWave's official Nexmark benchmark while adding a lightweight AI and RPA layer for demonstration.

The prototype must demonstrate this pipeline:

```text
Nexmark-style stream data
        ↓
Kafka topics: person / auction / bid
        ↓
RisingWave streaming core
        ↓
Materialized views: clean data, window features, risk candidates
        ↓
AI scoring layer: external UDF or mock model service
        ↓
RPA decision stream: SQL decision view + Kafka/PostgreSQL sink
        ↓
RPA mock worker / OpenRPA-compatible workitem simulation
        ↓
Audit log + dashboard metrics
```

This prototype is **not** a replacement for the official RisingWave/Flink benchmark. It is a **demonstration prototype** showing how the benchmark-style streaming pipeline can be extended with AI scoring and RPA actions.

Use the official RisingWave benchmark as the methodology reference:

- RisingWave Nexmark benchmark docs: https://docs.risingwave.com/get-started/rw-benchmarks-stream-processing
- Official benchmark repository: https://github.com/risingwavelabs/nexmark-risingwave-1.0
- RisingWave External Python UDF docs: https://docs.risingwave.com/sql/udfs/use-udfs-in-python
- RisingWave sink delivery semantics: https://docs.risingwave.com/delivery/overview
- RisingWave Kafka sink: https://docs.risingwave.com/integrations/destinations/apache-kafka

---

## 1. High-Level Strategy

### 1.1 What must be benchmark-like

Follow the official RisingWave Nexmark benchmark in these ways:

1. Use **Nexmark-style entities**:
   - `person`
   - `auction`
   - `bid`

2. Use **Kafka as the stream ingestion layer**.

3. Use **RisingWave SQL objects**:
   - source/table for ingestion;
   - materialized views for continuous computation;
   - sink for downstream delivery.

4. Use **stream-processing metrics**:
   - input rate;
   - processed rows;
   - materialized-view freshness;
   - decision latency;
   - throughput of `risk_candidates`;
   - number of generated RPA actions.

5. Keep the streaming benchmark concept separate from AI/RPA demo:
   - Official benchmark: Nexmark query throughput.
   - Prototype demo: AI/RPA extension built on top of Nexmark-style streaming data.

### 1.2 What does not need to match the official benchmark exactly

For local demo simplicity, this prototype does **not** need to reproduce the full Kubernetes benchmark environment unless available.

Acceptable simplifications:

- Use local Docker Compose instead of Kubernetes.
- Use Redpanda instead of Apache Kafka if easier.
- Use a Python Nexmark-style generator instead of the official Rust/Java generator.
- Use a mock AI scorer or small local model instead of a heavy LLM.
- Use a Python RPA worker or n8n webhook instead of full OpenRPA installation.

However, the plan must keep the same conceptual methodology:

```text
stream generator → Kafka → RisingWave → materialized views → sink
```

---

## 2. Recommended Prototype Scope

### 2.1 Mandatory features

Codex must implement:

1. Docker Compose stack:
   - RisingWave standalone or RisingWave cluster mode if easy;
   - Kafka-compatible broker, preferably Redpanda for local simplicity;
   - Python services for data generator, AI scoring, RPA worker;
   - optional PostgreSQL/SQLite for audit log;
   - optional Streamlit dashboard.

2. Nexmark-style stream generator:
   - generate `person`, `auction`, `bid` events;
   - publish to Kafka topics:
     - `nexmark_persons`
     - `nexmark_auctions`
     - `nexmark_bids`

3. RisingWave SQL setup:
   - create tables/sources for the three topics;
   - create materialized views for transaction/fraud-style analysis;
   - create AI-ready view `risk_candidates`;
   - create AI scoring output table or view;
   - create RPA decision view `rpa_decisions`;
   - create sink to Kafka topic `rpa_decisions`.

4. AI layer:
   - Option A: External Python UDF service called from RisingWave SQL.
   - Option B: Python worker consumes `risk_candidates` sink and writes `ai_scored_events`.
   - For demo reliability, prefer **Option B first**, then document Option A as upgrade.

5. RPA layer:
   - Python worker consumes `rpa_decisions`;
   - simulates RPA actions:
     - `BLOCK_AND_CREATE_TICKET`
     - `CREATE_TICKET_AND_NOTIFY`
     - `SEND_WARNING`
   - writes audit events to `workflow_audit_log`.

6. Metrics:
   - number of generated events;
   - number of risk candidates;
   - number of AI-scored events;
   - number of RPA decisions by action type;
   - average decision latency;
   - workflow success rate.

7. README with:
   - setup commands;
   - run commands;
   - SQL explanation;
   - demo script;
   - screenshots instructions.

### 2.2 Optional features

Codex may implement if time permits:

1. Streamlit dashboard:
   - event counts;
   - risk candidates;
   - RPA actions;
   - latency chart;
   - workflow status.

2. Official benchmark replication track:
   - clone `risingwavelabs/nexmark-risingwave-1.0`;
   - document how to run official benchmark on Kubernetes;
   - keep it separate from local AI/RPA demo.

3. n8n webhook integration:
   - instead of Python RPA worker, send decisions to n8n webhook;
   - n8n creates ticket payload and callback.

4. OpenRPA-compatible workitem export:
   - write `rpa_decisions` as JSON files or HTTP payloads compatible with OpenRPA/OpenFlow workitem queue concepts.

---

## 3. Repository Structure

Create a repository with this structure:

```text
risingwave-ai-rpa-prototype/
├── README.md
├── docker-compose.yml
├── .env.example
├── Makefile
├── sql/
│   ├── 00_create_tables.sql
│   ├── 01_create_streaming_views.sql
│   ├── 02_create_ai_tables.sql
│   ├── 03_create_rpa_decisions.sql
│   ├── 04_create_sinks.sql
│   └── 99_metrics_queries.sql
├── services/
│   ├── generator/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── nexmark_generator.py
│   ├── ai_scorer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── ai_scorer_worker.py
│   ├── rpa_worker/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── rpa_worker.py
│   └── dashboard/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── streamlit_app.py
├── data/
│   └── sample_events/
├── docs/
│   ├── architecture.md
│   ├── methodology.md
│   ├── demo_script.md
│   └── results_template.md
└── screenshots/
```

---

## 4. Docker Compose Design

Use Docker Compose for local reproducibility.

### 4.1 Services

Minimum services:

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp
      - "1"
      - --memory
      - "1G"
      - --reserve-memory
      - "0M"
      - --node-id
      - "0"
      - --check=false
      - --kafka-addr
      - "PLAINTEXT://0.0.0.0:9092"
      - --advertise-kafka-addr
      - "PLAINTEXT://redpanda:9092"
    ports:
      - "9092:9092"
      - "9644:9644"

  risingwave:
    image: risingwavelabs/risingwave:latest
    command: "standalone"
    ports:
      - "4566:4566"
      - "5691:5691"
    depends_on:
      - redpanda

  generator:
    build: ./services/generator
    depends_on:
      - redpanda
    environment:
      KAFKA_BOOTSTRAP_SERVERS: redpanda:9092
      EVENTS_PER_SECOND: 100
      RUN_SECONDS: 300

  ai_scorer:
    build: ./services/ai_scorer
    depends_on:
      - redpanda
      - risingwave
    environment:
      KAFKA_BOOTSTRAP_SERVERS: redpanda:9092
      RISINGWAVE_DSN: postgresql://root@risingwave:4566/dev

  rpa_worker:
    build: ./services/rpa_worker
    depends_on:
      - redpanda
      - risingwave
    environment:
      KAFKA_BOOTSTRAP_SERVERS: redpanda:9092
      RISINGWAVE_DSN: postgresql://root@risingwave:4566/dev

  dashboard:
    build: ./services/dashboard
    ports:
      - "8501:8501"
    depends_on:
      - risingwave
```

Notes for Codex:

- If `risingwavelabs/risingwave:latest` fails, pin a stable version.
- If Redpanda command changes, adjust based on current image docs.
- RisingWave SQL port is usually `4566`.

---

## 5. Kafka Topics

Create or rely on auto-creation for these topics:

```text
nexmark_persons
nexmark_auctions
nexmark_bids
risk_candidates
ai_scored_events
rpa_decisions
workflow_audit_events
```

If Redpanda auto-creation is disabled, add a setup script:

```bash
rpk topic create nexmark_persons
rpk topic create nexmark_auctions
rpk topic create nexmark_bids
rpk topic create risk_candidates
rpk topic create ai_scored_events
rpk topic create rpa_decisions
rpk topic create workflow_audit_events
```

---

## 6. Data Generator

### 6.1 Purpose

Generate Nexmark-style data:

- `Person`: user/customer profile;
- `Auction`: merchant/product/context;
- `Bid`: transaction-like event.

### 6.2 Event Schemas

#### Person event

```json
{
  "id": 101,
  "name": "user_101",
  "email": "user101@example.com",
  "city": "Hanoi",
  "state": "HN",
  "date_time": "2026-05-15T10:00:00Z",
  "extra": "{}"
}
```

#### Auction event

```json
{
  "id": 5001,
  "item_name": "merchant_5001",
  "description": "electronics merchant",
  "initial_bid": 100,
  "reserve": 500,
  "date_time": "2026-05-15T10:00:02Z",
  "expires": "2026-05-15T11:00:02Z",
  "seller": 101,
  "category": 3,
  "extra": "{}"
}
```

#### Bid event

```json
{
  "auction": 5001,
  "bidder": 101,
  "price": 1200,
  "channel": "mobile",
  "url": "https://example.com/txn/abc",
  "date_time": "2026-05-15T10:00:05Z",
  "extra": "{\"location\":\"Hanoi\",\"message\":\"urgent purchase\"}"
}
```

### 6.3 Mapping to Fraud/Risk Use Case

Use Nexmark entities as follows:

| Nexmark entity | Fraud/risk meaning |
|---|---|
| `Person` | customer/user |
| `Auction` | merchant/product/context |
| `Bid` | transaction event |

### 6.4 Generator behavior

Codex should implement:

- configurable events/sec;
- configurable run duration;
- fraud/risk patterns:
  - high bid price;
  - repeated bids by same user in short time;
  - unusual channel;
  - urgent text in `extra.message`;
  - location mismatch if implemented.

Pseudo-logic:

```python
while running:
    maybe_emit_person()
    maybe_emit_auction()
    emit_bid()
    sleep(1 / events_per_second)
```

---

## 7. RisingWave SQL

### 7.1 `00_create_tables.sql`

Create Kafka-backed tables.

```sql
CREATE TABLE IF NOT EXISTS bench_persons (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMPTZ,
    extra JSONB
) WITH (
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
    extra JSONB
) WITH (
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
    extra JSONB
) WITH (
    connector = 'kafka',
    topic = 'nexmark_bids',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

Adjust syntax if the RisingWave version requires `properties.bootstrap.server` or `properties.bootstrap.servers`.

### 7.2 `01_create_streaming_views.sql`

Create cleaned transaction view.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_transactions AS
SELECT
    md5(CONCAT(auction::VARCHAR, '-', bidder::VARCHAR, '-', date_time::VARCHAR)) AS transaction_id,
    bidder AS user_id,
    auction AS merchant_id,
    price::DOUBLE PRECISION AS amount,
    channel,
    url,
    date_time AS event_time,
    COALESCE(extra->>'location', 'unknown') AS location,
    COALESCE(extra->>'message', '') AS message,
    CASE WHEN price >= 1000 THEN 1 ELSE 0 END AS high_amount_flag
FROM bench_bids
WHERE bidder IS NOT NULL
  AND auction IS NOT NULL
  AND date_time IS NOT NULL;
```

Create user-window features.

```sql
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
```

Create merchant/user enrichment view.

```sql
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
    c.high_amount_flag
FROM clean_transactions c
LEFT JOIN bench_persons p
    ON c.user_id = p.id
LEFT JOIN bench_auctions a
    ON c.merchant_id = a.id;
```

Create risk candidates.

```sql
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
    f.txn_count,
    f.total_amount,
    f.avg_amount,
    f.max_amount,
    CASE
        WHEN e.high_amount_flag = 1 AND f.txn_count >= 5 THEN 0.90
        WHEN e.high_amount_flag = 1 THEN 0.75
        WHEN f.txn_count >= 10 THEN 0.70
        WHEN e.channel = 'unknown' THEN 0.60
        ELSE 0.20
    END AS rule_risk_score
FROM enriched_transactions e
JOIN user_10min_features f
  ON e.user_id = f.user_id
 AND e.event_time >= f.window_start
 AND e.event_time < f.window_end
WHERE e.high_amount_flag = 1
   OR f.txn_count >= 5
   OR e.channel = 'unknown';
```

### 7.3 `02_create_ai_tables.sql`

For a robust prototype, avoid making RisingWave call the model directly at first. Use an AI worker that reads `risk_candidates` and writes scores back.

Create AI score table:

```sql
CREATE TABLE IF NOT EXISTS ai_scored_events (
    transaction_id VARCHAR PRIMARY KEY,
    user_id BIGINT,
    ai_score DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    model_version VARCHAR,
    decision_reason VARCHAR,
    inference_time TIMESTAMPTZ
);
```

Create AI decision context:

```sql
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
    COALESCE(a.inference_time, NOW()) AS inference_time
FROM risk_candidates r
LEFT JOIN ai_scored_events a
    ON r.transaction_id = a.transaction_id;
```

### 7.4 `03_create_rpa_decisions.sql`

```sql
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
    NOW() AS decision_time
FROM ai_decision_context
WHERE final_risk_score >= 0.60;
```

Create audit log table:

```sql
CREATE TABLE IF NOT EXISTS workflow_audit_log (
    action_id VARCHAR PRIMARY KEY,
    transaction_id VARCHAR,
    rpa_action VARCHAR,
    status VARCHAR,
    retry_count INT,
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    processor VARCHAR,
    error_message VARCHAR
);
```

### 7.5 `04_create_sinks.sql`

Create Kafka sink for RPA decisions:

```sql
CREATE SINK IF NOT EXISTS rpa_decisions_sink
FROM rpa_decisions
WITH (
    connector = 'kafka',
    topic = 'rpa_decisions',
    properties.bootstrap.server = 'redpanda:9092',
    type = 'upsert',
    primary_key = 'transaction_id'
) FORMAT UPSERT ENCODE JSON;
```

Optional: create sink for risk candidates so the AI worker can consume from Kafka instead of polling RisingWave.

```sql
CREATE SINK IF NOT EXISTS risk_candidates_sink
FROM risk_candidates
WITH (
    connector = 'kafka',
    topic = 'risk_candidates',
    properties.bootstrap.server = 'redpanda:9092',
    type = 'upsert',
    primary_key = 'transaction_id'
) FORMAT UPSERT ENCODE JSON;
```

---

## 8. AI Scorer Worker

### 8.1 Recommended implementation

Use a Python worker. For demo reliability, implement a deterministic scorer first.

Input:

- consume Kafka topic `risk_candidates`, or poll RisingWave view `risk_candidates`.

Output:

- write rows into RisingWave table `ai_scored_events`.

### 8.2 Scoring logic

Use lightweight rule/model hybrid:

```python
score = rule_risk_score

if amount >= 5000:
    score = max(score, 0.95)

if txn_count >= 8:
    score = max(score, 0.85)

if "urgent" in message.lower() or "suspicious" in message.lower():
    score = max(score, 0.80)

if channel == "unknown":
    score = max(score, 0.70)

confidence = min(0.99, 0.60 + score * 0.35)
```

Reason examples:

- `high_amount_and_repeated_transactions`
- `large_amount`
- `high_frequency_user`
- `suspicious_text`
- `unknown_channel`

### 8.3 Why deterministic scorer is acceptable

For the prototype:

- It demonstrates the integration flow.
- It avoids heavy model setup.
- It remains reproducible for presentation.
- It does not claim AI benchmark performance.

Document clearly:

> This mock AI scorer is used to demonstrate the AI integration layer. It is not used to claim model accuracy. Streaming performance is evaluated separately using Nexmark benchmark references.

### 8.4 Optional Hugging Face mode

If time allows, add `AI_MODE=hf`:

- Use Hugging Face `pipeline("text-classification")` for message classification.
- Keep default `AI_MODE=mock` for reproducibility.

---

## 9. RPA Worker

### 9.1 Purpose

Consume `rpa_decisions` and simulate RPA automation.

### 9.2 Input

Kafka topic:

```text
rpa_decisions
```

### 9.3 Action mapping

| `rpa_action` | Simulated action |
|---|---|
| `BLOCK_AND_CREATE_TICKET` | create high-priority fraud ticket and mark transaction blocked |
| `CREATE_TICKET_AND_NOTIFY` | create review ticket and send notification |
| `SEND_WARNING` | create warning log and notify user/customer service |
| `NO_ACTION` | ignore |

### 9.4 Idempotency

Use:

```text
action_id = sha256(transaction_id + rpa_action)
```

Before processing, check `workflow_audit_log`.

If action exists with `SUCCEEDED`, skip.

### 9.5 Audit log status

Use these states:

```text
QUEUED
RUNNING
SUCCEEDED
FAILED
RETRYING
HUMAN_REVIEW
```

### 9.6 SQL insert/update examples

Insert queued action:

```sql
INSERT INTO workflow_audit_log (
    action_id,
    transaction_id,
    rpa_action,
    status,
    retry_count,
    created_at,
    processor,
    error_message
) VALUES (
    $1, $2, $3, 'QUEUED', 0, NOW(), 'rpa_worker', NULL
)
ON CONFLICT (action_id) DO NOTHING;
```

Update success:

```sql
UPDATE workflow_audit_log
SET status = 'SUCCEEDED',
    processed_at = NOW(),
    error_message = NULL
WHERE action_id = $1;
```

If RisingWave version does not support `ON CONFLICT`, use a read-before-write idempotency check in Python.

---

## 10. Metrics Queries

Create `sql/99_metrics_queries.sql`.

### 10.1 Counts

```sql
SELECT COUNT(*) AS total_transactions FROM clean_transactions;

SELECT COUNT(*) AS total_risk_candidates FROM risk_candidates;

SELECT COUNT(*) AS total_ai_scored FROM ai_scored_events;

SELECT rpa_action, COUNT(*) AS total
FROM rpa_decisions
GROUP BY rpa_action;

SELECT status, COUNT(*) AS total
FROM workflow_audit_log
GROUP BY status;
```

### 10.2 Average decision latency

```sql
SELECT
    AVG(EXTRACT(EPOCH FROM (decision_time - event_time))) AS avg_decision_latency_seconds
FROM rpa_decisions;
```

### 10.3 Workflow success rate

```sql
SELECT
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END)::DOUBLE PRECISION
    / NULLIF(COUNT(*), 0) AS workflow_success_rate
FROM workflow_audit_log;
```

### 10.4 Top risky users

```sql
SELECT
    user_id,
    COUNT(*) AS risky_events,
    AVG(final_risk_score) AS avg_risk_score,
    MAX(final_risk_score) AS max_risk_score
FROM rpa_decisions
GROUP BY user_id
ORDER BY risky_events DESC
LIMIT 10;
```

---

## 11. Streamlit Dashboard

Optional but recommended.

Dashboard pages:

1. Overview:
   - total transactions;
   - risk candidates;
   - AI-scored events;
   - RPA actions;
   - workflow success rate.

2. RPA actions:
   - bar chart by `rpa_action`;
   - table of latest decisions.

3. Latency:
   - average decision latency;
   - latest high-latency events.

4. Audit log:
   - workflow status count;
   - latest failures.

Use `psycopg2` or `sqlalchemy` to query RisingWave.

---

## 12. Makefile Commands

Create a Makefile:

```makefile
up:
	docker compose up -d redpanda risingwave

build:
	docker compose build

init-sql:
	psql postgresql://root@localhost:4566/dev -f sql/00_create_tables.sql
	psql postgresql://root@localhost:4566/dev -f sql/01_create_streaming_views.sql
	psql postgresql://root@localhost:4566/dev -f sql/02_create_ai_tables.sql
	psql postgresql://root@localhost:4566/dev -f sql/03_create_rpa_decisions.sql
	psql postgresql://root@localhost:4566/dev -f sql/04_create_sinks.sql

run-generator:
	docker compose up generator

run-ai:
	docker compose up ai_scorer

run-rpa:
	docker compose up rpa_worker

dashboard:
	docker compose up dashboard

metrics:
	psql postgresql://root@localhost:4566/dev -f sql/99_metrics_queries.sql

down:
	docker compose down -v
```

---

## 13. Demo Script

Codex must create `docs/demo_script.md`.

### 13.1 Demo steps

1. Start services:

```bash
make up
```

2. Initialize SQL:

```bash
make init-sql
```

3. Start generator:

```bash
make run-generator
```

4. Start AI scorer:

```bash
make run-ai
```

5. Start RPA worker:

```bash
make run-rpa
```

6. Run metrics:

```bash
make metrics
```

7. Optional dashboard:

```bash
make dashboard
```

### 13.2 Demo narrative

Use this narrative in presentation:

1. Nexmark-style data simulates real-time customer/merchant/transaction streams.
2. RisingWave ingests Kafka topics and maintains materialized views.
3. `risk_candidates` filters suspicious transactions before AI scoring.
4. AI worker enriches candidates with `ai_score` and `decision_reason`.
5. `rpa_decisions` maps risk into workflow actions.
6. RPA worker processes actions and writes audit logs.
7. Dashboard shows counts, actions, latency and workflow success.

---

## 14. Results Template

Create `docs/results_template.md`.

Use this table after running the demo:

| Metric | Result | Notes |
|---|---:|---|
| Generated persons | TODO | from generator logs/topic count |
| Generated auctions | TODO | from generator logs/topic count |
| Generated bids/transactions | TODO | from RisingWave `clean_transactions` |
| Risk candidates | TODO | from `risk_candidates` |
| AI-scored events | TODO | from `ai_scored_events` |
| RPA decisions | TODO | from `rpa_decisions` |
| Workflow success rate | TODO | from `workflow_audit_log` |
| Average decision latency | TODO | `decision_time - event_time` |

Important note:

> These prototype results demonstrate integration feasibility only. They are not used as a formal performance comparison. Formal streaming performance evidence is based on published RisingWave Nexmark benchmark results.

---

## 15. Official Benchmark Track

Create `docs/official_benchmark_track.md`.

### 15.1 Purpose

Document how to reproduce official benchmark methodology separately.

### 15.2 Steps

```bash
git clone https://github.com/risingwavelabs/nexmark-risingwave-1.0.git
cd nexmark-risingwave-1.0
```

Follow the repository README.

Expected prerequisites:

- Kubernetes >= 1.21
- Helm >= 3.0
- Bash >= 4.4
- RisingWave Operator >= 0.5.0

### 15.3 How to cite in report

Use wording:

> The prototype follows the Nexmark-style stream ingestion methodology, while formal performance comparison is based on the official RisingWave Nexmark benchmark and its public benchmark repository.

Do not claim the local demo reproduces official benchmark numbers unless it actually uses the official repository and matching hardware.

---

## 16. Acceptance Criteria

Codex should consider the task complete when:

1. `docker compose up` starts RisingWave and Kafka-compatible broker.
2. SQL scripts create all tables, materialized views and sinks.
3. Generator publishes Nexmark-style events.
4. RisingWave views populate:
   - `clean_transactions`
   - `user_10min_features`
   - `risk_candidates`
5. AI scorer writes rows to `ai_scored_events`.
6. `ai_decision_context` and `rpa_decisions` produce rows.
7. RPA worker processes decisions and writes `workflow_audit_log`.
8. `make metrics` returns meaningful numbers.
9. README explains setup and demo flow.
10. Docs clearly state:
    - prototype is an integration demo;
    - official benchmark evidence is separate;
    - AI/RPA performance is not benchmarked formally.

---

## 17. Important Report Wording

Use this exact wording in the final report/prototype explanation:

> The prototype is designed to demonstrate the feasibility of integrating AI scoring and RPA automation on top of a RisingWave-based streaming core. It follows the Nexmark-style methodology of streaming data generation, Kafka ingestion, RisingWave materialized views and downstream sinks. However, it is not a formal end-to-end benchmark. Formal streaming performance evidence is taken from RisingWave's published Nexmark benchmark, while the prototype illustrates the integration flow from stream ingestion to AI-assisted RPA decisioning.

---

## 18. Risk and Mitigation

| Risk | Mitigation |
|---|---|
| RisingWave SQL syntax mismatch | Pin RisingWave version; test SQL scripts incrementally |
| Kafka/Redpanda connectivity issues | Add health checks and retry loops |
| External Python UDF is hard to set up | Use AI worker mode first; document UDF mode as optional |
| Sink duplicate messages | Use upsert sink and idempotency key |
| Demo too heavy for local laptop | Lower events/sec to 10–50 |
| Streamlit not ready | Keep metrics SQL as required output; dashboard optional |
| Official benchmark hard to run | Keep it as documented optional track, not required for demo |

---

## 19. Minimal Implementation Order for Codex

Implement in this order:

1. Create repository skeleton.
2. Create Docker Compose with Redpanda + RisingWave.
3. Create SQL scripts.
4. Implement generator.
5. Test tables and materialized views.
6. Implement AI scorer worker.
7. Implement RPA worker.
8. Implement metrics queries.
9. Add dashboard if time remains.
10. Write README and docs.

Do not start with dashboard or Hugging Face. The first working version must prove the core pipeline.

---

## 20. Final Deliverables

Codex must produce:

1. Working repository.
2. `README.md`.
3. `docker-compose.yml`.
4. SQL scripts.
5. Python generator.
6. Python AI scorer worker.
7. Python RPA worker.
8. Metrics queries.
9. `docs/methodology.md`.
10. `docs/demo_script.md`.
11. `docs/results_template.md`.
12. Optional Streamlit dashboard.
