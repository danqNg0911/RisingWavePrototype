# Methodology

This prototype follows the dataflow spirit of the official RisingWave Nexmark benchmark:

1. Generate Nexmark-style stream entities: `person`, `auction`, `bid`.
2. Publish them to Kafka-compatible topics.
3. Ingest them into RisingWave using connector-backed tables.
4. Build materialized views for transaction cleaning, feature generation, and risk candidate selection.
5. Extend the stream-processing core with an asynchronous AI scoring worker.
6. Convert final risk scores into RPA actions and dispatch downstream workitems.

## What matches the benchmark methodology

- Kafka-based streaming ingestion.
- RisingWave SQL objects for ingestion, transformation, and sinks.
- Continuous materialized-view computation over streaming inputs.
- Metrics around throughput, risk candidate volume, decision latency, and action counts.

## What is intentionally different

- Local Docker Compose rather than Kubernetes benchmark infrastructure.
- Deterministic AI scoring rather than a heavy ML model.
- Python bridge-based OpenFlow/OpenRPA dispatch, with desktop execution remaining a host-side concern.
- Demo-oriented metrics rather than formal benchmark claims.

## Reporting guidance

Use the prototype to show integration feasibility, not benchmark superiority. Formal benchmark claims should cite RisingWave's official Nexmark benchmark documentation and repository separately.
