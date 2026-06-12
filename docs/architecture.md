# Architecture — real-time-fraud-detection

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────┐    ┌─────────┐    ┌──────────────────────────┐   │
│  │ Producer │───▶│  Kafka  │───▶│      Flink Cluster       │   │
│  │ (Python) │    │ (6 par) │    │  ┌────────────────────┐  │   │
│  └──────────┘    └─────────┘    │  │ velocity_detector  │  │   │
│                                 │  └────────────────────┘  │   │
│                                 │  ┌────────────────────┐  │   │
│                                 │  │ amount_deviation   │  │   │
│                                 │  └────────────────────┘  │   │
│                                 └──────────┬───────────────┘   │
│                                            │ fraud-alerts topic │
│                                 ┌──────────▼───────────────┐   │
│                                 │    FastAPI Alert Sink     │   │
│                                 └──────────┬───────────────┘   │
│                                            │                   │
│                                 ┌──────────▼───────────────┐   │
│                                 │       PostgreSQL          │   │
│                                 └──────────┬───────────────┘   │
│                                            │                   │
│                                 ┌──────────▼───────────────┐   │
│                                 │        Grafana            │   │
│                                 └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Design decisions

### Why PyFlink over Flink Scala API?

The team at FairMoney is Python-first. PyFlink 1.18 supports the full DataStream API including event-time windows, watermarks, and stateful operators — there's no material performance gap for this workload (I/O-bound, not CPU-bound). Using Scala would mean a separate build toolchain and a steeper onboarding curve for ML engineers who occasionally contribute to the pipeline.

### Why sliding windows instead of tumbling windows?

A card doing 11 transactions in 60 seconds would be split across two tumbling windows (e.g., 5 in window 1, 6 in window 2) and never trigger the threshold of 10. Sliding windows with a 10-second slide step ensure no burst can hide in a boundary. The cost is higher state: each event lives in `window_size / slide = 6` windows simultaneously. At our transaction volumes this is well within Flink's RocksDB state backend capacity.

### Why Kafka with 6 partitions?

Six partitions allow up to 6 parallel Flink task slots to consume the `transactions` topic independently. Each partition is keyed by `card_id % 6` at the producer, so all transactions for a given card land in the same partition — this means the Flink `keyBy(card_id)` operator does no network shuffle for the common case.

### Why Postgres as the alert sink?

Operational simplicity: one service, easy backup, Grafana has a native Postgres datasource. For analytical queries at scale (joining alerts with transaction history, cohort analysis), the sink would be replaced with BigQuery or Redshift behind the same FastAPI interface — the Flink job is decoupled from the storage layer.

### Checkpointing strategy

Flink checkpoints every 30 seconds to a local volume (configurable to S3 for production). This means maximum replay exposure is 30 seconds of transactions in the event of a job failure. The `fraud-alerts` topic is deduplicated at the sink layer using `alert_id` as an idempotent upsert key.

## Scaling path

| Bottleneck | Solution |
|---|---|
| Kafka throughput | Increase partitions, use MSK |
| Flink processing | Add task managers, increase parallelism |
| Alert sink writes | Add connection pooling, batch inserts |
| Dashboard queries | Add materialized views in Postgres |

## Production checklist

- [ ] Replace local Kafka with AWS MSK
- [ ] Point Flink checkpoint store at S3
- [ ] Replace Postgres with RDS Multi-AZ
- [ ] Add Flink job monitoring alerts (job restarts, checkpoint failures)
- [ ] Set Kafka retention policy based on replay requirements
- [ ] Add schema registry (Confluent or AWS Glue) for transaction schema evolution
