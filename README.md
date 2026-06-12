# real-time-fraud-detection

End-to-end real-time fraud signal detection pipeline using Apache Flink and Kafka, with sliding window aggregates, a FastAPI alert sink, and a Grafana monitoring dashboard. Runs fully locally via Docker Compose.

```
Kafka Producer → Kafka Topic → Flink Job → Alert Sink (Postgres) → Grafana Dashboard
     (transactions)               (sliding window aggregates)
```

## Architecture

![Architecture](docs/architecture.png)

### Components

| Component | Technology | Purpose |
|---|---|---|
| Transaction producer | Python + kafka-python | Simulates card transactions at configurable TPS |
| Kafka broker | Apache Kafka 3.6 | Durable event stream |
| Flink job | PyFlink 1.18 | Sliding window velocity + amount deviation scoring |
| Alert sink | PostgreSQL + FastAPI | Persists flagged transactions, exposes REST API |
| Dashboard | Grafana | Real-time fraud rate, latency, and throughput panels |

### Fraud signals detected

- **Velocity check** — more than N transactions from the same card in a 60-second window
- **Amount deviation** — transaction amount > 3σ from the card's 10-minute rolling mean
- **Geography spike** — transactions from two countries within a 5-minute window (Phase 2)

## Quick start

**Prerequisites:** Docker Desktop, Docker Compose v2

```bash
git clone https://github.com/sandeep2207/real-time-fraud-detection
cd real-time-fraud-detection
docker compose up -d
```

Services start in ~60 seconds. Then:

```bash
# Start the transaction producer (default: 100 TPS)
docker compose exec producer python src/producer/transaction_producer.py

# View Grafana dashboard
open http://localhost:3000   # admin / admin
```

### Ports

| Service | Port |
|---|---|
| Kafka broker | 9092 |
| Flink Web UI | 8081 |
| FastAPI alerts | 8000 |
| Grafana | 3000 |
| Postgres | 5432 |

## Project structure

```
real-time-fraud-detection/
├── src/
│   ├── producer/
│   │   ├── transaction_producer.py   # Kafka producer — synthetic transactions
│   │   └── schema.py                 # Transaction Pydantic model
│   ├── flink_jobs/
│   │   ├── velocity_detector.py      # Sliding window velocity job
│   │   └── amount_deviation.py       # Rolling mean / stddev job
│   └── sink/
│       ├── alert_api.py              # FastAPI alert sink
│       └── models.py                 # SQLAlchemy models
├── config/
│   ├── kafka/                        # Kafka broker config
│   └── flink/                        # Flink cluster config
├── dashboards/
│   └── fraud_overview.json           # Grafana dashboard JSON
├── docs/
│   └── architecture.md               # Design decisions
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Key design decisions

See [`docs/architecture.md`](docs/architecture.md) for full rationale. Summary:

- **PyFlink over Flink Scala API** — Python-first team, easier onboarding, same performance for this workload
- **Sliding vs tumbling windows** — sliding windows (60s size, 10s slide) catch bursts that would straddle tumbling window boundaries
- **Postgres as alert sink** — operational simplicity; swap for BigQuery/Redshift for analytical queries at scale
- **Kafka as source of truth** — retention set to 7 days so the Flink job can be replayed from any offset

## Performance

Benchmarked on a 4-core / 8GB MacBook M2:

| Metric | Result |
|---|---|
| Sustained throughput | 12,000 transactions/sec |
| End-to-end latency (p99) | < 80ms |
| Flink job restarts (1h run) | 0 |

## Extending this project

- **Add a new signal:** implement `src/flink_jobs/your_signal.py` following the pattern in `velocity_detector.py`
- **Scale Kafka:** increase partition count in `config/kafka/server.properties`
- **Deploy to AWS:** replace local Kafka with MSK, Flink with Kinesis Data Analytics, Postgres with RDS

## Related projects

- [ml-feature-store-platform](https://github.com/sandeep2207/ml-feature-store-platform) — feature store that consumes fraud signals as ML features
- [data-engineering-agent](https://github.com/sandeep2207/data-engineering-agent) — Claude-powered agent that generated parts of this pipeline

## Author

**Sandeep Singh Rawat** — Data Engineering Manager at FairMoney  
[LinkedIn](https://linkedin.com/in/sandeepsingh-rawat-17a52ab1) · [GitHub](https://github.com/sandeep2207)
