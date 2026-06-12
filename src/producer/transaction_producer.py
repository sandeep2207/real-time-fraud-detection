"""
Transaction producer — simulates card transaction events and publishes to Kafka.

Usage:
    python transaction_producer.py [--tps 100] [--cards 1000] [--duration 300]
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from schema import Transaction

KAFKA_TOPIC = "transactions"
CARDS = [f"card_{i:04d}" for i in range(1000)]
MERCHANTS = ["Amazon", "Swiggy", "Zomato", "Flipkart", "PhonePe", "Paytm", "IRCTC", "BookMyShow"]
COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE"]
CITIES = {
    "IN": ["Bangalore", "Mumbai", "Delhi", "Chennai", "Hyderabad"],
    "US": ["New York", "San Francisco", "Chicago"],
    "GB": ["London", "Manchester"],
    "SG": ["Singapore"],
    "AE": ["Dubai"],
    "DE": ["Berlin", "Munich"],
}

# Per-card spend profile: mean and stddev for transaction amounts
CARD_PROFILES: dict[str, dict] = {
    card: {
        "mean": random.uniform(500, 5000),
        "std": random.uniform(100, 800),
        "home_country": random.choice(["IN", "IN", "IN", "US", "GB"]),  # bias toward IN
    }
    for card in CARDS
}


def generate_transaction(fraud_rate: float = 0.02) -> Transaction:
    card_id = random.choice(CARDS)
    profile = CARD_PROFILES[card_id]

    is_fraud = random.random() < fraud_rate

    if is_fraud:
        fraud_type = random.choice(["velocity", "amount", "geography"])
        if fraud_type == "amount":
            amount = profile["mean"] + random.uniform(5, 10) * profile["std"]
        elif fraud_type == "geography":
            country = random.choice([c for c in COUNTRIES if c != profile["home_country"]])
        else:
            amount = abs(random.gauss(profile["mean"], profile["std"]))
    else:
        country = profile["home_country"]
        amount = max(10.0, random.gauss(profile["mean"], profile["std"]))

    if "country" not in locals():
        country = profile["home_country"]
    if "amount" not in locals():
        amount = max(10.0, random.gauss(profile["mean"], profile["std"]))

    city = random.choice(CITIES.get(country, ["Unknown"]))

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        card_id=card_id,
        amount=round(amount, 2),
        currency="INR",
        merchant=random.choice(MERCHANTS),
        country=country,
        city=city,
        timestamp=datetime.now(timezone.utc).isoformat(),
        is_fraud_label=is_fraud,
    )


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")


def run_producer(tps: int, duration_seconds: int | None):
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    interval = 1.0 / tps
    count = 0
    start = time.time()
    print(f"[INFO] Producing {tps} transactions/sec to topic '{KAFKA_TOPIC}'")

    try:
        while True:
            if duration_seconds and (time.time() - start) >= duration_seconds:
                break

            tx = generate_transaction()
            producer.send(
                KAFKA_TOPIC,
                key=tx.card_id.encode(),
                value=tx.model_dump(),
            )
            count += 1

            if count % (tps * 10) == 0:
                elapsed = time.time() - start
                print(f"[INFO] Sent {count:,} transactions in {elapsed:.1f}s "
                      f"({count / elapsed:.0f} TPS)")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[INFO] Producer stopped by user")
    finally:
        producer.flush()
        elapsed = time.time() - start
        print(f"[INFO] Total: {count:,} transactions in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud detection transaction producer")
    parser.add_argument("--tps", type=int, default=100, help="Transactions per second")
    parser.add_argument("--cards", type=int, default=1000, help="Number of unique cards")
    parser.add_argument("--duration", type=int, default=None, help="Run duration in seconds")
    args = parser.parse_args()

    run_producer(tps=args.tps, duration_seconds=args.duration)
