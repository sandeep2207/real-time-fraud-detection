"""
Velocity Detector — PyFlink job

Detects cards with > VELOCITY_THRESHOLD transactions in a sliding window.

Window:  60-second size, 10-second slide
Output:  fraud-alerts Kafka topic
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import SlidingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction

import json
import uuid
from datetime import datetime, timezone

KAFKA_BROKERS = "kafka:29092"
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "fraud-alerts"
VELOCITY_THRESHOLD = 10          # max transactions per card per 60s window
WINDOW_SIZE_SEC = 60
WINDOW_SLIDE_SEC = 10


class VelocityAlertFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        txns = list(elements)
        count = len(txns)

        if count > VELOCITY_THRESHOLD:
            window = context.window()
            sample = txns[-1]  # most recent transaction in window

            alert = {
                "alert_id": str(uuid.uuid4()),
                "transaction_id": sample["transaction_id"],
                "card_id": key,
                "amount": sample["amount"],
                "signal_type": "velocity",
                "score": min(1.0, round((count - VELOCITY_THRESHOLD) / VELOCITY_THRESHOLD, 3)),
                "window_start": datetime.fromtimestamp(
                    window.start / 1000, tz=timezone.utc).isoformat(),
                "window_end": datetime.fromtimestamp(
                    window.end / 1000, tz=timezone.utc).isoformat(),
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "details": {
                    "transaction_count": count,
                    "threshold": VELOCITY_THRESHOLD,
                    "window_seconds": WINDOW_SIZE_SEC,
                },
            }
            yield json.dumps(alert)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30_000)  # checkpoint every 30s

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("velocity-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda event, _: int(
                datetime.fromisoformat(json.loads(event)["timestamp"]).timestamp() * 1000
            )
        )
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    stream = (
        env
        .from_source(source, watermark_strategy, "Kafka transactions")
        .map(lambda raw: json.loads(raw))
        .key_by(lambda tx: tx["card_id"])
        .window(SlidingEventTimeWindows.of(
            Time.seconds(WINDOW_SIZE_SEC),
            Time.seconds(WINDOW_SLIDE_SEC),
        ))
        .process(VelocityAlertFunction())
        .sink_to(sink)
    )

    env.execute("fraud-velocity-detector")


if __name__ == "__main__":
    main()
