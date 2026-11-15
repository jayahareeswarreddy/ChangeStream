"""Apply Debezium PostgreSQL events to an idempotent read model."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition
from psycopg.types.json import Jsonb

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("changestream.consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cdc.public.orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "changestream-readmodel")
DATABASE_URL = os.getenv(
    "SINK_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/read_model"
)
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "5"))
running = True


def stop(_signum: int, _frame: Any) -> None:
    global running
    running = False


def commit(consumer: Consumer, message: Any) -> None:
    consumer.commit(message=message, asynchronous=False)


def apply_event(message: Any) -> None:
    raw = message.value()
    if raw is None:  # Debezium emits a null tombstone after a delete event.
        return

    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Change event must be a JSON object")
    operation = event.get("op")
    row = event.get("before") if operation == "d" else event.get("after")
    if operation not in {"c", "u", "d", "r"}:
        raise ValueError(f"Unsupported Debezium operation: {operation!r}")
    if not isinstance(row, dict) or "id" not in row:
        raise ValueError("Change event does not contain an order row with an id")

    source_ts_ms = (event.get("source") or {}).get("ts_ms")
    topic, partition, offset = message.topic(), message.partition(), message.offset()
    order_id = int(row["id"])

    # Commit the projection and dedupe marker together. If the process stops
    # after this transaction but before Kafka offset commit, replay is harmless.
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processed_events
                    (topic, partition_id, offset_id, operation, order_id, source_ts_ms, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (topic, partition_id, offset_id) DO NOTHING
                RETURNING id
                """,
                (topic, partition, offset, operation, order_id, source_ts_ms, Jsonb(event)),
            )
            inserted = cursor.fetchone() is not None
            if not inserted:
                return

            if operation == "d":
                cursor.execute("DELETE FROM orders_read_model WHERE id = %s", (order_id,))
            else:
                cursor.execute(
                    """
                    INSERT INTO orders_read_model
                        (id, customer_name, status, total_cents, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        status = EXCLUDED.status,
                        total_cents = EXCLUDED.total_cents,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        source_updated_at = NOW()
                    """,
                    (
                        order_id,
                        row["customer_name"],
                        row["status"],
                        row["total_cents"],
                        row["created_at"],
                        row["updated_at"],
                    ),
                )

            cursor.execute(
                """
                UPDATE pipeline_status
                SET processed_count = processed_count + 1,
                    last_event_at = NOW(),
                    last_error = NULL
                WHERE id = 1
                """
            )


def quarantine(message: Any, error: Exception) -> None:
    raw = message.value()
    raw_text = raw.decode("utf-8", errors="replace") if raw is not None else None
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO dead_letters (topic, partition_id, offset_id, error, raw_value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (topic, partition_id, offset_id) DO NOTHING
                RETURNING id
                """,
                (message.topic(), message.partition(), message.offset(), str(error), raw_text),
            )
            if cursor.fetchone() is not None:
                cursor.execute(
                    """
                    UPDATE pipeline_status
                    SET failed_count = failed_count + 1, last_error = %s
                    WHERE id = 1
                    """,
                    (str(error)[:1000],),
                )


def current_lag(consumer: Consumer) -> int | None:
    try:
        metadata = consumer.list_topics(TOPIC, timeout=2)
        topic_metadata = metadata.topics.get(TOPIC)
        if topic_metadata is None or topic_metadata.error:
            return None

        lag = 0
        partitions = [TopicPartition(TOPIC, number) for number in topic_metadata.partitions]
        committed = consumer.committed(partitions, timeout=2)
        for partition in committed:
            low, high = consumer.get_watermark_offsets(partition, timeout=2)
            next_offset = partition.offset if partition.offset >= 0 else low
            lag += max(0, high - max(low, next_offset))
        return lag
    except KafkaException:
        return None


def heartbeat(consumer: Consumer) -> None:
    lag = current_lag(consumer)
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE pipeline_status SET last_heartbeat = NOW(), consumer_lag = %s WHERE id = 1",
                (lag,),
            )


def main() -> None:
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([TOPIC])
    next_heartbeat = 0.0
    logger.info("Consuming %s as group %s", TOPIC, GROUP_ID)

    try:
        while running:
            message = consumer.poll(1.0)
            if message is not None:
                if message.error():
                    logger.warning("Kafka consumer event: %s", message.error())
                else:
                    try:
                        apply_event(message)
                        commit(consumer, message)
                    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError) as error:
                        logger.error("Quarantining %s[%s]@%s: %s", message.topic(), message.partition(), message.offset(), error)
                        quarantine(message, error)
                        commit(consumer, message)
                    except Exception:
                        # Stop the process so Docker restarts it from the last
                        # committed offset. Continuing polling could advance
                        # the in-memory position past this uncommitted record.
                        logger.exception("Processing failed; stopping for safe replay")
                        raise

            if time.monotonic() >= next_heartbeat:
                try:
                    heartbeat(consumer)
                except Exception:
                    logger.exception("Could not update pipeline heartbeat")
                next_heartbeat = time.monotonic() + HEARTBEAT_SECONDS
    finally:
        consumer.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    main()
