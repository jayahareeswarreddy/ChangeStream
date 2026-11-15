"""Read-only API and dashboard for the CDC read model."""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import psycopg
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row

DATABASE_URL = os.getenv(
    "SINK_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/read_model"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cdc.public.orders")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "changestream-readmodel")
SOURCE_DATABASE_URL = os.getenv(
    "SOURCE_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/source_db"
)
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
CONNECT_PUBLIC_URL = os.getenv("CONNECT_PUBLIC_URL", "http://localhost:8083")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "changestream-orders")
INDEX_FILE = Path(__file__).parent / "static" / "index.html"
GUIDE_FILE = Path(__file__).parent / "static" / "guide.html"
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

app = FastAPI(title="ChangeStream", version="0.1.0")


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as connection:
        return list(connection.execute(query, params).fetchall())


def broker_lag() -> int | None:
    """Read committed group offsets and log-end offsets directly from Kafka."""
    try:
        metadata = admin.list_topics(KAFKA_TOPIC, timeout=2)
        topic_metadata = metadata.topics.get(KAFKA_TOPIC)
        if topic_metadata is None or topic_metadata.error or not topic_metadata.partitions:
            return None
        partitions = [TopicPartition(KAFKA_TOPIC, number) for number in topic_metadata.partitions]
        group_future = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(KAFKA_GROUP_ID, partitions)], request_timeout=2
        )[KAFKA_GROUP_ID]
        group_offsets = group_future.result(timeout=3).topic_partitions
        committed = {partition.partition: partition.offset for partition in group_offsets}
        latest = admin.list_offsets(
            {partition: OffsetSpec.latest() for partition in partitions}, request_timeout=2
        )
        return sum(
            max(0, future.result(timeout=3).offset - max(0, committed.get(partition.partition, 0)))
            for partition, future in latest.items()
        )
    except Exception:
        # Kafka may not be ready yet or the topic/group may not exist yet.
        return None


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(INDEX_FILE)


@app.get("/guide", include_in_schema=False)
def guide() -> FileResponse:
    return FileResponse(GUIDE_FILE)


@app.get("/api/health")
def health() -> dict[str, Any]:
    try:
        row = fetch_all(
            """
            SELECT last_heartbeat, consumer_lag, processed_count, failed_count,
                   last_event_at, last_error
            FROM pipeline_status WHERE id = 1
            """
        )[0]
    except Exception as error:
        raise HTTPException(status_code=503, detail="Read model database unavailable") from error

    heartbeat = row["last_heartbeat"]
    row["consumer_online"] = bool(
        heartbeat and (datetime.now(timezone.utc) - heartbeat).total_seconds() < 20
    )
    # Calculate against Kafka on every health request so lag continues to rise
    # during the demo even while the consumer process is stopped.
    row["consumer_lag"] = broker_lag()
    return row


def database_service(url: str, label: str) -> dict[str, str]:
    try:
        with psycopg.connect(url, connect_timeout=2) as connection:
            connection.execute("SELECT 1")
        return {"status": "online", "detail": f"{label} connected"}
    except Exception:
        return {"status": "offline", "detail": f"{label} unavailable"}


@app.get("/api/services")
def services() -> dict[str, Any]:
    """Expose the live state of each pipeline component in the dashboard."""
    source = database_service(SOURCE_DATABASE_URL, "source_db")
    read_model = database_service(DATABASE_URL, "read_model")

    try:
        metadata = admin.list_topics(timeout=2)
        broker_count = len(metadata.brokers)
        topic = metadata.topics.get(KAFKA_TOPIC)
        topic_ready = topic is not None and not topic.error
        kafka = {
            "status": "online" if broker_count else "offline",
            "detail": f"{broker_count} broker · {KAFKA_TOPIC}" if broker_count else "No broker metadata",
            "topic_ready": topic_ready,
        }
    except Exception:
        kafka = {"status": "offline", "detail": "Broker unavailable", "topic_ready": False}

    try:
        with urlopen(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=2) as response:
            connector = json.loads(response.read())
        task_states = [task.get("state", "UNKNOWN") for task in connector.get("tasks", [])]
        connector_state = connector.get("connector", {}).get("state", "UNKNOWN")
        connector_online = connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states)
        debezium = {
            "status": "online" if connector_online else "degraded",
            "detail": f"{CONNECTOR_NAME} · {len(task_states)} task(s)",
            "connector_state": connector_state,
            "task_states": task_states,
            "url": f"{CONNECT_PUBLIC_URL}/connectors/{CONNECTOR_NAME}/status",
        }
    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
        debezium = {
            "status": "offline",
            "detail": "Kafka Connect or connector unavailable",
            "connector_state": "UNKNOWN",
            "task_states": [],
            "url": f"{CONNECT_PUBLIC_URL}/connectors/{CONNECTOR_NAME}/status",
        }

    try:
        pipeline = health()
        consumer_online = bool(pipeline["consumer_online"])
        consumer = {
            "status": "online" if consumer_online else "offline",
            "detail": f"{pipeline['consumer_lag'] if pipeline['consumer_lag'] is not None else '—'} events behind",
        }
    except HTTPException:
        consumer = {"status": "offline", "detail": "Read-model heartbeat unavailable"}

    return {
        "source_postgres": source,
        "debezium": debezium,
        "kafka": kafka,
        "consumer": consumer,
        "read_model": read_model,
    }


@app.get("/api/orders")
def orders() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT id, customer_name, status, total_cents, created_at, updated_at,
               source_updated_at
        FROM orders_read_model ORDER BY id
        """
    )


@app.get("/api/events")
def events(limit: int = Query(default=50, ge=1, le=200)) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT id, topic, partition_id, offset_id, operation, order_id,
               source_ts_ms, processed_at
        FROM processed_events ORDER BY id DESC LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/dead-letters")
def dead_letters(limit: int = Query(default=25, ge=1, le=100)) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT id, topic, partition_id, offset_id, error, raw_value, failed_at
        FROM dead_letters ORDER BY id DESC LIMIT %s
        """,
        (limit,),
    )
