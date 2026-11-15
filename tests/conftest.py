"""Shared fixtures for the ChangeStream tests.

Unit tests (``test_parse_event.py``) need no services. The integration tests
(``test_projection.py``) exercise ``apply_event`` against a real PostgreSQL
read model and run only when ``TEST_DATABASE_URL`` is set, so a normal local
``pytest`` run never touches the live demo database. CI sets that variable to a
throwaway PostgreSQL service container.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

# Minimal read-model schema, mirroring the read_model half of postgres/init.sql.
# CREATE ... IF NOT EXISTS keeps the fixture idempotent across test runs.
SCHEMA = """
CREATE TABLE IF NOT EXISTS orders_read_model (
    id BIGINT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    operation TEXT NOT NULL,
    order_id BIGINT,
    source_ts_ms BIGINT,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    error TEXT NOT NULL,
    raw_value TEXT,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS pipeline_status (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    last_heartbeat TIMESTAMPTZ,
    consumer_lag BIGINT,
    processed_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ,
    last_error TEXT
);
INSERT INTO pipeline_status (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""


class FakeMessage:
    """A stand-in for a confluent_kafka Message, enough for ``apply_event``."""

    def __init__(self, value: Any, topic: str = "cdc.public.orders", partition: int = 0, offset: int = 0) -> None:
        if value is None or isinstance(value, (bytes, bytearray)):
            self._value = value
        else:
            self._value = json.dumps(value).encode()
        self._topic, self._partition, self._offset = topic, partition, offset

    def value(self) -> Any:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def make_event(op: str, order_id: int, *, customer_name: str = "Test", status: str = "created",
               total_cents: int = 1000) -> dict[str, Any]:
    """Build a Debezium-shaped change event for the sample orders table."""
    row = {
        "id": order_id,
        "customer_name": customer_name,
        "status": status,
        "total_cents": total_cents,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    event: dict[str, Any] = {"op": op, "source": {"ts_ms": 1_700_000_000_000}}
    if op == "d":
        event["before"] = row
        event["after"] = None
    else:
        event["after"] = row
        event["before"] = None
    return event


@pytest.fixture
def db(monkeypatch: pytest.MonkeyPatch) -> str:
    """Provide a clean read-model schema and point the consumer at it.

    Skips when TEST_DATABASE_URL is unset so local runs never hit the demo DB.
    """
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to run read-model integration tests")

    import psycopg

    import consumer

    with psycopg.connect(TEST_DATABASE_URL, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA)
            cursor.execute("TRUNCATE orders_read_model, processed_events, dead_letters")
            cursor.execute(
                "UPDATE pipeline_status SET processed_count = 0, failed_count = 0, "
                "last_error = NULL, last_event_at = NULL WHERE id = 1"
            )

    # Route the consumer's own connections at the throwaway test database.
    monkeypatch.setattr(consumer, "DATABASE_URL", TEST_DATABASE_URL)
    return TEST_DATABASE_URL
