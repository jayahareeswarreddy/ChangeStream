"""Integration tests: apply events to a real PostgreSQL read model.

These run only when TEST_DATABASE_URL is set (the `db` fixture skips otherwise),
so a normal local run won't touch the demo database. They prove the properties
the project claims: idempotent replay, deletes, and dead-letter de-duplication.
"""

from __future__ import annotations

import psycopg

import consumer

from conftest import FakeMessage, make_event


def _scalar(url: str, query: str, params: tuple = ()) -> object:
    with psycopg.connect(url) as connection:
        return connection.execute(query, params).fetchone()[0]


def test_insert_creates_read_model_row(db: str) -> None:
    consumer.apply_event(FakeMessage(make_event("c", 1, customer_name="Asha", status="created"), offset=0))

    assert _scalar(db, "SELECT count(*) FROM orders_read_model") == 1
    assert _scalar(db, "SELECT status FROM orders_read_model WHERE id = 1") == "created"
    assert _scalar(db, "SELECT processed_count FROM pipeline_status WHERE id = 1") == 1


def test_replay_of_same_offset_is_idempotent(db: str) -> None:
    message = FakeMessage(make_event("c", 1, status="created"), offset=0)
    consumer.apply_event(message)
    consumer.apply_event(message)  # redelivery after a crash between DB and offset commit

    # The unique (topic, partition, offset) key makes the second apply a no-op.
    assert _scalar(db, "SELECT count(*) FROM orders_read_model") == 1
    assert _scalar(db, "SELECT count(*) FROM processed_events") == 1
    assert _scalar(db, "SELECT processed_count FROM pipeline_status WHERE id = 1") == 1


def test_update_changes_status(db: str) -> None:
    consumer.apply_event(FakeMessage(make_event("c", 1, status="created"), offset=0))
    consumer.apply_event(FakeMessage(make_event("u", 1, status="paid"), offset=1))

    assert _scalar(db, "SELECT status FROM orders_read_model WHERE id = 1") == "paid"
    assert _scalar(db, "SELECT count(*) FROM processed_events") == 2


def test_delete_removes_row(db: str) -> None:
    consumer.apply_event(FakeMessage(make_event("c", 1), offset=0))
    consumer.apply_event(FakeMessage(make_event("d", 1), offset=1))

    assert _scalar(db, "SELECT count(*) FROM orders_read_model WHERE id = 1") == 0
    # Both the insert and the delete are recorded in the processed log.
    assert _scalar(db, "SELECT count(*) FROM processed_events") == 2


def test_tombstone_is_a_no_op(db: str) -> None:
    consumer.apply_event(FakeMessage(None, offset=0))  # tombstone

    assert _scalar(db, "SELECT count(*) FROM processed_events") == 0
    assert _scalar(db, "SELECT processed_count FROM pipeline_status WHERE id = 1") == 0


def test_quarantine_dedupes_and_counts(db: str) -> None:
    bad = FakeMessage(b"{ not valid json", offset=7)
    consumer.quarantine(bad, ValueError("bad payload"))
    consumer.quarantine(bad, ValueError("bad payload"))  # redelivery of the same offset

    assert _scalar(db, "SELECT count(*) FROM dead_letters") == 1
    assert _scalar(db, "SELECT failed_count FROM pipeline_status WHERE id = 1") == 1
