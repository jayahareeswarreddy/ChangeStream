"""Unit tests for event parsing and validation. No database or Kafka needed."""

from __future__ import annotations

import json

import pytest

from consumer import ParsedEvent, parse_event

from conftest import make_event


def test_insert_is_parsed() -> None:
    parsed = parse_event(json.dumps(make_event("c", 1, status="created")))
    assert isinstance(parsed, ParsedEvent)
    assert parsed.operation == "c"
    assert parsed.order_id == 1
    assert parsed.row["status"] == "created"
    assert parsed.source_ts_ms == 1_700_000_000_000


def test_update_is_parsed() -> None:
    parsed = parse_event(json.dumps(make_event("u", 5, status="paid")))
    assert parsed is not None
    assert parsed.operation == "u"
    assert parsed.row["status"] == "paid"


def test_snapshot_read_is_parsed() -> None:
    parsed = parse_event(json.dumps(make_event("r", 9)))
    assert parsed is not None
    assert parsed.operation == "r"


def test_delete_uses_before_row() -> None:
    parsed = parse_event(json.dumps(make_event("d", 2)))
    assert parsed is not None
    assert parsed.operation == "d"
    assert parsed.order_id == 2  # taken from `before`, since `after` is null on delete


def test_tombstone_returns_none() -> None:
    assert parse_event(None) is None


def test_invalid_json_raises() -> None:
    with pytest.raises(json.JSONDecodeError):
        parse_event(b"{ not valid json")


def test_non_object_json_raises() -> None:
    with pytest.raises(ValueError):
        parse_event(json.dumps([1, 2, 3]))


def test_unsupported_operation_raises() -> None:
    with pytest.raises(ValueError):
        parse_event(json.dumps({"op": "x", "after": {"id": 1}}))


def test_missing_id_raises() -> None:
    event = make_event("c", 1)
    del event["after"]["id"]
    with pytest.raises(ValueError):
        parse_event(json.dumps(event))


def test_delete_without_before_raises() -> None:
    with pytest.raises(ValueError):
        parse_event(json.dumps({"op": "d", "before": None, "after": None}))
