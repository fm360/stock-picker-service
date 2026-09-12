import json

import pytest

from stockpicker.ingestor import process_tick_message
from stockpicker.sharding import ShardRouter
from stockpicker.storage import InMemoryShard, ShardedStorage


def _storage():
    router = ShardRouter([InMemoryShard(), InMemoryShard(), InMemoryShard()])
    return ShardedStorage(router)


def test_process_tick_message_handles_invalid_json():
    s = _storage()
    res = process_tick_message(s, b"not-json")
    assert res["ok"] is False


def test_process_tick_message_idempotent():
    s = _storage()
    tick = {"event_id": "e1", "symbol": "AAPL", "price": 100.0, "ts": "2026-01-10T00:00:00Z"}
    raw = json.dumps(tick).encode("utf-8")
    r1 = process_tick_message(s, raw)
    r2 = process_tick_message(s, raw)
    assert r1["ok"] is True
    assert r2["ok"] is True
    assert r2.get("duplicate") is True



