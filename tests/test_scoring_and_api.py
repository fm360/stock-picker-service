import json

import pytest
from fastapi.testclient import TestClient

from stockpicker.api import app, _make_storage
from stockpicker.scoring import compute_leaderboard
from stockpicker.sharding import ShardRouter
from stockpicker.storage import InMemoryShard, Picks, ShardedStorage
from stockpicker.validation import parse_tick_event


def _storage():
    router = ShardRouter([InMemoryShard(), InMemoryShard(), InMemoryShard()])
    return ShardedStorage(router)


def test_compute_leaderboard_basic():
    s = _storage()
    s.create_user("u1", "Ada")
    s.submit_picks(Picks(user_id="u1", round="2026-01-10", symbols=["AAPL", "MSFT"]))
    # Apply ticks to produce known returns: AAPL +10%, MSFT -10% => avg 0.
    s.apply_tick(parse_tick_event({"event_id": "e1", "symbol": "AAPL", "price": 100.0, "ts": "2026-01-10T00:00:00Z"}))
    s.apply_tick(parse_tick_event({"event_id": "e2", "symbol": "AAPL", "price": 110.0, "ts": "2026-01-10T23:59:59Z"}))
    s.apply_tick(parse_tick_event({"event_id": "e3", "symbol": "MSFT", "price": 200.0, "ts": "2026-01-10T00:00:00Z"}))
    s.apply_tick(parse_tick_event({"event_id": "e4", "symbol": "MSFT", "price": 180.0, "ts": "2026-01-10T23:59:59Z"}))

    lb = compute_leaderboard(s, "2026-01-10")
    assert len(lb) == 1
    assert lb[0].user_id == "u1"
    assert abs(lb[0].score - 0.0) < 1e-9


def test_api_contract_smoke():
    client = TestClient(app)
    r = client.post("/api/users", json={"name": "Ada"})
    assert r.status_code == 200
    user_id = r.json()["user_id"]

    r2 = client.post("/api/picks", json={"user_id": user_id, "round": "2026-01-10", "symbols": ["AAPL"]})
    assert r2.status_code == 200


