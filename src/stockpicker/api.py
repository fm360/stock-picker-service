from __future__ import annotations

import os
import threading
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .scoring import compute_leaderboard
from .sharding import ShardRouter
from .storage import InMemoryShard, Picks, ShardedStorage

NUM_SHARDS = 3
USER_ID_PREFIX = "u_"
USER_ID_HEX_LENGTH = 8
ROUND_DATE_LENGTH = 10  # YYYY-MM-DD
RANK_OFFSET = 1  # 1-based ranking
DEFAULT_SCORE = 0.0

APP_TITLE = "Stock Picker Service"

HTTP_400_BAD_REQUEST = 400
HTTP_404_NOT_FOUND = 404


class CreateUserIn(BaseModel):
    name: str = Field(min_length=1)


class CreateUserOut(BaseModel):
    user_id: str
    name: str
    created_at: str


class SubmitPicksIn(BaseModel):
    user_id: str = Field(min_length=1)
    round: str = Field(min_length=ROUND_DATE_LENGTH, max_length=ROUND_DATE_LENGTH)
    symbols: list[str] = Field(min_length=1)


def _make_storage() -> ShardedStorage:
    shards = [InMemoryShard() for _ in range(NUM_SHARDS)]
    router = ShardRouter(shards)
    return ShardedStorage(router)


_storage = _make_storage()


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Start the Kafka ingestor in a background thread when configured."""
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        from .ingestor import run_consumer_loop

        t = threading.Thread(target=run_consumer_loop, args=(_storage,), daemon=True)
        t.start()
        print("[api] ingestor thread started (shares in-memory storage with API)", flush=True)
    yield


app = FastAPI(title=APP_TITLE, lifespan=_lifespan)


@app.post("/api/users", response_model=CreateUserOut)
def create_user(inp: CreateUserIn):
    """Create a user from a non-empty name; returns the generated user_id."""
    name = inp.name.strip()
    if not name:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="name must be non-empty")
    user_id = f"{USER_ID_PREFIX}{uuid.uuid4().hex[:USER_ID_HEX_LENGTH]}"
    user = _storage.create_user(user_id, name)
    return CreateUserOut(user_id=user.user_id, name=user.name, created_at=user.created_at.isoformat().replace("+00:00", "Z"))


@app.post("/api/picks")
def submit_picks(inp: SubmitPicksIn):
    """Record a user's symbol picks for a round; 404 if the user does not exist."""
    user = _storage.get_user(inp.user_id)
    if user is None:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND,detail="user not found")
    
    try:
        from datetime import date
        date.fromisoformat(inp.round)
    except ValueError:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="round must be YYYY-MM-DD")
    
    cleaned_symbols = [symbol.strip() for symbol in inp.symbols if symbol.strip()]
    if not cleaned_symbols:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="symbols must be non-empty")
    
    picks = Picks(user_id=inp.user_id, round=inp.round, symbols=cleaned_symbols)
    _storage.submit_picks(picks)

    return {"ok": True}


@app.get("/api/leaderboard")
def leaderboard(round: str):
    """Return the scored, sorted leaderboard for a round (YYYY-MM-DD)."""
    try:
        from datetime import date
        date.fromisoformat(round)
    except ValueError:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="round must be YYYY-MM-DD")
    
    leaderboard_entries = compute_leaderboard(_storage, round)
    entries = []

    for rank, entry in enumerate(leaderboard_entries, start=RANK_OFFSET):
        entries.append(
            {
                "rank": rank,
                "user_id": entry.user_id,
                "score": entry.score,
            }
        )

    return {
        "round": round,
        "entries": entries,
    }


@app.get("/api/portfolio/{user_id}")
def portfolio(user_id: str, round: str):
    """Return a user's picks and score for a round; 404 if user or picks are missing."""
    from datetime import date
    from .scoring import compute_user_score
    try:
        date.fromisoformat(round)
    except ValueError:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="round must be YYYY-MM-DD")
    picks_for_round = _storage.list_picks_for_round(round)
    user_picks = None
    for picks in picks_for_round:
        if picks.user_id == user_id:
            user_picks = picks
            break
    if user_picks is None:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="no picks for user/round")
    
    score = compute_user_score(_storage, user_picks)
    
    return {
        "user_id": user_id,
        "round": round,
        "symbols": user_picks.symbols,
        "score": score,
    }
