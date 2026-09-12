from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .sharding import ShardRouter
from .validation import TickEvent

SHARD_KEY_USER_PREFIX = "user:"
SHARD_KEY_SYMBOL_PREFIX = "symbol:"
PRICE_START = "start"
PRICE_END = "end"


@dataclass(frozen=True)
class User:
    user_id: str
    name: str
    created_at: datetime


@dataclass(frozen=True)
class Picks:
    user_id: str
    round: str  # YYYY-MM-DD
    symbols: list[str]


class InMemoryShard:
    """
    A single in-memory shard.
    For educational purposes, we store:
    - users by user_id
    - picks by (round, user_id)
    - ticks "start/end" prices per (round, symbol)
    - processed event IDs for idempotency
    """

    def __init__(self) -> None:
        self.users: dict[str, User] = {}
        self.picks: dict[tuple[str, str], Picks] = {}
        self.processed_events: set[str] = set()
        self.prices: dict[tuple[str, str], dict[str, tuple[datetime, float]]] = {}


class ShardedStorage:
    """
    Sharded in-memory storage.

    Users and picks are routed by "user:<user_id>"; prices and processed
    event ids by "symbol:<symbol>", so all ticks for a symbol land on one
    shard and idempotency checks stay local to it.
    """

    def __init__(self, router: ShardRouter):
        self._router = router

    # -------------------------
    # Users
    # -------------------------
    def create_user(self, user_id: str, name: str) -> User:
        """Create a user with a UTC created_at timestamp on the user's shard."""
        shard = self._router.route(f"{SHARD_KEY_USER_PREFIX}{user_id}")
        user = User(
            user_id=user_id,
            name=name,
            created_at=datetime.now(timezone.utc),
        )
        shard.users[user_id] = user
        return user

    def get_user(self, user_id: str) -> User | None:
        """Return the user from its shard, or None if it does not exist."""
        shard = self._router.route(f"{SHARD_KEY_USER_PREFIX}{user_id}")
        return shard.users.get(user_id)

    # -------------------------
    # Picks
    # -------------------------
    def submit_picks(self, p: Picks) -> None:
        """Store (or replace) a user's picks for a round on the user's shard."""
        shard = self._router.route(f"{SHARD_KEY_USER_PREFIX}{p.user_id}")
        shard.picks[(p.round, p.user_id)] = p

    def list_picks_for_round(self, round: str) -> list[Picks]:
        """Collect every user's picks for the round across all shards."""
        picks_for_round = []
        
        for shard in self._router._shards:
            for (stored_round, _user_id), picks in shard.picks.items():
                if stored_round == round:
                    picks_for_round.append(picks)
    
        return picks_for_round

    # -------------------------
    # Ticks (stored as start/end prices per day)
    # -------------------------
    def apply_tick(self, tick: TickEvent) -> bool:
        """
        Apply a tick to the symbol's shard, tracking the earliest and latest
        price seen per (round, symbol). Returns False without changing state
        if the event_id has already been processed (idempotent ingestion).
        """
        shard = self._router.route(f"{SHARD_KEY_SYMBOL_PREFIX}{tick.symbol}")
        round_str = tick.ts.date().isoformat()
        price_key = (round_str, tick.symbol)
    
        if tick.event_id in shard.processed_events:
            return False
    
        shard.processed_events.add(tick.event_id)
    
        if price_key not in shard.prices:
            shard.prices[price_key] = {}
    
        price_entry = shard.prices[price_key]
    
        start_entry = price_entry.get(PRICE_START)
        if start_entry is None or tick.ts < start_entry[0]:
            price_entry[PRICE_START] = (tick.ts, tick.price)
        
        end_entry = price_entry.get(PRICE_END)
        if end_entry is None or tick.ts > end_entry[0]:
            price_entry[PRICE_END] = (tick.ts, tick.price)
        return True

    def get_start_end(self, round: str, symbol: str) -> tuple[float | None, float | None]:
        """
        Return (start_price, end_price) for a symbol in a round, taken from the
        earliest and latest ticks applied. Either value is None if missing.
        """
        shard = self._router.route(f"{SHARD_KEY_SYMBOL_PREFIX}{symbol}")
        price_data = shard.prices.get((round, symbol))
    
        if price_data is None:
            return (None, None)
        
        start_entry = price_data.get(PRICE_START)
        end_entry = price_data.get(PRICE_END)
    
        start_price = start_entry[1] if start_entry is not None else None
        end_price = end_entry[1] if end_entry is not None else None
        return (start_price, end_price)
