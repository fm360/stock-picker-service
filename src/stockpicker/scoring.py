from __future__ import annotations

from dataclasses import dataclass

from .storage import Picks, ShardedStorage

DEFAULT_SCORE = 0.0
MIN_VALID_START_PRICE = 0


@dataclass(frozen=True)
class LeaderboardEntry:
    user_id: str
    score: float


def compute_user_score(storage: ShardedStorage, p: Picks) -> float:
    """
    Score a user's picks for a round as the mean simple return
    (end - start) / start across the picked symbols.

    Symbols with no price data for the round, or a non-positive start price,
    are skipped. Returns DEFAULT_SCORE when no symbol can be scored.
    """
    total_return = 0.0
    valid_symbol_count = 0

    for symbol in p.symbols:
        start_price, end_price = storage.get_start_end(p.round, symbol)
        
        #use continue to skip unwanted cases
        if start_price is None or end_price is None:
            continue
        
        if start_price <= MIN_VALID_START_PRICE:
            continue
        
        symbol_return = (end_price - start_price) / start_price
        total_return += symbol_return
        valid_symbol_count += 1
    
    if valid_symbol_count == 0:
        return DEFAULT_SCORE
    
    return total_return / valid_symbol_count


def compute_leaderboard(storage: ShardedStorage, round: str) -> list[LeaderboardEntry]:
    """
    Build the leaderboard for a round: one entry per user who submitted picks,
    sorted by score descending with user_id ascending as the tie-breaker.
    """
    leaderboard = []
    picks_for_round = storage.list_picks_for_round(round)
    
    for picks in picks_for_round:
        score = compute_user_score(storage, picks)
        entry = LeaderboardEntry(
            user_id=picks.user_id,
            score=score,
        )
        leaderboard.append(entry)
    
    leaderboard.sort(key=lambda entry: (-entry.score, entry.user_id))
    return leaderboard
