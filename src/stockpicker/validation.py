from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

REQUIRED_TICK_FIELDS = ("event_id", "symbol", "price", "ts")
UTC_SUFFIX = "Z"
UTC_OFFSET_REPLACEMENT = "+00:00"
MIN_VALID_PRICE = 0


@dataclass(frozen=True)
class TickEvent:
    event_id: str
    symbol: str
    price: float
    ts: datetime  # UTC


class ValidationError(ValueError):
    pass


def parse_ts(ts: str) -> datetime:
    """
    Parse ISO-8601 timestamps like '2026-01-10T18:30:05.120Z' into a UTC datetime.
    """
    if not isinstance(ts, str):
        raise ValidationError("ts must be a string")
    s = ts.strip()
    if s.endswith(UTC_SUFFIX):
        s = s[:-1] + UTC_OFFSET_REPLACEMENT
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:  # noqa: BLE001 -- surface any parse failure as a ValidationError
        raise ValidationError(f"ts is not ISO-8601 parseable: {ts}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_tick_event(obj: dict[str, Any]) -> TickEvent:
    """
    Validate a raw tick dict and return a TickEvent.

    Requires event_id and symbol to be non-empty strings (whitespace stripped),
    price to be a positive number (bool rejected), and ts to be an ISO-8601
    timestamp (normalised to UTC). Raises ValidationError on any violation.
    """
    if not isinstance(obj, dict):
        raise ValidationError("Base Case: Only json dictionary can be used as input")
    
    for field in REQUIRED_TICK_FIELDS:
        if field not in obj:
            raise ValidationError(f"missing required field: {field}")
        
    event_id_stripped = obj["event_id"]
    if not isinstance(event_id_stripped, str):
        raise ValidationError("Base Case: event_id must be a string")
    event_id_stripped = event_id_stripped.strip()
    if not event_id_stripped:
        raise ValidationError("Base Case: event_id must be non-empty")
    symbol_stripped = obj["symbol"]
    if not isinstance(symbol_stripped, str):
        raise ValidationError("Base Case: symbol must be a string")
    symbol_stripped = symbol_stripped.strip()
    if not symbol_stripped:
        raise ValidationError("Base Case: symbol must be non-empty")
    price_raw = obj["price"]
    if isinstance(price_raw, bool) or not isinstance(price_raw, (int, float)):
        raise ValidationError("Base Case: price must be a number")
    price_float = float(price_raw)
    if price_float <= MIN_VALID_PRICE:
        raise ValidationError("Base Case: price must be positive")
    ts_parsed = parse_ts(obj["ts"])
    
    
    return TickEvent(event_id_stripped, symbol_stripped, price_float, ts_parsed)