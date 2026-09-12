from __future__ import annotations

import json
import os
import time
from typing import Any

from .storage import ShardedStorage
from .validation import ValidationError, parse_tick_event

DEFAULT_KAFKA_TOPIC = "stock_ticks"
CONSUMER_GROUP_ID = "stockpicker-ingestor"
CONSUMER_AUTO_OFFSET_RESET = "earliest"
CONSUMER_TIMEOUT_MS = 1000
POLL_SLEEP_SECONDS = 0.1
ENCODING = "utf-8"

ERROR_DECODE = "decode_error"
ERROR_JSON_PARSE = "json_parse_error"
ERROR_VALIDATION = "validation_error"


def process_tick_message(storage: ShardedStorage, raw: bytes) -> dict[str, Any]:
    """
    Decode, parse and validate one raw Kafka message, then apply it to storage.

    Returns {"ok": True} on success, {"ok": True, "duplicate": True} if the
    event_id was already processed, or {"ok": False, "error": ..., "message": ...}
    for undecodable bytes, malformed JSON, or a tick that fails validation.
    """
    try:
        decoded = raw.decode(ENCODING)
    except UnicodeDecodeError as e:
        return {"ok": False, "error": ERROR_DECODE, "message": str(e)}
    try:
        obj = json.loads(decoded)
    except json.JSONDecodeError as e:
        return {"ok": False, "error": ERROR_JSON_PARSE, "message": str(e)}
    try:
        tick = parse_tick_event(obj)
    except ValidationError as e:
        return {"ok": False, "error": ERROR_VALIDATION, "message": str(e)}
    applied = storage.apply_tick(tick)
    if not applied:
        return {"ok": True, "duplicate": True}
    return {"ok": True}


def run_consumer_loop(storage: ShardedStorage) -> None:
    """
    Blocking Kafka consumer loop.  Call from a background thread so it does
    not block the API server.  Exits silently if Kafka is not configured.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
    if not bootstrap:
        return

    from kafka import KafkaConsumer  # type: ignore

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )

    print(f"[ingestor] consuming topic={topic} bootstrap={bootstrap}", flush=True)
    while True:
        any_msg = False
        for msg in consumer:
            any_msg = True
            try:
                res = process_tick_message(storage, msg.value)
                if res.get("ok"):
                    consumer.commit()
                dup = " (duplicate)" if res.get("duplicate") else ""
                print(f"[ingestor] processed event ok={res['ok']}{dup}", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"[ingestor] ERROR processing message: {e}", flush=True)
        if not any_msg:
            time.sleep(POLL_SLEEP_SECONDS)


def main() -> None:
    """Standalone entry point: ``python -m stockpicker.ingestor``."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        print("[ingestor] KAFKA_BOOTSTRAP_SERVERS not set; exiting (unit tests do not require Kafka).")
        return

    from .api import _storage  # noqa: WPS433

    run_consumer_loop(_storage)


if __name__ == "__main__":
    main()
