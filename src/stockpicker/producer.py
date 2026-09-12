from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

DEFAULT_SYMBOLS = "AAPL,MSFT,GOOG"
DEFAULT_TICK_RATE = 10.0
DEFAULT_DURATION_SECONDS = 5.0
DEFAULT_BAD_EVENT_FRACTION = 0.0
DEFAULT_KAFKA_TOPIC = "stock_ticks"

PRICE_BASE = 100.0
PRICE_SPREAD = 50.0
PRICE_FLOOR = 0.01
PRICE_STEP_MIN = -1.0
PRICE_STEP_MAX = 1.0
PRICE_DECIMAL_PLACES = 2

INVALID_PRICE = -1
INVALID_TIMESTAMP = "not-a-timestamp"

MIN_RATE_DIVISOR = 1e-6
KAFKA_FLUSH_TIMEOUT_SECONDS = 0.1

ENCODING = "utf-8"


def _now_iso_z() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--rate", type=float, default=DEFAULT_TICK_RATE, help="ticks/second")
    parser.add_argument("--seconds", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_EVENT_FRACTION, help="fraction of invalid events")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)

    producer = None
    if bootstrap:
        from kafka import KafkaProducer  # type: ignore
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        print(f"[producer] sending to kafka topic={topic} bootstrap={bootstrap}")
    else:
        print("[producer] KAFKA_BOOTSTRAP_SERVERS not set; printing events to stdout only.")

    interval = 1.0 / max(args.rate, MIN_RATE_DIVISOR)
    end_time = time.time() + args.seconds

    last_price = {s: PRICE_BASE + random.random() * PRICE_SPREAD for s in symbols}
    while time.time() < end_time:
        sym = random.choice(symbols)
        delta = random.uniform(PRICE_STEP_MIN, PRICE_STEP_MAX)
        last_price[sym] = max(PRICE_FLOOR, last_price[sym] + delta)

        is_bad = random.random() < args.bad_rate
        if is_bad:
            payload = {"event_id": str(uuid.uuid4()), "symbol": sym, "price": INVALID_PRICE, "ts": INVALID_TIMESTAMP}
        else:
            payload = {"event_id": str(uuid.uuid4()), "symbol": sym, "price": round(last_price[sym], PRICE_DECIMAL_PLACES), "ts": _now_iso_z()}

        raw = json.dumps(payload).encode(ENCODING)
        if producer:
            producer.send(topic, raw)
            producer.flush(KAFKA_FLUSH_TIMEOUT_SECONDS)
        print(f"[producer] sent {payload}")
        time.sleep(interval)


if __name__ == "__main__":
    main()
