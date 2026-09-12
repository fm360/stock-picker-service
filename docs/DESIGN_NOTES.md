# DESIGN_NOTES.md

## Overview

This file records the latency, throughput, reliability behavior, and design trade-offs for my Stock Picker Service. I wrote this after getting the Python tests to pass and after bringing the full Docker Compose stack up with Kafka, the API service, and the Nginx gateway.

## Latency

For the public entry point, I used the exact gateway-based load test flow from the handout:

```bash
docker compose up -d
make load-test
```

My `make load-test` result was:

```text
Load test: GET http://localhost:8080/api/leaderboard?round=2026-01-10
  requests=100  concurrency=5

--- Results ---
  Total requests : 100
  Wall clock     : 0.14s
  Throughput     : 736.1 req/s

  Latency p50    : 1.3 ms
  Latency p95    : 4.0 ms
  Latency p99    : 15.2 ms
  Latency min    : 0.5 ms
  Latency max    : 30.4 ms
  Latency mean   : 2.1 ms

  Status codes:
    200: 1
    429: 99
```

My interpretation:

- The gateway responded very quickly, but almost all requests were `429 Too Many Requests`.
- This was expected because the Nginx gateway is configured to enforce `10 req/s` per IP, and the default load test sends a much bigger burst than that.
- So these numbers are useful as evidence that the rate limit is working, but they are not a clean measurement of the FastAPI app by itself.

To separate gateway throttling from application latency, I also ran the same script directly against the FastAPI server on port `8000`:

```bash
python3 scripts/load_test.py --url "http://localhost:8000/api/leaderboard?round=2026-01-10" --requests 100 --concurrency 5
```

That result was:

```text
Load test: GET http://localhost:8000/api/leaderboard?round=2026-01-10
  requests=100  concurrency=5

--- Results ---
  Total requests : 100
  Wall clock     : 0.13s
  Throughput     : 769.8 req/s

  Latency p50    : 2.5 ms
  Latency p95    : 8.3 ms
  Latency p99    : 11.2 ms
  Latency min    : 0.9 ms
  Latency max    : 11.7 ms
  Latency mean   : 3.2 ms

  Status codes:
    200: 100
```

My interpretation:

- The application itself was still very fast for this small in-memory project.
- The direct API `p50` was `2.5 ms` and the `p95` was `8.3 ms`.
- The gateway test is still the more important public-facing result, because clients are supposed to go through Nginx, but the direct API result helped me understand the difference between app latency and gateway throttling.

## Ingestion Throughput

To measure ingestion throughput, I used a short controlled producer run and then counted both producer sends and ingestor success logs.

Command used:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock_ticks
PYTHONPATH=src python3 -m stockpicker.producer --symbols AAPL,MSFT,GOOG --rate 50 --seconds 10 --seed 42
```

Observed counts:

- Producer log count: `360` lines of `[producer] sent`
- API log count: `360` lines of `[ingestor] processed event ok=True`

From that run, my approximate sustained ingestion throughput was:

- `360 events / 10 seconds = about 36 ticks/sec`

My interpretation:

- The consumer kept up with the producer during this run because the number of ingested events matched the number of produced events.
- The producer did not actually reach the requested `50 ticks/sec` on my machine. The real achieved rate was closer to `36 ticks/sec`.
- Because the ingestor matched that same count, I would say the pipeline kept up with the real producer speed and Kafka lag stayed small in this test.

## Reliability Scenario

The failure scenario I tested was restarting the `api` container while the stack was running.

Steps I used:

```bash
docker compose restart api
```

Before restart:

- I created a user and submitted picks.
- I checked the portfolio endpoint directly on port `8000` and got a `200 OK` response.

What I observed during restart:

- An immediate request through the gateway returned `502 Bad Gateway` while the API container was still coming back up.

After restart:

- The API logs showed the service came back successfully.
- I saw the ingestor thread start again.
- I saw the consumer reconnect to Kafka.

Relevant log lines:

```text
[api] ingestor thread started (shares in-memory storage with API)
[ingestor] consuming topic=stock_ticks bootstrap=kafka:29092
```

I then sent another short producer burst after the restart:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock_ticks
PYTHONPATH=src python3 -m stockpicker.producer --symbols AAPL,MSFT --rate 5 --seconds 2 --seed 7
```

That sent `10` events, and the API log showed `10` new lines of `[ingestor] processed event ok=True`, so the consumer was working again after the restart.

There was also an important limitation:

- After the restart, the portfolio request for the user I created earlier returned `404 Not Found`.
- This happened because the storage layer is in-memory and process-local, so restarting the `api` container wipes users, picks, prices, and processed event IDs.

My interpretation:

- The service does recover in the sense that the API process restarts, the ingestor thread reconnects, and new messages keep getting processed.
- The weak point is durability. Since storage is only in memory, restarting the API loses the current state.
- So my design is good enough for the assignment and unit tests, but it is not durable enough for a real production service.

## Design Trade-offs

These are the main design choices I made and the trade-offs behind them.

- I used stable hash modulo `N` for sharding instead of consistent hashing.
- I picked this because the project uses a fixed shard count of `3`, and modulo routing is simpler to implement and easier to test.

- I routed users and picks by `user_id`, but I routed ticks and prices by `symbol`.
- This keeps most writes single-shard, which makes the basic data path easy to understand.
- The downside is that round-wide reads like the leaderboard need to scan all shards to gather picks for a given date.

- I used in-memory shard objects instead of a shared database.
- This made the code much simpler and kept latency low, but it also means state is not durable across container restarts.

- I committed Kafka offsets only after a message was decoded, validated, and stored successfully.
- That is better for correctness because it avoids acknowledging a message before the system actually uses it.
- I also used `processed_events` for idempotency so duplicate event IDs are ignored during normal operation.

- I used Nginx rate limiting at `10 req/s` per IP because that is what the handout asked for.
- The good part is that the gateway clearly protects the API from burst traffic.
- The downside is that the default load test gets dominated by `429` responses, so I needed one extra direct-API measurement to understand the raw application latency.

## Final Summary

My main takeaways are:

- The project is fast for a small in-memory service.
- The public gateway correctly enforces the required rate limit.
- Kafka ingestion worked correctly in my tests, and the consumer kept up with the real producer speed I measured.
- The biggest weakness is durability, because the starter architecture stores everything in memory inside the `api` process.

If I were extending this project further, the first improvement I would make would be replacing the in-memory shards with a shared persistent database so that API restarts do not erase the state.
