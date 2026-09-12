# CS496 Project - Stock Picker Service

This is my submission for the Stock Picker Service project. I built a backend that uses Kafka for stock tick ingestion, FastAPI for the REST API, in-memory sharded storage for state, and Nginx as the API gateway.

I followed the requirements in the course handouts (`docs/stockpicker.pdf` and
`docs/project_appendix.pdf`), which are not included in this repository.

## Quick Start

To verify the Python code first, I run:

```bash
make install
make test
```

Expected result:

- the provided pytest suite passes
- on my run, `make test` finished with `7 passed`

If I want to run one area at a time, I use:

```bash
python3 -m pytest -q tests/test_sharding.py
python3 -m pytest -q tests/test_validation_and_ingestion.py
python3 -m pytest -q tests/test_scoring_and_api.py
```

## Main Files

The main project files are:

- `src/stockpicker/sharding.py` for shard routing
- `src/stockpicker/validation.py` for tick validation
- `src/stockpicker/storage.py` for the sharded storage layer
- `src/stockpicker/ingestor.py` for Kafka message processing
- `src/stockpicker/scoring.py` for portfolio scoring and leaderboard sorting
- `src/stockpicker/api.py` for the FastAPI endpoints
- `services/gateway/nginx.conf` for the gateway and rate limit

The required API endpoints are:

- `POST /api/users`
- `POST /api/picks`
- `GET /api/leaderboard?round=YYYY-MM-DD`
- `GET /api/portfolio/{user_id}?round=YYYY-MM-DD`

## Local Run

For quick local API testing, I run:

```bash
make run-api
```

This starts FastAPI on port `8000`.

Example requests:

```bash
curl -s -X POST "http://localhost:8000/api/users" \
  -H "Content-Type: application/json" \
  -d '{"name":"Ada"}'

curl -s -X POST "http://localhost:8000/api/picks" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"PUT_USER_ID_HERE","round":"2026-01-10","symbols":["AAPL","MSFT"]}'

curl -s "http://localhost:8000/api/leaderboard?round=2026-01-10"

curl -s "http://localhost:8000/api/portfolio/PUT_USER_ID_HERE?round=2026-01-10"
```

Expected user creation output shape:

```json
{"user_id":"u_xxxxxxxx","name":"Ada","created_at":"...Z"}
```

Important note:

- this mode is only for Python-side testing
- it does not start Kafka or the Nginx gateway

## Full Stack Run

To run the full system, I use Docker Compose:

```bash
docker compose up -d
docker compose ps
docker compose logs -f api gateway
```

This starts:

- Zookeeper
- Kafka
- the API service
- the Nginx gateway

Expected ports:

- Kafka on `9092`
- FastAPI on `8000`
- gateway on `8080`

Important detail:

- clients are supposed to go through the gateway on `8080`
- the Kafka ingestor runs inside the `api` container as a background thread

## End-to-End Demo

This is the simplest flow I use to show producer -> Kafka -> ingestor -> storage -> API.

### 1. Start the full stack

```bash
docker compose up -d
docker compose ps
```

### 2. Create a user through the gateway

```bash
curl -s -X POST "http://localhost:8080/api/users" \
  -H "Content-Type: application/json" \
  -d '{"name":"Ada Lovelace"}'
```

Expected output shape:

```json
{"user_id":"u_xxxxxxxx","name":"Ada Lovelace","created_at":"...Z"}
```

### 3. Submit picks for a round

```bash
curl -s -X POST "http://localhost:8080/api/picks" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"PUT_USER_ID_HERE","round":"2026-01-10","symbols":["AAPL","MSFT"]}'
```

Expected output:

```json
{"ok":true}
```

### 4. Send stock ticks into Kafka

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock_ticks
PYTHONPATH=src python3 -m stockpicker.producer --symbols AAPL,MSFT,GOOG --rate 20 --seconds 10 --seed 42
```

Expected producer log shape:

```text
[producer] sending to kafka topic=stock_ticks bootstrap=localhost:9092
[producer] sent {...}
```

### 5. Watch the ingestor logs

```bash
docker compose logs -f api
```

Expected log shape:

```text
[api] ingestor thread started (shares in-memory storage with API)
[ingestor] consuming topic=stock_ticks bootstrap=kafka:29092
[ingestor] processed event ok=True
```

### 6. Query the API through the gateway

```bash
curl -s "http://localhost:8080/api/leaderboard?round=2026-01-10"

curl -s "http://localhost:8080/api/portfolio/PUT_USER_ID_HERE?round=2026-01-10"
```

Expected output shapes:

```json
{"round":"2026-01-10","entries":[...]}
```

```json
{"user_id":"PUT_USER_ID_HERE","round":"2026-01-10","symbols":["AAPL","MSFT"],"score":0.0}
```

## Load Test

The provided load test targets the gateway on port `8080`.

I run:

```bash
make load-test
```

This uses:

```bash
python3 scripts/load_test.py --url http://localhost:8080/api/leaderboard?round=2026-01-10 --requests 100 --concurrency 5
```

Important notes:

- this only works if I already started `docker compose up -d`
- the request goes through Nginx, not directly to FastAPI
- because the gateway is rate limited to `10 req/s` per IP, the default burst test can produce many `429` responses

If I want to test the FastAPI app without the gateway, I use:

```bash
python3 scripts/load_test.py --url "http://localhost:8000/api/leaderboard?round=2026-01-10" --requests 100 --concurrency 5
```

## Makefile vs Docker Compose

I use the Makefile when I want a fast local loop:

- `make install`
- `make test`
- `make run-api`
- `make run-producer`
- `make run-ingestor`
- `make load-test`
- `make verify`
- `make clean`

I use Docker Compose when I want the full project architecture:

- Kafka
- the gateway on `8080`
- the end-to-end producer -> Kafka -> ingestor -> API flow

## Troubleshooting

### `502 Bad Gateway`

This usually means the gateway container is up, but the API container is still starting.

Fix:

```bash
docker compose logs -f api gateway
```

Then I wait for the API to finish starting and try again.

### Producer cannot connect to Kafka

This usually means Kafka is not ready yet.

Fix:

- wait about 30 to 60 seconds after `docker compose up -d`
- check `docker compose ps`
- make sure I am using `localhost:9092` from the host machine

### Gateway returns many `429 Too Many Requests`

This is expected under burst traffic because the project requires a rate limit of `10 req/s` per IP.

### Restarting the API loses old users, picks, and prices

This is expected because the storage is in memory.

## Written Deliverables

- `README.md`
- `docs/DESIGN_NOTES.md` -- latency, throughput and reliability measurements with interpretation
