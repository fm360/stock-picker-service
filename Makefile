.PHONY: help install test run-api run-ingestor run-producer load-test clean verify

help:
	@echo "Common commands:"
	@echo "  make install     - install Python deps"
	@echo "  make test        - run unit tests (same entry point as Gradescope)"
	@echo "  make run-api     - run FastAPI app locally (no Docker required)"
	@echo "  make run-producer- run demo tick producer (stdout or Kafka if configured)"
	@echo "  make run-ingestor- run demo ingestor (Kafka if configured)"
	@echo "  make load-test   - run load test against the API (via gateway)"
	@echo "  make verify      - quick structural checks"
	@echo "  make clean       - remove caches"

install:
	python3 -m pip install --upgrade pip
	python3 -m pip install -r requirements.txt

test:
	python3 -m pytest -q

run-api:
	PYTHONPATH=src python3 -m uvicorn stockpicker.api:app --host 0.0.0.0 --port 8000 --reload

run-producer:
	PYTHONPATH=src python3 -m stockpicker.producer --symbols AAPL,MSFT,GOOG --rate 20 --seconds 10 --seed 123

run-ingestor:
	PYTHONPATH=src python3 -m stockpicker.ingestor

load-test:
	python3 scripts/load_test.py --url http://localhost:8080/api/leaderboard?round=2026-01-10 --requests 100 --concurrency 5

verify:
	@python3 -c "import pathlib; req=['src/stockpicker/sharding.py','src/stockpicker/api.py','src/stockpicker/scoring.py','src/stockpicker/ingestor.py','docker-compose.yml','services/gateway/nginx.conf','requirements.txt']; missing=[p for p in req if not pathlib.Path(p).exists()]; print('OK' if not missing else ('Missing: '+', '.join(missing))); raise SystemExit(0 if not missing else 1)"

clean:
	rm -rf .pytest_cache **/__pycache__ *.pyc



