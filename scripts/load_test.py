#!/usr/bin/env python3
"""
Simple load test for the StockPicker API.

Sends a burst of HTTP requests to a target endpoint and reports
latency percentiles (p50, p95, p99) and throughput.

Usage:
    python scripts/load_test.py                          # defaults
    python scripts/load_test.py --url http://localhost:8080/api/leaderboard?round=2026-01-10
    python scripts/load_test.py --url http://localhost:8080/api/users --method POST \
        --body '{"name":"loadtest"}' --requests 200 --concurrency 10

Requirements: httpx (already in requirements.txt)
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx


def send_request(
    client: httpx.Client,
    method: str,
    url: str,
    body: dict | None,
) -> tuple[float, int]:
    """Send one request and return (elapsed_ms, status_code)."""
    start = time.perf_counter()
    if method == "GET":
        resp = client.get(url)
    else:
        resp = client.post(url, json=body)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, resp.status_code


def percentile(sorted_data: list[float], p: float) -> float:
    """Return the p-th percentile (0-100) from sorted data."""
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def run_load_test(
    url: str,
    method: str,
    body: dict | None,
    num_requests: int,
    concurrency: int,
) -> None:
    status_counts: dict[int, int] = {}
    latencies: list[float] = []

    print(f"Load test: {method} {url}")
    print(f"  requests={num_requests}  concurrency={concurrency}")
    if body:
        print(f"  body={json.dumps(body)}")
    print()

    wall_start = time.perf_counter()

    with httpx.Client(timeout=30.0) as client:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [
                pool.submit(send_request, client, method, url, body)
                for _ in range(num_requests)
            ]
            for future in as_completed(futures):
                elapsed_ms, status = future.result()
                latencies.append(elapsed_ms)
                status_counts[status] = status_counts.get(status, 0) + 1

    wall_elapsed = time.perf_counter() - wall_start

    latencies.sort()
    throughput = num_requests / wall_elapsed if wall_elapsed > 0 else 0

    print("--- Results ---")
    print(f"  Total requests : {num_requests}")
    print(f"  Wall clock     : {wall_elapsed:.2f}s")
    print(f"  Throughput     : {throughput:.1f} req/s")
    print()
    print(f"  Latency p50    : {percentile(latencies, 50):.1f} ms")
    print(f"  Latency p95    : {percentile(latencies, 95):.1f} ms")
    print(f"  Latency p99    : {percentile(latencies, 99):.1f} ms")
    print(f"  Latency min    : {latencies[0]:.1f} ms")
    print(f"  Latency max    : {latencies[-1]:.1f} ms")
    print(f"  Latency mean   : {statistics.mean(latencies):.1f} ms")
    print()
    print("  Status codes:")
    for code in sorted(status_counts):
        print(f"    {code}: {status_counts[code]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="StockPicker API load test")
    parser.add_argument(
        "--url",
        default="http://localhost:8080/api/leaderboard?round=2026-01-10",
        help="Target URL (default: leaderboard endpoint via gateway)",
    )
    parser.add_argument(
        "--method",
        choices=["GET", "POST"],
        default="GET",
        help="HTTP method (default: GET)",
    )
    parser.add_argument(
        "--body",
        default=None,
        help='JSON body for POST requests (e.g. \'{"name":"test"}\')',
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=100,
        help="Total number of requests to send (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent workers (default: 5)",
    )
    args = parser.parse_args()

    body = json.loads(args.body) if args.body else None

    run_load_test(
        url=args.url,
        method=args.method,
        body=body,
        num_requests=args.requests,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
