"""
Phase 7 -- Load & Resilience Testing: KPI report.

Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
section 11 (KPIs to capture for Phase 7):
  records/sec, end-to-end freshness, queue latency, API latency,
  failed-job rate, recovery time, duplicate rate, invalid-record rate,
  dbt pass rate.

This script pulls each of those from the source that already computes it
correctly, rather than recomputing anything in Python:

  - API latency (p50/p95/avg), request/failure counts
        -> load_balancer's GET /lb-metrics (load_balancer/metrics.py)
  - End-to-end freshness (p50/p95 telemetry_age_seconds), duplicate rate,
    invalid-record rate
        -> dbt's mart_telemetry_quality (Phase 6), queried directly
  - dbt test pass rate
        -> dbt's own run_results.json, produced by `dbt test`
           (per dbt/README.md's own note: "dbt's own PASS/WARN/ERROR
           test-run output is the source for the 'dbt test pass rate'
           KPI -- it isn't duplicated into this mart.")

Records/sec and recovery time aren't queried here -- they come directly
from replay/player.py's own summary dict (records/sec) and from manually
timing a fault-injection scenario (recovery time is "how long until the
system serves correctly again after we broke it", which isn't a single
query -- see docs/phase7_load_testing.md for how each fault-injection
run's recovery time is captured).

Usage (from repo root, PYTHONPATH set, pipeline running):

    python replay/report.py
    python replay/report.py --run-dbt-tests   # also runs `dbt test` fresh
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import psycopg
import requests
from psycopg.rows import dict_row

from worker.settings import DATABASE_URL

LB_METRICS_URL = "http://localhost:8080/lb-metrics"
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"
DBT_RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"


def fetch_lb_metrics() -> dict | None:
    """Pull API latency/throughput KPIs straight from the load balancer's
    own metrics endpoint -- no recomputation, just surfacing what
    load_balancer/metrics.py already tracks."""
    try:
        resp = requests.get(LB_METRICS_URL, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"WARNING: could not reach {LB_METRICS_URL}: {e}", file=sys.stderr)
        return None


def fetch_telemetry_quality() -> dict | None:
    """Pull the most recent hour's row from mart_telemetry_quality --
    freshness percentiles, duplicate rate, and invalid-record counts are
    already computed there by dbt (Phase 6), so this just reads the
    latest result rather than recalculating anything.
    """
    query = """
        SELECT
            observation_date,
            observation_hour,
            total_observations,
            missing_callsign_count,
            missing_origin_country_count,
            missing_position_count,
            invalid_latitude_count,
            invalid_longitude_count,
            invalid_velocity_count,
            retry_duplicate_count,
            avg_telemetry_age_seconds,
            max_telemetry_age_seconds,
            p50_telemetry_age_seconds,
            p95_telemetry_age_seconds,
            extraction_cycles,
            failed_extraction_cycles
        FROM mart_telemetry_quality
        ORDER BY observation_date DESC, observation_hour DESC
        LIMIT 1
    """
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return dict(row) if row else None
    except psycopg.OperationalError as e:
        print(f"WARNING: could not query mart_telemetry_quality: {e}", file=sys.stderr)
        print(
            "Has `dbt run` been executed against current data? "
            "The mart won't exist/refresh until it has.",
            file=sys.stderr,
        )
        return None
    except psycopg.errors.UndefinedTable:
        print(
            "WARNING: mart_telemetry_quality doesn't exist yet -- run "
            "`dbt run` from the dbt/ directory first.",
            file=sys.stderr,
        )
        return None


def run_dbt_tests() -> None:
    """Actually invoke `dbt test`, so run_results.json reflects the
    current data rather than whatever the last manual `dbt test` run
    happened to leave behind."""
    print("Running `dbt test` (this may take a moment)...")
    result = subprocess.run(
        ["dbt", "test"],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout[-2000:])  # dbt's own summary line is at the end
    if result.returncode not in (0, 1):
        # dbt exits 1 when tests fail (that's still a valid result to
        # report), but other codes mean something more fundamental broke
        # (e.g. can't connect, project misconfigured).
        print(f"WARNING: `dbt test` exited with code {result.returncode}", file=sys.stderr)
        print(result.stderr[-1000:], file=sys.stderr)


def fetch_dbt_test_pass_rate() -> dict | None:
    """Parse dbt's own run_results.json for the test pass rate -- this
    is dbt's own PASS/WARN/ERROR output, not a recomputation. Per
    dbt/README.md's own note, this is deliberately not duplicated into
    any mart; it's read directly from dbt's artifact instead.
    """
    if not DBT_RUN_RESULTS_PATH.exists():
        print(
            f"WARNING: {DBT_RUN_RESULTS_PATH} not found -- run `dbt test` "
            "at least once (or pass --run-dbt-tests) before this can "
            "report a pass rate.",
            file=sys.stderr,
        )
        return None

    with DBT_RUN_RESULTS_PATH.open("r", encoding="utf-8") as f:
        run_results = json.load(f)

    results = run_results.get("results", [])
    test_results = [r for r in results if r.get("unique_id", "").startswith("test.")]
    if not test_results:
        print(
            "WARNING: run_results.json has no test results -- was the "
            "last `dbt` invocation a `dbt run` rather than `dbt test`?",
            file=sys.stderr,
        )
        return None

    passed = sum(1 for r in test_results if r.get("status") == "pass")
    total = len(test_results)
    return {
        "tests_passed": passed,
        "tests_total": total,
        "pass_rate_pct": round(100 * passed / total, 1) if total else None,
        "failing_tests": [
            r["unique_id"] for r in test_results if r.get("status") != "pass"
        ],
    }


def print_report(lb_metrics, quality, dbt_tests) -> None:
    print("\n" + "=" * 60)
    print("FlightPulse Phase 7 -- KPI Report")
    print("=" * 60)

    print("\n-- API / Load Balancer (load_balancer/metrics.py) --")
    if lb_metrics:
        print(f"  Total requests:       {lb_metrics['total_requests']}")
        print(f"  Successful:           {lb_metrics['successful_requests']}")
        print(f"  Failed:               {lb_metrics['failed_requests']}")
        print(f"  Retried:              {lb_metrics['retried_requests']}")
        print(f"  Avg latency:          {lb_metrics['avg_latency_ms']} ms")
        print(f"  p50 latency:          {lb_metrics['p50_latency_ms']} ms")
        print(f"  p95 latency:          {lb_metrics['p95_latency_ms']} ms")
        print(f"  p99 latency:          {lb_metrics['p99_latency_ms']} ms")
        print(f"  Active backends:      {lb_metrics['active_backend_count']}")
        failed_rate = (
            round(100 * lb_metrics["failed_requests"] / lb_metrics["total_requests"], 2)
            if lb_metrics["total_requests"] else 0.0
        )
        print(f"  Failed-request rate:  {failed_rate}%")
    else:
        print("  (unavailable -- is the load balancer running on :8080?)")

    print("\n-- Telemetry Quality (dbt mart_telemetry_quality, most recent hour) --")
    if quality:
        print(f"  Observation hour:     {quality['observation_date']} {quality['observation_hour']}:00")
        print(f"  Total observations:   {quality['total_observations']}")
        print(f"  Avg freshness:        {quality['avg_telemetry_age_seconds']} s")
        print(f"  p50 freshness:        {quality['p50_telemetry_age_seconds']} s")
        print(f"  p95 freshness:        {quality['p95_telemetry_age_seconds']} s")
        print(f"  Max freshness:        {quality['max_telemetry_age_seconds']} s")
        print(f"  Retry-duplicate count:{quality['retry_duplicate_count']}")
        invalid_total = (
            quality["invalid_latitude_count"]
            + quality["invalid_longitude_count"]
            + quality["invalid_velocity_count"]
        )
        invalid_rate = (
            round(100 * invalid_total / quality["total_observations"], 3)
            if quality["total_observations"] else 0.0
        )
        print(f"  Invalid-record rate:  {invalid_rate}%")
        print(f"  Failed extraction cycles: {quality['failed_extraction_cycles']} / {quality['extraction_cycles']}")
    else:
        print("  (unavailable -- see warning above)")

    print("\n-- dbt Test Pass Rate (dbt's own run_results.json) --")
    if dbt_tests:
        print(f"  Passed:               {dbt_tests['tests_passed']} / {dbt_tests['tests_total']}")
        print(f"  Pass rate:            {dbt_tests['pass_rate_pct']}%")
        if dbt_tests["failing_tests"]:
            print(f"  Failing: {dbt_tests['failing_tests']}")
    else:
        print("  (unavailable -- see warning above)")

    print("\n" + "=" * 60)
    print(
        "Note: records/sec comes from replay/player.py's own summary "
        "output; recovery time is measured manually per fault-injection "
        "scenario. Both are documented in docs/phase7_load_testing.md, "
        "not this report."
    )
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Phase 7 KPI report -- pulls latency, freshness, "
        "duplicate/invalid rate, and dbt test pass rate from their "
        "respective sources of truth."
    )
    parser.add_argument(
        "--run-dbt-tests",
        action="store_true",
        help="Run `dbt test` fresh before reading the pass rate, instead "
        "of reading whatever run_results.json already has on disk.",
    )
    args = parser.parse_args()

    if args.run_dbt_tests:
        run_dbt_tests()

    lb_metrics = fetch_lb_metrics()
    quality = fetch_telemetry_quality()
    dbt_tests = fetch_dbt_test_pass_rate()

    print_report(lb_metrics, quality, dbt_tests)


if __name__ == "__main__":
    main()
