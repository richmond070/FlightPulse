"""
Phase 7 -- Load & Resilience Testing: fixture export.

Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf, section 10
("Create a replay mode so the system can be tested without depending on
OpenSky's live API limits. Replay previously collected observations at
controlled rates.")

This script pulls a *contiguous* time window from the real `raw_telemetry`
table (not a random sample) so the replay preserves genuine polling-cycle
structure -- burst shape, natural duplicate/redelivery patterns, and
realistic per-batch record counts -- rather than flattening them out the
way a random row sample would.

Each exported record's `payload` column is already a full canonical
TelemetryEvent (see ingestion/schemas.py), so it can be replayed straight
through POST /telemetry with no reshaping in replay/player.py.

Usage (from repo root, with PYTHONPATH set):

    export PYTHONPATH=$(pwd)                 # bash
    $env:PYTHONPATH = (Get-Location).Path    # PowerShell

    python replay/export_fixture.py --limit 24000
    python replay/export_fixture.py --start "2026-08-20T00:00:00Z" --end "2026-08-20T00:15:00Z"

Output: replay/fixtures/telemetry_sample.jsonl (one JSON object per line,
in ingested_at order, so replay/player.py can reconstruct realistic batches
just by reading the file top to bottom).
"""

import argparse
import json
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from worker.settings import DATABASE_URL

FIXTURE_DIR = Path(__file__).parent / "fixtures"
DEFAULT_OUTPUT = FIXTURE_DIR / "telemetry_sample.jsonl"

# Default slice size: a few real OpenSky poll cycles' worth of data.
# Per project notes, one poll of /states/all yields ~8,000 state vectors,
# so ~24,000 rows is roughly 3 poll cycles -- enough to exercise batching,
# burst shape, and any redelivery/duplicate behavior across cycle
# boundaries without needing the full table.
DEFAULT_LIMIT = 24000


def export_fixture(
    limit: int | None = DEFAULT_LIMIT,
    start: str | None = None,
    end: str | None = None,
    output: Path = DEFAULT_OUTPUT,
) -> int:
    """Export a contiguous window of raw_telemetry to a JSONL fixture.

    Returns the number of rows written.
    """
    where_clauses = []
    params: dict[str, object] = {}

    if start:
        where_clauses.append("ingested_at >= %(start)s")
        params["start"] = start
    if end:
        where_clauses.append("ingested_at <= %(end)s")
        params["end"] = end

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    limit_sql = "LIMIT %(limit)s" if limit else ""
    if limit:
        params["limit"] = limit

    # Ordered ascending by ingested_at so the fixture file is already in
    # chronological/poll-cycle order for replay/player.py to consume
    # sequentially.
    query = f"""
        SELECT
            ingestion_id,
            source,
            icao24,
            collector_id,
            payload,
            received_at,
            ingested_at,
            processed_at
        FROM raw_telemetry
        {where_sql}
        ORDER BY ingested_at ASC
        {limit_sql}
    """

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    row_count = 0
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur, output.open("w", encoding="utf-8") as f:
            cur.execute(query, params)
            for row in cur:
                # payload is already the canonical TelemetryEvent shape --
                # write it through untouched so replay/player.py can POST
                # it directly. Keep the row-level metadata alongside it
                # (not merged in) so export provenance isn't confused with
                # the event schema itself.
                record = {
                    "ingestion_id": str(row["ingestion_id"]),
                    "source": row["source"],
                    "icao24": row["icao24"],
                    "collector_id": row["collector_id"],
                    "payload": row["payload"],
                    "received_at": row["received_at"].isoformat(),
                    "ingested_at": row["ingested_at"].isoformat(),
                    "processed_at": (
                        row["processed_at"].isoformat()
                        if row["processed_at"]
                        else None
                    ),
                }
                f.write(json.dumps(record) + "\n")
                row_count += 1

    return row_count


def main():
    parser = argparse.ArgumentParser(
        description="Export a raw_telemetry window as a Phase 7 replay fixture."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max rows to export (default: {DEFAULT_LIMIT}). "
        "Ignored if you rely on --start/--end alone to bound the window.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="ISO8601 lower bound on ingested_at (optional).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="ISO8601 upper bound on ingested_at (optional).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT}).",
    )
    args = parser.parse_args()

    print(f"Exporting from raw_telemetry (limit={args.limit}, "
          f"start={args.start}, end={args.end}) -> {args.output}")

    try:
        count = export_fixture(
            limit=args.limit, start=args.start, end=args.end, output=args.output
        )
    except psycopg.OperationalError as e:
        print(f"ERROR: could not connect to Postgres: {e}", file=sys.stderr)
        print(
            "Is Postgres running? (docker compose up -d postgres)",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Wrote {count} record(s) to {args.output}")
    if count == 0:
        print(
            "WARNING: 0 rows exported -- check your --start/--end window "
            "or that raw_telemetry actually has data.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
