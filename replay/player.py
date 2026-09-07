"""
Phase 7 -- Load & Resilience Testing: replay engine.

Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf, section 10
("Replay previously collected observations at controlled rates":
1x baseline, 5x baseline, 10x baseline, burst traffic, sustained traffic.)

This module reads the JSONL fixture produced by replay/export_fixture.py
and re-plays it into the *real* pipeline -- load balancer -> FastAPI ->
queue -> worker -> Postgres -- exactly the way collector/producer.py does
for live OpenSky data. It deliberately reuses collector.producer.send_batch
rather than reimplementing the POST/Idempotency-Key logic, so replay
traffic is indistinguishable from real collector traffic as far as the
rest of the pipeline is concerned.

Two identity modes, both needed for different Phase 7 KPIs (continuation
doc section 11 -- "Duplicate rate"):

  --mode replay    (default) Keep each record's original ingestion_id.
                   Replaying the same fixture twice in this mode is a
                   genuine duplicate-detection test: the second run
                   should insert 0 new rows, proving
                   uq_raw_telemetry_ingestion_id does its job.

  --mode fresh     Recompute ingestion_id the same way
                   collector/normalizer.py does (uuid5 over
                   source:icao24:last_contact) but stamp a *new*
                   ingested_at, so the batch is treated as newly-observed
                   data. Use this for pure throughput/latency testing
                   where you don't want every record already sitting in
                   raw_telemetry from a prior test run.

Usage (from repo root, PYTHONPATH set):

    python replay/player.py --speed 5 --batch-size 8000
    python replay/player.py --speed 10 --mode fresh
    python replay/player.py --burst --batch-size 20000   # one big burst, no pacing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Reuse the collector's actual send path -- same POST, same
# Idempotency-Key header behavior, same target URL resolution -- so
# replay traffic exercises the pipeline identically to live collection.
from collector.producer import TELEMETRY_TARGET_URL, send_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("flightpulse.replay.player")

FIXTURE_DIR = Path(__file__).parent / "fixtures"
DEFAULT_FIXTURE = FIXTURE_DIR / "telemetry_sample.jsonl"

# Same namespace collector/normalizer.py uses, so --mode fresh produces
# ingestion_ids that follow the exact same semantics as a real collector
# cycle, not an ad-hoc scheme that only replay understands.
_INGESTION_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "flightpulse.opensky.ingestion_id")


def _fresh_ingestion_id(source: str, icao24: str, last_contact, run_salt: str) -> str:
    """Compute a fresh-mode ingestion_id.

    run_salt (a random value minted once per run_replay() invocation) is
    mixed into the hash alongside the event's own identity fields. Without
    it, this function is a pure hash of source+icao24+last_contact --
    fields that never change since they come from a static fixture file
    -- so every run of --mode fresh against the same fixture would
    produce *identical* ingestion_ids to the very first run, ever. That
    turned out to be a real Phase 7 finding: after the first successful
    fresh-mode run, every subsequent run showed "0 newly inserted, 8000
    skipped as duplicate" and looked like a failure, when the pipeline
    was actually working correctly -- it was "fresh" mode that wasn't
    living up to its name. Salting per run restores repeatable
    throughput testing: each invocation mints genuinely new IDs
    regardless of how many times the fixture has been replayed before.
    """
    if last_contact is None:
        return str(uuid.uuid4())
    identity = f"{run_salt}:{source}:{icao24}:{last_contact}"
    return str(uuid.uuid5(_INGESTION_ID_NAMESPACE, identity))


def load_fixture(path: Path) -> list[dict]:
    """Load the exported fixture, returning the list of raw
    TelemetryEvent-shaped payloads in the order export_fixture.py wrote
    them (i.e. chronological, per original ingested_at)."""
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            records.append(row["payload"])
    return records


def chunk(events: list[dict], size: int):
    for i in range(0, len(events), size):
        yield events[i : i + size]


def prepare_batch(events: list[dict], mode: str, now_iso: str, run_salt: str = "") -> list[dict]:
    """Apply identity-mode transforms to a batch before sending.

    mode='replay': events pass through unchanged -- same ingestion_id as
    originally captured, so re-running the same fixture is a genuine
    duplicate-detection test.

    mode='fresh': ingestion_id is recomputed via the same uuid5 scheme
    the real collector uses (now salted per-run -- see
    _fresh_ingestion_id's docstring for why that matters), and
    ingested_at is stamped to "now" so the batch is treated as
    newly-observed rather than a redelivery.
    """
    if mode == "replay":
        return events

    prepared = []
    for ev in events:
        ev = dict(ev)  # don't mutate the loaded fixture in place
        ev["ingestion_id"] = _fresh_ingestion_id(
            ev.get("source", "opensky"), ev.get("icao24"), ev.get("last_contact"),
            run_salt,
        )
        ev["ingested_at"] = now_iso
        prepared.append(ev)
    return prepared


def run_replay(
    fixture_path: Path,
    speed: float,
    batch_size: int,
    mode: str,
    burst: bool,
    poll_interval_seconds: float,
    concurrency: int = 3,
) -> dict:
    """Replay the fixture into the pipeline.

    Pacing model: under normal (non-burst) replay, batches are *submitted*
    to a thread pool at poll_interval_seconds / speed apart -- speed=1
    mimics the collector's real polling cadence, speed=5/10 compress that
    interval to simulate 5x/10x baseline load per continuation doc
    section 10. --burst ignores pacing entirely and submits every batch
    back-to-back, for the "burst traffic" scenario.

    concurrency controls how many batches can be in flight to the load
    balancer at once (default 3, matching a typical 3-backend local
    setup per the README). This matters: earlier Phase 7 testing found
    that sending batches strictly one-at-a-time meant only one backend
    was ever busy regardless of how many were running, since round-robin
    routing only spreads load across requests that arrive concurrently.
    Submitting several batches without waiting for each one to finish is
    what actually lets the load balancer fan them out across multiple
    backends at once.

    Returns a small summary dict (batches sent, events sent, wall-clock
    duration, failures) -- report.py in Step 3 builds on this alongside
    the load balancer's own /lb-metrics for the full KPI picture.
    """
    if not fixture_path.exists():
        logger.error(
            "Fixture not found at %s -- run replay/export_fixture.py first.",
            fixture_path,
        )
        sys.exit(1)

    events = load_fixture(fixture_path)
    if not events:
        logger.error("Fixture at %s is empty.", fixture_path)
        sys.exit(1)

    run_salt = str(uuid.uuid4()) if mode == "fresh" else ""

    logger.info(
        "Loaded %d events from %s. Target=%s speed=%sx batch_size=%d mode=%s "
        "burst=%s concurrency=%d%s",
        len(events), fixture_path, TELEMETRY_TARGET_URL, speed, batch_size, mode,
        burst, concurrency,
        f" run_salt={run_salt}" if run_salt else "",
    )

    delay_seconds = 0.0 if burst else max(poll_interval_seconds / speed, 0.0)

    from datetime import datetime, timezone

    batches_sent = 0
    events_sent = 0
    failures = 0
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        for batch in chunk(events, batch_size):
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            prepared = prepare_batch(batch, mode=mode, now_iso=now_iso, run_salt=run_salt)

            # Submit and move on immediately -- don't wait for this
            # batch's response before starting the next one. This is
            # what actually produces concurrent load instead of one
            # request at a time.
            future = executor.submit(send_batch, prepared)
            futures[future] = len(prepared)

            if delay_seconds > 0:
                time.sleep(delay_seconds)

        for future in as_completed(futures):
            batch_len = futures[future]
            batches_sent += 1
            try:
                ok = future.result()
            except Exception:
                logger.exception("Batch raised unexpectedly")
                ok = False
            if ok:
                events_sent += batch_len
            else:
                failures += 1
                logger.warning(
                    "Batch %d failed to send (will not retry at the replay "
                    "layer -- the load balancer's own retry/idempotency "
                    "handling, if any, applies as normal).",
                    batches_sent,
                )

    duration = time.monotonic() - start
    summary = {
        "batches_sent": batches_sent,
        "events_sent": events_sent,
        "failed_batches": failures,
        "duration_seconds": round(duration, 2),
        "effective_events_per_sec": round(events_sent / duration, 1) if duration > 0 else None,
        "speed": speed,
        "mode": mode,
        "burst": burst,
        "concurrency": concurrency,
    }

    logger.info("Replay complete: %s", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Phase 7 replay engine -- replays a captured telemetry "
        "fixture into the live FlightPulse pipeline at a controllable rate."
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_FIXTURE,
        help=f"Path to the JSONL fixture (default: {DEFAULT_FIXTURE}).",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Speed multiplier vs. baseline poll interval (1, 5, 10, ...). "
        "Ignored if --burst is set.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8000,
        help="Events per batch sent to /telemetry (default: 8000, matching "
        "a real OpenSky poll cycle size).",
    )
    parser.add_argument(
        "--mode",
        choices=["replay", "fresh"],
        default="replay",
        help="'replay' keeps original ingestion_ids (duplicate-detection "
        "test). 'fresh' mints new ones (pure throughput test).",
    )
    parser.add_argument(
        "--burst",
        action="store_true",
        help="Send all batches back-to-back with no pacing delay "
        "(the 'burst traffic' scenario from the continuation doc).",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30.0,
        help="Baseline poll interval in seconds before the --speed "
        "multiplier is applied (default: 30, matching "
        "POLL_INTERVAL_SECONDS's own default).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="How many batches may be in flight to the load balancer at "
        "once (default: 3, matching a typical 3-backend local setup). "
        "This is what actually lets round-robin spread load across "
        "multiple backends -- sending one batch at a time keeps only "
        "one backend busy regardless of how many are running.",
    )
    args = parser.parse_args()

    run_replay(
        fixture_path=args.fixture,
        speed=args.speed,
        batch_size=args.batch_size,
        mode=args.mode,
        burst=args.burst,
        poll_interval_seconds=args.poll_interval,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
