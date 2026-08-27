"""
Persistence layer used by worker/processor.py.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 5
(raw_telemetry is the immutable-ish landing table; dedup based on source
identifiers + timestamps) and section 9, Phase 5 checklist ("Implement
batch inserts", "Record ingestion and processing timestamps", "Verify
duplicate handling").

Writes against sql/001_raw_telemetry.sql's existing raw_telemetry table
(Phase 1) plus the processed_at column added in
sql/002_add_processed_at.sql (Phase 5). No new table is created here --
Phase 1 already defined the landing schema; this module just fills it in.

write_to_dead_letter() is unchanged from Phase 4 -- it doesn't touch
Postgres and is included here as it was before, since it lives in this
module regardless of persistence backend.
"""

import json
import logging
import time

import psycopg
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from ingestion.schemas import TelemetryEvent
from worker.settings import DATABASE_URL, DB_POOL_CONNECT_TIMEOUT_SECONDS, DB_POOL_MAX_SIZE, DB_POOL_MIN_SIZE

logger = logging.getLogger("flightpulse.worker.persistence")


class PersistenceUnavailable(Exception):
    """Raised when the persistence layer can't be reached. Treated as a
    transient failure by worker/processor.py (retry/backoff applies)."""


# Lazily-created, process-wide async connection pool. Created on first use
# (not at import time) so importing this module doesn't require a live
# Postgres connection -- mirrors the arq pool pattern in ingestion/routes.py.
_pool: AsyncConnectionPool | None = None


async def _get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=DATABASE_URL,
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            timeout=DB_POOL_CONNECT_TIMEOUT_SECONDS,
            open=False,
        )
        await _pool.open()
    return _pool


_INSERT_SQL = """
    INSERT INTO raw_telemetry
        (ingestion_id, source, icao24, payload, ingested_at, processed_at)
    VALUES
        (%(ingestion_id)s, %(source)s, %(icao24)s, %(payload)s, %(ingested_at)s, now())
    ON CONFLICT (ingestion_id) DO NOTHING
    RETURNING ingestion_id
"""


def _event_to_row(event: TelemetryEvent) -> dict:
    """Map a TelemetryEvent onto raw_telemetry's columns.

    payload retains the full normalized event as JSONB (section 5:
    raw_telemetry's purpose is "payload, source, received_at" -- the full
    record, not just the columns we've chosen to index on).
    """
    return {
        "ingestion_id": event.ingestion_id,
        "source": event.source,
        "icao24": event.icao24,
        "payload": json.dumps(event.model_dump()),
        "ingested_at": event.ingested_at,
    }


async def persist_batch(events: list[TelemetryEvent]) -> int:
    """Batch-insert events into raw_telemetry.

    Uses ON CONFLICT (ingestion_id) DO NOTHING against the unique index
    already defined in sql/001_raw_telemetry.sql -- this is what makes a
    redelivered arq job (at-least-once delivery), a load-balancer retry
    (idempotency key), or -- as of the deterministic ingestion_id in
    collector/normalizer.py -- the *same real-world observation* polled
    twice, all safe to persist without creating duplicate rows. This is
    the "verify duplicate handling" checklist item in practice.

    Timing: logs elapsed insert time and records/second per batch, per
    FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf section
    5.3 ("measure insert latency and records/second before optimizing
    further") and section 11's "Records processed per second" KPI. This
    is deliberately just a log line for now, not a metrics backend --
    Section 9's observability work (latency, throughput, queue depth)
    is a later, dedicated step, and this gives it real numbers to start
    from rather than a guess.

    Returns the number of rows actually inserted (excludes rows skipped
    by ON CONFLICT), so callers/logs can distinguish "processed" from
    "newly persisted".

    Raises PersistenceUnavailable on connection-level failures so
    worker/processor.py's existing retry/backoff handling applies
    unchanged -- this function's contract with processor.py doesn't
    change from the Phase 4 stub.
    """
    if not events:
        return 0

    rows = [_event_to_row(e) for e in events]
    start = time.monotonic()

    try:
        pool = await _get_pool()
        newly_inserted = 0
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # One statement per row rather than executemany: batches
                # are kept compact per section 8 ("pass a batch reference
                # or compact normalized records"), so the per-row round
                # trip cost is small, and RETURNING per row is the
                # simplest correct way to count exactly how many rows
                # survived ON CONFLICT DO NOTHING (executemany's batched
                # RETURNING semantics vary enough across drivers that
                # they're not worth the complexity at this batch size).
                for row in rows:
                    await cur.execute(_INSERT_SQL, row)
                    if await cur.fetchone() is not None:
                        newly_inserted += 1
            await conn.commit()

        elapsed = time.monotonic() - start
        skipped = len(rows) - newly_inserted
        records_per_second = len(rows) / elapsed if elapsed > 0 else float("inf")
        logger.info(
            "Persisted batch: %d event(s) submitted, %d newly inserted, %d skipped as duplicate "
            "(%.2fs elapsed, %.0f records/sec)",
            len(rows),
            newly_inserted,
            skipped,
            elapsed,
            records_per_second,
        )
        return newly_inserted
    except (psycopg.OperationalError, PoolTimeout, OSError) as exc:
        elapsed = time.monotonic() - start
        logger.warning("Persistence failed after %.2fs for %d event(s): %s", elapsed, len(rows), exc)
        raise PersistenceUnavailable(str(exc)) from exc


async def write_to_dead_letter(redis, dead_letter_key: str, batch_payload: dict, reason: str) -> None:
    """Push a permanently-failed batch onto a Redis list for later
    inspection (section 8: "failed -> dead-letter handling").

    Unchanged from Phase 4 -- dead-lettering doesn't depend on Postgres.
    """
    record = {
        "reason": reason,
        "dead_lettered_at": time.time(),
        "batch": batch_payload,
    }
    await redis.rpush(dead_letter_key, json.dumps(record))
    logger.warning("Dead-lettered batch: %s", reason)
