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
from psycopg import sql
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


_INSERT_COLUMNS = ("ingestion_id", "source", "icao24", "collector_id", "payload", "ingested_at")

# Rows per multi-row INSERT statement. Chosen so a full 8000-event batch
# needs only ~4 round trips instead of 8000 (one per row, the original
# Phase 5 approach) -- see the Phase 7 load-test finding this replaces:
# under 10x replay load, row-by-row inserts sometimes took 40-48s for an
# 8000-row batch, exceeding ARQ_JOB_TIMEOUT_SECONDS and causing arq to
# kill the job mid-write, losing the batch entirely.
# 2000 rows * 6 params/row = 12000 placeholders per statement, safely
# under PostgreSQL's ~65535-parameter-per-statement limit.
_INSERT_CHUNK_SIZE = 2000


def _build_multi_row_insert(num_rows: int) -> sql.Composed:
    """Build a single INSERT ... VALUES (..), (..), ... statement for
    num_rows rows, keeping ON CONFLICT DO NOTHING + RETURNING semantics
    identical to the original per-row statement -- just batched so
    Postgres round trips scale with chunk count, not event count."""
    value_group = sql.SQL("({}, now())").format(
        sql.SQL(", ").join(sql.Placeholder() * len(_INSERT_COLUMNS))
    )
    all_value_groups = sql.SQL(", ").join([value_group] * num_rows)
    return sql.SQL(
        "INSERT INTO raw_telemetry ({columns}, processed_at) "
        "VALUES {values} "
        "ON CONFLICT (ingestion_id) DO NOTHING "
        "RETURNING ingestion_id"
    ).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in _INSERT_COLUMNS),
        values=all_value_groups,
    )


def _event_to_row(event: TelemetryEvent) -> tuple:
    """Map a TelemetryEvent onto raw_telemetry's columns, as a positional
    tuple matching _INSERT_COLUMNS's order -- needed for the flattened
    multi-row VALUES statement built by _build_multi_row_insert.

    payload retains the full normalized event as JSONB (section 5:
    raw_telemetry's purpose is "payload, source, received_at" -- the full
    record, not just the columns we've chosen to index on).

    collector_id is promoted to its own column (sql/004_add_collector_id.sql)
    since it's record-provenance metadata, the same category as source and
    icao24 -- not a flight measurement. Flight-measurement fields
    (latitude, velocity, altitude, etc.) deliberately stay JSONB-only here;
    typing/promoting those is dbt's stg_opensky_states job in Phase 6, per
    the continuation doc's own "keep source-oriented storage separate from
    analytics models" instruction (section 3, Load).
    """
    return (
        event.ingestion_id,
        event.source,
        event.icao24,
        event.collector_id,
        json.dumps(event.model_dump()),
        event.ingested_at,
    )


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

    # Sort rows by ingestion_id (first column of _INSERT_COLUMNS) before
    # inserting. Phase 7 finding: concurrent batches inserting into
    # raw_telemetry occasionally hit "deadlock detected" from Postgres --
    # arq's own retry/backoff already recovers from this cleanly (see
    # worker/processor.py), so it wasn't causing data loss, but it's
    # avoidable. Deadlocks like this happen when concurrent transactions
    # acquire the same unique-index locks in different orders; sorting
    # every transaction's rows into the same order before inserting means
    # concurrent batches always approach shared index entries in the same
    # sequence, which is the standard fix for this class of deadlock.
    rows.sort(key=lambda row: row[0])
    start = time.monotonic()

    try:
        pool = await _get_pool()
        newly_inserted = 0
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # Multi-row INSERT in chunks of _INSERT_CHUNK_SIZE rather
                # than one statement per row. The original per-row
                # approach caused a real Phase 7 load-test failure: at
                # 10x replay speed, 8000 individual round trips per batch
                # sometimes took 40-48s, exceeding ARQ_JOB_TIMEOUT_SECONDS
                # and causing arq to kill the job mid-write -- losing the
                # batch entirely (confirmed: a fresh-mode 24000-event
                # replay run left only 8000 rows persisted). Chunking
                # keeps ON CONFLICT DO NOTHING + RETURNING semantics
                # identical, just batched, cutting round trips from
                # thousands to single digits per batch.
                for i in range(0, len(rows), _INSERT_CHUNK_SIZE):
                    chunk_rows = rows[i : i + _INSERT_CHUNK_SIZE]
                    stmt = _build_multi_row_insert(len(chunk_rows))
                    flat_params = [value for row in chunk_rows for value in row]
                    await cur.execute(stmt, flat_params)
                    returned = await cur.fetchall()
                    newly_inserted += len(returned)
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


_EXTRACTION_LOG_INSERT_SQL = """
    INSERT INTO extraction_log
        (request_id, source, collector_id, collector_version, request_scope,
         extraction_started_at, extraction_completed_at,
         source_observation_time, record_count, success, error_message)
    VALUES
        (%(request_id)s, %(source)s, %(collector_id)s, %(collector_version)s,
         %(request_scope)s, %(extraction_started_at)s, %(extraction_completed_at)s,
         to_timestamp(%(source_observation_time)s), %(record_count)s,
         %(success)s, %(error_message)s)
    ON CONFLICT (request_id) DO NOTHING
"""


async def persist_extraction_log(entry: dict) -> None:
    """Write one extraction-cycle record to extraction_log.

    Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
    section 3 (Extract-stage metadata) -- see sql/003_extraction_log.sql
    for the table this writes to and worker/processor.py's
    process_extraction_log for how this gets called.

    ON CONFLICT (request_id) DO NOTHING gives this the same redelivery
    safety as persist_batch(), for the same reason: arq's at-least-once
    delivery could run this job twice.

    Raises PersistenceUnavailable on connection failure, same contract
    as persist_batch(), so the caller's retry/backoff is unchanged.
    """
    try:
        pool = await _get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_EXTRACTION_LOG_INSERT_SQL, entry)
            await conn.commit()
        logger.info(
            "Logged extraction cycle request_id=%s success=%s record_count=%s",
            entry.get("request_id"), entry.get("success"), entry.get("record_count"),
        )
    except (psycopg.OperationalError, PoolTimeout, OSError) as exc:
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
