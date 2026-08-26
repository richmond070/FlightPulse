"""
Phase 4 job processing.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 8, step 7
("Workers consume jobs, validate business rules, normalize nulls/units,
deduplicate events, and prepare database writes.") and section 9's Phase 4
checklist (retries, backoff, idempotency).

Scope boundary: this module prepares validated, deduplicated records and
hands them to worker/persistence.py. It deliberately stops short of
writing to PostgreSQL -- batched inserts and indexing are Phase 5 work
(section 9, Phase 5). Keeping that boundary means Phase 5 can fill in
persistence.py without reopening anything here.
"""

import logging

from arq import Retry
from pydantic import ValidationError

from ingestion.schemas import TelemetryBatch, TelemetryEvent
from worker import persistence
from worker.settings import DEAD_LETTER_KEY, RETRY_BACKOFF_BASE_SECONDS, RETRY_BACKOFF_MAX_SECONDS

logger = logging.getLogger("flightpulse.worker.processor")


def compute_backoff_seconds(attempt: int) -> float:
    """Exponential backoff with a cap.

    attempt is 1-indexed (the first retry is attempt=2, since attempt=1
    is the original run). Section 8: "Use retries for transient failures,
    exponential backoff, bounded attempts."
    """
    delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** max(attempt - 1, 0))
    return min(delay, RETRY_BACKOFF_MAX_SECONDS)


def _normalize_event(event: TelemetryEvent) -> TelemetryEvent:
    """Normalize nulls/units on a single event (section 8, step 7).

    Phase 4 scope keeps this intentionally light -- the collector's
    normalizer (collector/normalizer.py) already does the heavy lifting
    when converting raw OpenSky state vectors into TelemetryEvent. This
    is a second, defensive pass for anything that could still slip
    through (e.g. an on_ground flag missing where altitude implies it).
    """
    if event.on_ground is None and event.baro_altitude_m is not None:
        event.on_ground = event.baro_altitude_m <= 0
    return event


def _dedupe_events(events: list[TelemetryEvent]) -> list[TelemetryEvent]:
    """Drop duplicate events within a single batch, keyed on the same
    identity Phase 5's unique index will enforce durably
    (icao24 + last_contact + source), so a batch that contains the same
    state vector twice doesn't try to persist it twice.

    This is a same-batch safety net only. Cross-batch/cross-request
    deduplication is enforced durably in Phase 5 via the
    `uq_raw_telemetry_ingestion_id` unique index in
    sql/001_raw_telemetry.sql.
    """
    seen: set = set()
    deduped: list[TelemetryEvent] = []
    for event in events:
        key = (event.icao24, event.last_contact, event.source)
        if key in seen:
            logger.info(
                "Dropping in-batch duplicate icao24=%s last_contact=%s",
                event.icao24,
                event.last_contact,
            )
            continue
        seen.add(key)
        deduped.append(event)
    return deduped


async def process_telemetry_batch(ctx: dict, batch_payload: dict) -> dict:
    """arq job function. Registered by name in worker/consumer.py's
    WorkerSettings.functions and enqueued by name from
    ingestion/routes.py, so the two never need to import each other.

    ctx is arq's job context (contains ctx["job_try"], the 1-indexed
    attempt number, among other things).
    """
    attempt = ctx.get("job_try", 1)

    try:
        batch = TelemetryBatch.model_validate(batch_payload)
    except ValidationError as exc:
        # A payload that fails schema validation will never succeed on
        # retry -- it's a bad record, not a transient failure. Send it
        # straight to the dead letter destination instead of burning
        # through MAX_JOB_ATTEMPTS.
        logger.error("Schema validation failed, sending to dead letter: %s", exc)
        await persistence.write_to_dead_letter(
            ctx["redis"], DEAD_LETTER_KEY, batch_payload, reason=str(exc)
        )
        return {"status": "dead_lettered", "reason": "validation_error"}

    normalized = [_normalize_event(e) for e in batch.events]
    deduped = _dedupe_events(normalized)

    logger.info(
        "Processing batch attempt=%d received=%d after_dedupe=%d",
        attempt,
        len(batch.events),
        len(deduped),
    )

    try:
        # Phase 5 fills this in with real batched inserts. For Phase 4
        # this call exists so the job lifecycle (queued -> active ->
        # completed) is real and testable end-to-end, per section 9's
        # "run multiple workers and verify concurrent processing".
        await persistence.persist_batch(deduped)
    except persistence.PersistenceUnavailable as exc:
        # Treated as transient: the DB being briefly unreachable should
        # not dead-letter a perfectly good batch. Raising arq's Retry
        # with an explicit `defer` is how per-job exponential backoff
        # (section 8) is actually expressed in arq -- there's no separate
        # WorkerSettings-level backoff hook to plug into.
        delay = compute_backoff_seconds(attempt)
        logger.warning(
            "Persistence unavailable on attempt=%d, retrying in %.1fs: %s",
            attempt,
            delay,
            exc,
        )
        raise Retry(defer=delay) from exc

    return {"status": "completed", "accepted": len(deduped)}
