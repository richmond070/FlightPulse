"""
Persistence hook used by worker/processor.py.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 9, Phase 5
("Create raw telemetry table. Implement batch inserts...").

Scope boundary: batched PostgreSQL inserts against
sql/001_raw_telemetry.sql are Phase 5 work and are NOT implemented here.
persist_batch() is a placeholder so Phase 4's job lifecycle
(queued -> active -> completed) is real and testable end-to-end without
a database. Phase 5 replaces the body of persist_batch() with actual
psycopg batched inserts; the function signature and PersistenceUnavailable
exception are the contract worker/processor.py already relies on, so
Phase 5 shouldn't need to touch processor.py.

write_to_dead_letter() IS live in Phase 4 -- it's how section 8's
"failed -> dead-letter handling" step is satisfied without needing
Postgres yet (see worker/settings.py: DEAD_LETTER_KEY).
"""

import json
import logging
import time

from ingestion.schemas import TelemetryEvent

logger = logging.getLogger("flightpulse.worker.persistence")


class PersistenceUnavailable(Exception):
    """Raised when the persistence layer can't be reached. Treated as a
    transient failure by worker/processor.py (retry/backoff applies)."""


async def persist_batch(events: list[TelemetryEvent]) -> None:
    """Phase 4 placeholder for Phase 5's batched PostgreSQL insert.

    Deliberately does not touch a database yet. Logs what *would* be
    persisted so the end-to-end job lifecycle can be exercised and
    verified now, per section 9's Phase 4 checklist ("run multiple
    workers and verify concurrent processing").
    """
    logger.info("(Phase 5 pending) would persist %d event(s)", len(events))


async def write_to_dead_letter(redis, dead_letter_key: str, batch_payload: dict, reason: str) -> None:
    """Push a permanently-failed batch onto a Redis list for later
    inspection (section 8: "failed -> dead-letter handling").

    Uses the same Redis connection arq already holds (ctx["redis"]) so no
    second connection needs to be opened from within a job.
    """
    record = {
        "reason": reason,
        "dead_lettered_at": time.time(),
        "batch": batch_payload,
    }
    await redis.rpush(dead_letter_key, json.dumps(record))
    logger.warning("Dead-lettered batch: %s", reason)
