"""
Ingestion routes: POST /telemetry, GET /health, GET /version.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 7
(FastAPI Ingestion Service) and section 6 Phase C ("use an idempotency
key so retries cannot create duplicate records").

Phase 1 scope: validate the incoming batch and return an accepted
response. No queue enqueueing and no database writes yet — those are
wired in during Phase 4 (Queue) and Phase 5 (Persistence).

Phase C addition: accept an optional Idempotency-Key header. If a
request with a previously-seen key arrives again (i.e. the load balancer
retried it after a failed first attempt), return the original response
instead of reprocessing — this is what makes a load-balancer retry safe
for a non-idempotent method like POST.

NOTE: this in-memory idempotency cache is a Phase-1-through-3 placeholder.
It is per-process and not persisted, so it only protects against retries
that land on the *same* ingestion instance and does not survive a
restart. The durable version lands in Phase 5, backed by the
`uq_raw_telemetry_ingestion_id` unique index in sql/001_raw_telemetry.sql.
"""

import threading

from arq import create_pool
from fastapi import APIRouter, Header
from typing import Optional

from ingestion.schemas import TelemetryBatch, IngestResponse
from worker.settings import QUEUE_NAME, get_redis_settings

router = APIRouter()

APP_VERSION = "0.1.0"

_idempotency_lock = threading.Lock()
_idempotency_cache: dict[str, IngestResponse] = {}

# Lazily-created, process-wide arq connection pool. Created on first use
# rather than at import time so importing this module (e.g. in tests)
# doesn't require a live Redis connection.
_arq_pool = None
_arq_pool_lock = threading.Lock()


async def _get_arq_pool():
    global _arq_pool
    if _arq_pool is None:
        with _arq_pool_lock:
            if _arq_pool is None:
                _arq_pool = await create_pool(get_redis_settings())
    return _arq_pool


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/version")
async def version():
    return {"version": APP_VERSION}


@router.post("/telemetry", response_model=IngestResponse)
async def post_telemetry(
    batch: TelemetryBatch,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    if idempotency_key:
        with _idempotency_lock:
            cached = _idempotency_cache.get(idempotency_key)
        if cached is not None:
            return cached

    # Phase 4: enqueue the whole batch as a single compact job rather than
    # one job per event (section 8: "keep the queue payload compact; pass
    # a batch reference or compact normalized records rather than huge
    # raw payloads"). The batch is already small, normalized JSON at this
    # point, so we pass it directly instead of writing it somewhere first
    # just to pass a reference — that indirection can be revisited if
    # batches grow large enough to matter.
    #
    # This request handler stays lightweight per section 7 ("avoid doing
    # database-heavy work inside the request handler") — enqueueing only,
    # no DB writes happen here.
    ingestion_ids = [event.ingestion_id for event in batch.events]

    pool = await _get_arq_pool()
    await pool.enqueue_job(
        "process_telemetry_batch",
        batch.model_dump(),
        _queue_name=QUEUE_NAME,
    )

    response = IngestResponse(
        status="accepted",
        accepted=len(batch.events),
        ingestion_ids=ingestion_ids,
    )

    if idempotency_key:
        with _idempotency_lock:
            _idempotency_cache[idempotency_key] = response

    return response
