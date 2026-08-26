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

from fastapi import APIRouter, Header
from typing import Optional

from ingestion.schemas import TelemetryBatch, IngestResponse

router = APIRouter()

APP_VERSION = "0.1.0"

_idempotency_lock = threading.Lock()
_idempotency_cache: dict[str, IngestResponse] = {}


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

    # Phase 1: validation only. TODO (Phase 4): enqueue each event as a
    # job on the async queue instead of just echoing back what was accepted.
    ingestion_ids = [event.ingestion_id for event in batch.events]
    response = IngestResponse(
        status="accepted",
        accepted=len(batch.events),
        ingestion_ids=ingestion_ids,
    )

    if idempotency_key:
        with _idempotency_lock:
            _idempotency_cache[idempotency_key] = response

    return response
