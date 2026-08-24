"""
Ingestion routes: POST /telemetry, GET /health, GET /version.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 7
(FastAPI Ingestion Service).

Phase 1 scope: validate the incoming batch and return an accepted
response. No queue enqueueing and no database writes yet — those are
wired in during Phase 4 (Queue) and Phase 5 (Persistence).
"""

from fastapi import APIRouter
from ingestion.schemas import TelemetryBatch, IngestResponse

router = APIRouter()

APP_VERSION = "0.1.0"


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/version")
async def version():
    return {"version": APP_VERSION}


@router.post("/telemetry", response_model=IngestResponse)
async def post_telemetry(batch: TelemetryBatch):
    # Phase 1: validation only. TODO (Phase 4): enqueue each event as a
    # job on the async queue instead of just echoing back what was accepted.
    ingestion_ids = [event.ingestion_id for event in batch.events]
    return IngestResponse(
        status="accepted",
        accepted=len(batch.events),
        ingestion_ids=ingestion_ids,
    )
