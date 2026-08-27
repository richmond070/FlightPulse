"""
Collector entrypoint: fetch -> normalize -> batch -> send.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 3 (Data Flow,
steps 1-4) and section 10, Phase 2.

Phase 2 scope: the collector sends batches directly to the FastAPI
ingestion service, since the load balancer (Phase 3) doesn't exist yet.
Once Phase 3 is built, point TELEMETRY_TARGET_URL at the load balancer
instead (per the guide: "The collector should not know which FastAPI
instance receives the request" — that's the load balancer's job, not
something to hardcode here).

Polling interval: OpenSky recommends conservative polling. Default here
is 30 seconds, configurable via POLL_INTERVAL_SECONDS. Anonymous access
has materially lower rate limits than authenticated access — set
OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET to use authenticated calls.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from collector.normalizer import normalize_batch
from collector.opensky_client import OpenSkyClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("flightpulse.collector.producer")

# Bumped manually when collector/normalizer.py's output schema changes.
# Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
# section 3 -- "collector_version" is one of the extraction metadata
# fields to record per cycle.
COLLECTOR_VERSION = "1.1.0"

EXTRACTION_LOG_URL = os.getenv(
    "EXTRACTION_LOG_URL",
    None,  # derived from TELEMETRY_TARGET_URL below once it's defined
)

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
TELEMETRY_TARGET_URL = os.getenv(
    "TELEMETRY_TARGET_URL",
    f"http://localhost:{os.getenv('INGESTION_PORT', '8001')}/telemetry",
)
COLLECTOR_ID = os.getenv("COLLECTOR_ID", "collector-1")

# Optional bounding box (lamin, lomin, lamax, lomax) to limit /states/all
# to a region instead of the whole globe -- conserves OpenSky API credits
# during local testing. All four must be set together or none are used.
# Nigeria/West Africa default given as an example; override via env.
_bbox_env = (
    os.getenv("OPENSKY_BBOX_LAMIN"),
    os.getenv("OPENSKY_BBOX_LOMIN"),
    os.getenv("OPENSKY_BBOX_LAMAX"),
    os.getenv("OPENSKY_BBOX_LOMAX"),
)
BBOX: tuple[float, float, float, float] | None = (
    tuple(float(v) for v in _bbox_env) if all(_bbox_env) else None
)
REQUEST_SCOPE = f"bbox={BBOX}" if BBOX else "global"

# Derive the extraction-log endpoint from the telemetry target so both
# point at the same ingestion service / load balancer by default,
# without requiring a second URL to be configured for the common case.
if EXTRACTION_LOG_URL is None:
    EXTRACTION_LOG_URL = TELEMETRY_TARGET_URL.rsplit("/telemetry", 1)[0] + "/extraction-log"


def build_client() -> OpenSkyClient:
    return OpenSkyClient(
        client_id=os.getenv("OPENSKY_CLIENT_ID") or None,
        client_secret=os.getenv("OPENSKY_CLIENT_SECRET") or None,
    )


def send_batch(events: list[dict]) -> bool:
    if not events:
        logger.info("No events to send this cycle")
        return True
    idempotency_key = str(uuid.uuid4())
    try:
        resp = requests.post(
            TELEMETRY_TARGET_URL,
            json={"events": events},
            headers={"Idempotency-Key": idempotency_key},
            timeout=15,
        )
        resp.raise_for_status()
        logger.info(
            "Sent batch of %d events -> %s (%s) [idempotency-key=%s]",
            len(events), TELEMETRY_TARGET_URL, resp.status_code, idempotency_key,
        )
        return True
    except requests.RequestException as exc:
        logger.error("Failed to send telemetry batch: %s", exc)
        return False


def send_extraction_log(entry: dict) -> None:
    """Best-effort POST of one extraction cycle's metadata.

    Deliberately does not affect run_once's return value or retry the
    main polling loop on failure -- losing one extraction-log row is
    far less costly than losing telemetry data, and this must never be
    the reason a collector cycle stalls. A failure here is logged
    locally (the collector-local fallback section 3 is otherwise meant
    to avoid) but doesn't block anything downstream.
    """
    try:
        resp = requests.post(EXTRACTION_LOG_URL, json=entry, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Failed to send extraction log (non-fatal): %s", exc)


def run_once(client: OpenSkyClient) -> int:
    """Fetch one batch of state vectors, normalize, and send. Returns
    the number of events sent (0 on failure or empty response).

    Also builds and sends an ExtractionLogEntry for this cycle
    regardless of success/failure, per
    FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
    section 3 ("record extraction failures").
    """
    request_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    def _log(record_count: int, success: bool, error_message: str | None, source_observation_time=None):
        send_extraction_log({
            "request_id": request_id,
            "source": "opensky",
            "collector_id": COLLECTOR_ID,
            "collector_version": COLLECTOR_VERSION,
            "request_scope": REQUEST_SCOPE,
            "extraction_started_at": started_at.isoformat(),
            "extraction_completed_at": datetime.now(timezone.utc).isoformat(),
            "source_observation_time": source_observation_time,
            "record_count": record_count,
            "success": success,
            "error_message": error_message,
        })

    try:
        raw = client.get_states(bbox=BBOX)
    except Exception as exc:
        logger.exception("OpenSky extraction failed")
        _log(record_count=0, success=False, error_message=str(exc))
        return 0

    if raw is None:
        logger.warning("No data returned from OpenSky this cycle")
        _log(record_count=0, success=False, error_message="No data returned from OpenSky (see client logs)")
        return 0

    events = normalize_batch(raw, collector_id=COLLECTOR_ID)
    logger.info("Normalized %d state vectors", len(events))

    sent_ok = send_batch(events)
    _log(
        record_count=len(events),
        success=sent_ok,
        error_message=None if sent_ok else "Failed to send telemetry batch (see logs)",
        source_observation_time=raw.get("time"),
    )

    return len(events) if sent_ok else 0


def run_forever():
    client = build_client()
    mode = "authenticated" if client.token_manager.is_authenticated else "anonymous"
    logger.info(
        "Starting FlightPulse collector (%s mode, poll interval=%ss, target=%s, bbox=%s, extraction_log=%s)",
        mode, POLL_INTERVAL_SECONDS, TELEMETRY_TARGET_URL, BBOX or "none (global)", EXTRACTION_LOG_URL,
    )
    while True:
        try:
            run_once(client)
        except Exception:
            logger.exception("Unhandled error during collection cycle")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_forever()
