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

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
TELEMETRY_TARGET_URL = os.getenv(
    "TELEMETRY_TARGET_URL",
    f"http://localhost:{os.getenv('INGESTION_PORT', '8001')}/telemetry",
)
COLLECTOR_ID = os.getenv("COLLECTOR_ID", "collector-1")


def build_client() -> OpenSkyClient:
    return OpenSkyClient(
        client_id=os.getenv("OPENSKY_CLIENT_ID") or None,
        client_secret=os.getenv("OPENSKY_CLIENT_SECRET") or None,
    )


def send_batch(events: list[dict]) -> bool:
    if not events:
        logger.info("No events to send this cycle")
        return True
    try:
        resp = requests.post(
            TELEMETRY_TARGET_URL, json={"events": events}, timeout=15
        )
        resp.raise_for_status()
        logger.info(
            "Sent batch of %d events -> %s (%s)",
            len(events), TELEMETRY_TARGET_URL, resp.status_code,
        )
        return True
    except requests.RequestException as exc:
        logger.error("Failed to send telemetry batch: %s", exc)
        return False


def run_once(client: OpenSkyClient) -> int:
    """Fetch one batch of state vectors, normalize, and send. Returns
    the number of events sent (0 on failure or empty response)."""
    raw = client.get_states()
    if raw is None:
        logger.warning("No data returned from OpenSky this cycle")
        return 0

    events = normalize_batch(raw, collector_id=COLLECTOR_ID)
    logger.info("Normalized %d state vectors", len(events))

    if send_batch(events):
        return len(events)
    return 0


def run_forever():
    client = build_client()
    mode = "authenticated" if client.token_manager.is_authenticated else "anonymous"
    logger.info(
        "Starting FlightPulse collector (%s mode, poll interval=%ss, target=%s)",
        mode, POLL_INTERVAL_SECONDS, TELEMETRY_TARGET_URL,
    )
    while True:
        try:
            run_once(client)
        except Exception:
            logger.exception("Unhandled error during collection cycle")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_forever()
