"""
Normalize raw OpenSky state vectors into FlightPulse's canonical event schema.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 3
(Step 2 — Normalize, Step 3 — Add ingestion metadata) and section 4
(Canonical Telemetry Event).

Ref (idempotency): FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
section 5.2 ("Define event identity before implementing database writes.
A practical strategy can use aircraft identifier plus source observation
timestamp... The key must match the semantics of the source rather than
assuming every polling response is unique.")

ingestion_id is therefore derived deterministically from
(source, icao24, last_contact) rather than a random UUID. last_contact is
OpenSky's own observation timestamp -- the "source observation timestamp"
the spec refers to -- so the same real-world observation produces the same
ingestion_id no matter how many times it's polled, retried, or redelivered.
This is what makes `ON CONFLICT (ingestion_id) DO NOTHING` in
worker/persistence.py catch genuine duplicate *observations*, not just
duplicate *job deliveries* of the same batch.

OpenSky's /states/all response is an array of positional fields per
aircraft, documented at:
https://openskynetwork.github.io/opensky-api/rest.html#response

Index  Field
0      icao24
1      callsign
2      origin_country
3      time_position
4      last_contact
5      longitude
6      latitude
7      baro_altitude (meters)
8      on_ground
9      velocity (m/s)
10     true_track (degrees)
11     vertical_rate (m/s)
12     sensors
13     geo_altitude (meters)
14     squawk
15     spi
16     position_source
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

STATE_VECTOR_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

# Fixed namespace UUID for deriving deterministic ingestion_ids via
# uuid5. Must never change once data has been persisted -- changing it
# would silently give every existing observation a new identity and
# defeat cross-run deduplication.
_INGESTION_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "flightpulse.opensky.ingestion_id")


def _clean_callsign(raw: Optional[str]) -> Optional[str]:
    return raw.strip() if raw else None


def _compute_ingestion_id(source: str, icao24: str, last_contact) -> str:
    """Deterministic event identity: same (source, icao24, last_contact)
    always yields the same ingestion_id.

    Falls back to a random UUID only when last_contact is missing (should
    be rare/never for real OpenSky data) -- an event with no source
    observation timestamp can't be deduplicated on that basis anyway, so
    there's nothing meaningful to hash against.
    """
    if last_contact is None:
        return str(uuid.uuid4())
    identity = f"{source}:{icao24}:{last_contact}"
    return str(uuid.uuid5(_INGESTION_ID_NAMESPACE, identity))


def normalize_state_vector(state: list, collector_id: str = "collector-1") -> dict:
    """
    Convert one raw OpenSky state-vector array into a canonical
    FlightPulse telemetry event dict (matches ingestion.schemas.TelemetryEvent).
    """
    fields = dict(zip(STATE_VECTOR_FIELDS, state))

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    source = "opensky"
    icao24 = fields.get("icao24")
    last_contact = fields.get("last_contact")

    return {
        "source": source,
        "icao24": icao24,
        "callsign": _clean_callsign(fields.get("callsign")),
        "origin_country": fields.get("origin_country"),
        "time_position": fields.get("time_position"),
        "last_contact": last_contact,
        "longitude": fields.get("longitude"),
        "latitude": fields.get("latitude"),
        "baro_altitude_m": fields.get("baro_altitude"),
        "geo_altitude_m": fields.get("geo_altitude"),
        "velocity_mps": fields.get("velocity"),
        "true_track_deg": fields.get("true_track"),
        "vertical_rate_mps": fields.get("vertical_rate"),
        "on_ground": fields.get("on_ground"),
        "ingested_at": now,
        "ingestion_id": _compute_ingestion_id(source, icao24, last_contact),
        "collector_id": collector_id,
    }


def normalize_batch(opensky_response: dict, collector_id: str = "collector-1") -> list[dict]:
    """
    Convert a full OpenSky /states/all response into a list of canonical
    telemetry events. Skips entries missing an icao24 (required identifier).
    """
    states = opensky_response.get("states") or []
    events = []
    for state in states:
        if not state or not state[0]:
            continue
        events.append(normalize_state_vector(state, collector_id=collector_id))
    return events
