"""
Normalize raw OpenSky state vectors into FlightPulse's canonical event schema.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 3
(Step 2 — Normalize, Step 3 — Add ingestion metadata) and section 4
(Canonical Telemetry Event).

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


def _clean_callsign(raw: Optional[str]) -> Optional[str]:
    return raw.strip() if raw else None


def normalize_state_vector(state: list, collector_id: str = "collector-1") -> dict:
    """
    Convert one raw OpenSky state-vector array into a canonical
    FlightPulse telemetry event dict (matches ingestion.schemas.TelemetryEvent).
    """
    fields = dict(zip(STATE_VECTOR_FIELDS, state))

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "source": "opensky",
        "icao24": fields.get("icao24"),
        "callsign": _clean_callsign(fields.get("callsign")),
        "origin_country": fields.get("origin_country"),
        "time_position": fields.get("time_position"),
        "last_contact": fields.get("last_contact"),
        "longitude": fields.get("longitude"),
        "latitude": fields.get("latitude"),
        "baro_altitude_m": fields.get("baro_altitude"),
        "geo_altitude_m": fields.get("geo_altitude"),
        "velocity_mps": fields.get("velocity"),
        "true_track_deg": fields.get("true_track"),
        "vertical_rate_mps": fields.get("vertical_rate"),
        "on_ground": fields.get("on_ground"),
        "ingested_at": now,
        "ingestion_id": str(uuid.uuid4()),
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
