"""
Canonical telemetry event schema.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 4
(Canonical Telemetry Event) and section 5 (PostgreSQL Data Model).
"""

from typing import Optional
from pydantic import BaseModel, Field


class TelemetryEvent(BaseModel):
    source: str = Field(default="opensky")
    icao24: str
    callsign: Optional[str] = None
    origin_country: Optional[str] = None
    time_position: Optional[int] = None
    last_contact: Optional[int] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    baro_altitude_m: Optional[float] = None
    geo_altitude_m: Optional[float] = None
    velocity_mps: Optional[float] = None
    true_track_deg: Optional[float] = None
    vertical_rate_mps: Optional[float] = None
    on_ground: Optional[bool] = None
    ingested_at: str
    ingestion_id: str


class TelemetryBatch(BaseModel):
    """A batch of normalized telemetry events sent by the collector
    to the load balancer / FastAPI ingestion service."""
    events: list[TelemetryEvent]


class IngestResponse(BaseModel):
    status: str
    accepted: int
    ingestion_ids: list[str]
