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
    collector_id: Optional[str] = None


class TelemetryBatch(BaseModel):
    """A batch of normalized telemetry events sent by the collector
    to the load balancer / FastAPI ingestion service."""
    events: list[TelemetryEvent]


class IngestResponse(BaseModel):
    status: str
    accepted: int
    ingestion_ids: list[str]


class ExtractionLogEntry(BaseModel):
    """Metadata about one collector extraction cycle (one poll of
    OpenSky), not the telemetry events themselves.

    Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
    section 3, Extract stage -- fields explicitly called out there:
    source, extraction_started_at, extraction_completed_at,
    source_observation_time, request_id, collector_version,
    record_count, request_scope, and recording extraction failures.
    """
    request_id: str
    source: str = Field(default="opensky")
    collector_id: Optional[str] = None
    collector_version: Optional[str] = None
    request_scope: Optional[str] = None
    extraction_started_at: str
    extraction_completed_at: Optional[str] = None
    source_observation_time: Optional[int] = None
    record_count: Optional[int] = None
    success: bool
    error_message: Optional[str] = None
