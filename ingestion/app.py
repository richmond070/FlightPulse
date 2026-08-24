"""
FlightPulse FastAPI ingestion service — entrypoint.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 7.

Run locally:
    uvicorn ingestion.app:app --host 0.0.0.0 --port 8001 --reload

Multiple instances (per section 7 / section 10 Phase 3) will later run as
fastapi-1:8001, fastapi-2:8002, fastapi-3:8003, fronted by the load balancer
on :8080. Only the load balancer is exposed as the ingestion entry point
once that phase is built.
"""

from fastapi import FastAPI
from ingestion.routes import router

app = FastAPI(title="FlightPulse Ingestion Service", version="0.1.0")
app.include_router(router)
