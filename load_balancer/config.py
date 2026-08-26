"""
Load balancer configuration.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6 (Phase A,
step 2 — "Configure two or three FastAPI backend URLs") and section 7
("Run multiple instances: fastapi-1:8001, fastapi-2:8002, fastapi-3:8003,
load-balancer:8080. The load balancer is the only component exposed as
the ingestion entry point.").
"""

import os

# Comma-separated list of backend base URLs, e.g.
#   BACKEND_URLS=http://localhost:8001,http://localhost:8002,http://localhost:8003
_default_backends = "http://localhost:8001,http://localhost:8002,http://localhost:8003"

BACKEND_URLS: list[str] = [
    url.strip()
    for url in os.getenv("BACKEND_URLS", _default_backends).split(",")
    if url.strip()
]

LOAD_BALANCER_HOST = os.getenv("LOAD_BALANCER_HOST", "0.0.0.0")
LOAD_BALANCER_PORT = int(os.getenv("LOAD_BALANCER_PORT", "8080"))

# Phase A had no timeouts/retries — Phase C adds both. Keep a sane
# default so the balancer doesn't hang forever on a dead backend.
FORWARD_TIMEOUT_SECONDS = float(os.getenv("LB_FORWARD_TIMEOUT_SECONDS", "10"))

# --- Phase C: failure handling ---
# Max number of retries on a *different* healthy backend after a failed
# forward (connection error / timeout before any response was received).
# Only applied to requests considered "safe to retry" — see server.py.
MAX_RETRIES = int(os.getenv("LB_MAX_RETRIES", "1"))

# HTTP methods that are inherently safe to retry (idempotent by definition).
SAFE_RETRY_METHODS = {"GET", "PUT", "DELETE", "HEAD", "OPTIONS"}

# Header name the collector/clients use to mark a request as safe to retry
# even though its method (POST) isn't inherently idempotent. Required for
# POST /telemetry retries so a retry can't create duplicate records.
IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"

# --- Phase B: health checking ---
HEALTH_CHECK_INTERVAL_SECONDS = float(os.getenv("LB_HEALTH_CHECK_INTERVAL_SECONDS", "5"))
HEALTH_CHECK_TIMEOUT_SECONDS = float(os.getenv("LB_HEALTH_CHECK_TIMEOUT_SECONDS", "3"))
HEALTH_RECOVERIES_REQUIRED = int(os.getenv("LB_HEALTH_RECOVERIES_REQUIRED", "2"))
