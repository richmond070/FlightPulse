"""
Custom HTTP reverse-proxy load balancer.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6.

Phase A (minimum viable balancer) — done.
Phase B (health-aware routing) — done (health.py, router.py).

Phase C (failure handling) — this file:
  "Set connection/read timeouts. If a backend fails before a response is
  received, mark it unhealthy and retry the request on another healthy
  backend only when the request is safe to retry. For telemetry
  ingestion, use an idempotency key so retries cannot create duplicate
  records."

  - Timeout: requests.request(..., timeout=FORWARD_TIMEOUT_SECONDS) already
    bounds connect+read; see forward_once().
  - On failure before a response arrives: mark_unhealthy() immediately
    (doesn't wait for the next periodic health-check tick), then retry
    on a different healthy backend, up to MAX_RETRIES times.
  - "Safe to retry": GET/PUT/DELETE/HEAD/OPTIONS are inherently safe.
    POST is only retried if the client sent an Idempotency-Key header —
    otherwise a retried POST /telemetry could create duplicate records,
    which is exactly what the guide says to avoid.

Phase D (metrics) — this file + metrics.py:
  "Track total requests, successful requests, failures, latency, backend
  selection counts, health-check failures and active backend count.
  Expose a simple internal metrics endpoint for testing."
  -> GET /lb-metrics

Run with:
    python -m load_balancer.server
"""

from __future__ import annotations

import logging
import time

import requests
from fastapi import FastAPI, Request, Response
import uvicorn

from load_balancer.config import (
    BACKEND_URLS,
    FORWARD_TIMEOUT_SECONDS,
    HEALTH_CHECK_INTERVAL_SECONDS,
    HEALTH_CHECK_TIMEOUT_SECONDS,
    HEALTH_RECOVERIES_REQUIRED,
    IDEMPOTENCY_KEY_HEADER,
    LOAD_BALANCER_HOST,
    LOAD_BALANCER_PORT,
    MAX_RETRIES,
    SAFE_RETRY_METHODS,
)
from load_balancer.health import HealthChecker
from load_balancer.metrics import Metrics
from load_balancer.router import RoundRobinRouter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("flightpulse.load_balancer")

app = FastAPI(title="FlightPulse Load Balancer", version="0.1.0-phaseCD")

metrics = Metrics()
health_checker = HealthChecker(
    BACKEND_URLS,
    check_interval_seconds=HEALTH_CHECK_INTERVAL_SECONDS,
    request_timeout_seconds=HEALTH_CHECK_TIMEOUT_SECONDS,
    recoveries_required=HEALTH_RECOVERIES_REQUIRED,
    metrics=metrics,
)
router = RoundRobinRouter(BACKEND_URLS, health_checker=health_checker)

HOP_BY_HOP_REQUEST_HEADERS = {"host", "content-length"}
HOP_BY_HOP_RESPONSE_HEADERS = {"content-length", "transfer-encoding", "connection"}


@app.on_event("startup")
async def startup():
    health_checker.start()


@app.get("/lb-status")
async def lb_status():
    """Internal/dev endpoint: current backend health states."""
    return {"backends": health_checker.snapshot()}


@app.get("/lb-metrics")
async def lb_metrics():
    """Internal/dev endpoint (Phase D): request/latency/backend metrics."""
    active_count = len(health_checker.healthy_backends())
    return metrics.snapshot(active_backend_count=active_count)


def _is_safe_to_retry(method: str, headers: dict) -> bool:
    if method.upper() in SAFE_RETRY_METHODS:
        return True
    # POST (and other non-idempotent methods) are only safe to retry if
    # the caller supplied an idempotency key, so a retry can be deduped
    # on the ingestion side instead of creating a duplicate record.
    return any(k.lower() == IDEMPOTENCY_KEY_HEADER.lower() for k in headers)


def forward_once(method: str, target_url: str, headers: dict, params: dict, body: bytes):
    """Single forward attempt. Raises requests.RequestException on
    connection/timeout failure before any response is received."""
    return requests.request(
        method=method,
        url=target_url,
        headers=headers,
        params=params,
        data=body,
        timeout=FORWARD_TIMEOUT_SECONDS,  # bounds connect + read
    )


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def proxy(full_path: str, request: Request):
    body = await request.body()
    forward_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP_REQUEST_HEADERS
    }
    query_params = dict(request.query_params)
    retryable = _is_safe_to_retry(request.method, forward_headers)

    attempted_backends: list[str] = []
    attempts_allowed = 1 + (MAX_RETRIES if retryable else 0)
    last_error: Exception | None = None

    for attempt in range(attempts_allowed):
        backend_base = router.next_backend()

        if backend_base is None:
            logger.error(
                "No healthy backends available to serve %s %s",
                request.method, request.url.path,
            )
            metrics.record_request(backend_url="none", success=False, latency_ms=0.0)
            return Response(content=b"Service Unavailable: no healthy backends", status_code=503)

        # Avoid immediately retrying on the exact same backend that just failed.
        if backend_base in attempted_backends and len(health_checker.healthy_backends()) > len(attempted_backends):
            continue

        attempted_backends.append(backend_base)
        target_url = f"{backend_base}/{full_path}"
        is_retry = attempt > 0

        start = time.monotonic()
        logger.info(
            "%s%s %s -> %s",
            "Retrying " if is_retry else "Forwarding ",
            request.method, request.url.path, target_url,
        )

        try:
            backend_resp = forward_once(request.method, target_url, forward_headers, query_params, body)
        except requests.RequestException as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.error(
                "Backend request failed: %s %s (%.1fms) — %s",
                request.method, target_url, elapsed_ms, exc,
            )
            # Phase C: mark unhealthy immediately, don't wait for the next
            # periodic health check to notice.
            health_checker.mark_unhealthy(backend_base)
            metrics.record_request(
                backend_url=backend_base, success=False, latency_ms=elapsed_ms, retried=is_retry,
            )
            last_error = exc
            if retryable and attempt < attempts_allowed - 1:
                continue  # try another healthy backend
            return Response(content=b"Bad Gateway", status_code=502)

        elapsed_ms = (time.monotonic() - start) * 1000
        logger.info(
            "Backend responded: %s %s -> %s (%.1fms)",
            request.method, target_url, backend_resp.status_code, elapsed_ms,
        )
        metrics.record_request(
            backend_url=backend_base,
            success=backend_resp.status_code < 500,
            latency_ms=elapsed_ms,
            retried=is_retry,
        )

        response_headers = {
            k: v
            for k, v in backend_resp.headers.items()
            if k.lower() not in HOP_BY_HOP_RESPONSE_HEADERS
        }
        return Response(
            content=backend_resp.content,
            status_code=backend_resp.status_code,
            headers=response_headers,
        )

    # Exhausted retries without a response.
    logger.error("All retry attempts exhausted for %s %s: %s", request.method, request.url.path, last_error)
    return Response(content=b"Bad Gateway", status_code=502)


if __name__ == "__main__":
    logger.info("Starting FlightPulse load balancer, backends=%s", BACKEND_URLS)
    uvicorn.run(app, host=LOAD_BALANCER_HOST, port=LOAD_BALANCER_PORT)
