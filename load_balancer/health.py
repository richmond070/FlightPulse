"""
Backend health checking.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6, Phase B
("Health-aware routing"):
  "Add a /health endpoint to every FastAPI instance. The balancer
  periodically checks each backend. Maintain a backend state such as
  HEALTHY, UNHEALTHY, or RECOVERING. Only healthy backends participate
  in normal routing."

Every ingestion instance already exposes GET /health (Phase 1), so no
backend changes are needed — only the balancer side.

State machine:
  HEALTHY     -> normal state, participates in routing
  UNHEALTHY   -> failed its last health check, excluded from routing
  RECOVERING  -> was UNHEALTHY, passed one health check, needs a further
                 consecutive pass before being trusted as HEALTHY again
                 (avoids flapping a backend back in on a single lucky ping)
"""

from __future__ import annotations

import logging
import threading
import time
from enum import Enum

import requests

logger = logging.getLogger("flightpulse.load_balancer.health")


class BackendState(str, Enum):
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    RECOVERING = "RECOVERING"


class HealthChecker:
    """
    Periodically polls GET {backend}/health for each configured backend
    and tracks its state. Thread-safe; intended to run its polling loop
    in a background thread started by the server at startup.
    """

    def __init__(
        self,
        backend_urls: list[str],
        check_interval_seconds: float = 5.0,
        request_timeout_seconds: float = 3.0,
        recoveries_required: int = 2,
        metrics=None,
    ):
        self.backend_urls = list(backend_urls)
        self.check_interval_seconds = check_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.recoveries_required = recoveries_required
        self.metrics = metrics

        self._lock = threading.Lock()
        # Start optimistic: assume healthy until the first check says otherwise.
        self._state: dict[str, BackendState] = {
            url: BackendState.HEALTHY for url in self.backend_urls
        }
        self._consecutive_passes: dict[str, int] = {url: 0 for url in self.backend_urls}
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Health checker started, interval=%ss, backends=%s",
            self.check_interval_seconds, self.backend_urls,
        )

    def stop(self):
        self._stop_event.set()

    def _run_loop(self):
        while not self._stop_event.is_set():
            for url in self.backend_urls:
                self._check_one(url)
            self._stop_event.wait(self.check_interval_seconds)

    def _check_one(self, url: str):
        healthy = False
        try:
            resp = requests.get(f"{url}/health", timeout=self.request_timeout_seconds)
            healthy = resp.status_code == 200
        except requests.RequestException as exc:
            logger.debug("Health check failed for %s: %s", url, exc)
            healthy = False

        if not healthy and self.metrics is not None:
            self.metrics.record_health_check_failure(url)

        with self._lock:
            current = self._state[url]

            if healthy:
                if current == BackendState.HEALTHY:
                    return
                if current == BackendState.UNHEALTHY:
                    self._state[url] = BackendState.RECOVERING
                    self._consecutive_passes[url] = 1
                    logger.info("Backend %s: UNHEALTHY -> RECOVERING", url)
                elif current == BackendState.RECOVERING:
                    self._consecutive_passes[url] += 1
                    if self._consecutive_passes[url] >= self.recoveries_required:
                        self._state[url] = BackendState.HEALTHY
                        self._consecutive_passes[url] = 0
                        logger.info("Backend %s: RECOVERING -> HEALTHY", url)
            else:
                if current != BackendState.UNHEALTHY:
                    logger.warning("Backend %s: %s -> UNHEALTHY", url, current.value)
                self._state[url] = BackendState.UNHEALTHY
                self._consecutive_passes[url] = 0

    def mark_unhealthy(self, url: str):
        """
        Immediately flag a backend as UNHEALTHY outside the normal polling
        cycle. Used by the proxy path (Phase C) when a live request to a
        backend fails before a response is received — we don't want to
        wait up to check_interval_seconds for the next periodic check to
        notice.
        """
        with self._lock:
            if self._state[url] != BackendState.UNHEALTHY:
                logger.warning("Backend %s: %s -> UNHEALTHY (marked by failed request)", url, self._state[url].value)
            self._state[url] = BackendState.UNHEALTHY
            self._consecutive_passes[url] = 0

    def get_state(self, url: str) -> BackendState:
        with self._lock:
            return self._state[url]

    def healthy_backends(self) -> list[str]:
        with self._lock:
            return [
                url for url, state in self._state.items()
                if state == BackendState.HEALTHY
            ]

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return {url: state.value for url, state in self._state.items()}
