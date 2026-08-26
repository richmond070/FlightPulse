"""
Round-robin backend selection, restricted to healthy backends.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6.
Phase A: plain round-robin over the static backend list.
Phase B: "Only healthy backends participate in normal routing" — the
router now consults a HealthChecker (health.py) and cycles only through
backends currently marked HEALTHY. If a backend flips to UNHEALTHY or
RECOVERING between checks, it drops out of rotation until it's HEALTHY
again.
"""

import itertools
import threading
from typing import Optional

from load_balancer.health import HealthChecker


class RoundRobinRouter:
    def __init__(self, backend_urls: list[str], health_checker: Optional[HealthChecker] = None):
        if not backend_urls:
            raise ValueError("RoundRobinRouter requires at least one backend URL")
        self.backend_urls = list(backend_urls)
        self.health_checker = health_checker
        self._cycle = itertools.cycle(self.backend_urls)
        self._lock = threading.Lock()

    def next_backend(self) -> Optional[str]:
        """
        Returns the next backend to use, restricted to healthy backends
        when a HealthChecker is attached. Returns None if no backend is
        currently healthy (Phase A had no such concept — this is new in
        Phase B).
        """
        if self.health_checker is None:
            with self._lock:
                return next(self._cycle)

        healthy = set(self.health_checker.healthy_backends())
        if not healthy:
            return None

        with self._lock:
            # Advance the shared cycle until we land on a healthy backend.
            # Bounded by len(backend_urls) so we never spin forever.
            for _ in range(len(self.backend_urls)):
                candidate = next(self._cycle)
                if candidate in healthy:
                    return candidate
        return None
