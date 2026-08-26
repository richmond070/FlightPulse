"""
Load balancer metrics.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6, Phase D
("Metrics"):
  "Track total requests, successful requests, failures, latency, backend
  selection counts, health-check failures and active backend count.
  Expose a simple internal metrics endpoint for testing."
"""

from __future__ import annotations

import threading
from collections import defaultdict


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.retried_requests = 0
        self._latencies_ms: list[float] = []
        self.backend_selection_counts: dict[str, int] = defaultdict(int)
        self.health_check_failures: dict[str, int] = defaultdict(int)

    def record_request(self, backend_url: str, success: bool, latency_ms: float, retried: bool = False):
        with self._lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
            else:
                self.failed_requests += 1
            if retried:
                self.retried_requests += 1
            self.backend_selection_counts[backend_url] += 1
            self._latencies_ms.append(latency_ms)
            # Keep memory bounded; a rolling window is sufficient for a
            # "simple internal metrics endpoint for testing".
            if len(self._latencies_ms) > 1000:
                self._latencies_ms = self._latencies_ms[-1000:]

    def record_health_check_failure(self, backend_url: str):
        with self._lock:
            self.health_check_failures[backend_url] += 1

    def snapshot(self, active_backend_count: int) -> dict:
        with self._lock:
            latencies = list(self._latencies_ms)
            avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
            return {
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "retried_requests": self.retried_requests,
                "avg_latency_ms": round(avg_latency, 2),
                "backend_selection_counts": dict(self.backend_selection_counts),
                "health_check_failures": dict(self.health_check_failures),
                "active_backend_count": active_backend_count,
            }
