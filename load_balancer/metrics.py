"""
Load balancer metrics.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 6, Phase D
("Metrics"):
  "Track total requests, successful requests, failures, latency, backend
  selection counts, health-check failures and active backend count.
  Expose a simple internal metrics endpoint for testing."
"""

from __future__ import annotations

import math
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

    @staticmethod
    def _percentile(sorted_values: list[float], pct: float) -> float:
        """Nearest-rank percentile over an already-sorted list.

        Phase 7 (continuation doc, section 10 & 11) explicitly asks for
        p50/p95 latency, not just an average -- an average hides exactly
        the kind of tail behavior load testing exists to find (a handful
        of very slow requests can sit well above the mean while barely
        moving it). Nearest-rank is used rather than linear
        interpolation since it's simpler and sufficiently accurate for
        the rolling 1000-sample window this class already keeps.
        """
        if not sorted_values:
            return 0.0
        n = len(sorted_values)
        # Nearest-rank: index = ceil(pct/100 * n) - 1, clamped to valid range.
        rank = max(1, math.ceil((pct / 100.0) * n))
        index = min(rank, n) - 1
        return sorted_values[index]

    def snapshot(self, active_backend_count: int) -> dict:
        with self._lock:
            latencies = sorted(self._latencies_ms)
            avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
            return {
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "retried_requests": self.retried_requests,
                "avg_latency_ms": round(avg_latency, 2),
                "p50_latency_ms": round(self._percentile(latencies, 50), 2),
                "p95_latency_ms": round(self._percentile(latencies, 95), 2),
                "p99_latency_ms": round(self._percentile(latencies, 99), 2),
                "min_latency_ms": round(latencies[0], 2) if latencies else 0.0,
                "max_latency_ms": round(latencies[-1], 2) if latencies else 0.0,
                "sample_count": len(latencies),
                "backend_selection_counts": dict(self.backend_selection_counts),
                "health_check_failures": dict(self.health_check_failures),
                "active_backend_count": active_backend_count,
            }
