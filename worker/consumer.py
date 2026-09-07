"""
Phase 4 worker entrypoint.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 8 (job
lifecycle, retries/backoff) and section 9, Phase 4 checklist ("Run
multiple workers and verify concurrent processing").

Run locally (one instance):
    arq worker.consumer.WorkerSettings

Run multiple instances to verify concurrent processing (Phase 4
checklist item) by starting this command in separate processes/
containers -- arq workers pull from the same Redis queue and don't
double-process a job because arq acquires a per-job lock in Redis.
"""

import asyncio
import logging
import sys

from arq.connections import RedisSettings

from worker.processor import process_extraction_log, process_telemetry_batch
from worker.settings import (
    JOB_TIMEOUT_SECONDS,
    MAX_JOB_ATTEMPTS,
    QUEUE_NAME,
    get_redis_settings,
)

# On Windows, asyncio's default event loop is ProactorEventLoop, which
# psycopg's async driver cannot use (it requires selector-based I/O).
# Without this, every DB connection attempt fails with:
#   "Psycopg cannot use the 'ProactorEventLoop' to run in async mode"
# regardless of retries/backoff, since it's a loop incompatibility, not a
# transient failure. This must be set before arq creates its event loop,
# so it's applied at import time here rather than inside a function.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flightpulse.worker.consumer")


async def on_job_start(ctx: dict) -> None:
    logger.info("job started: %s (try %s)", ctx.get("job_id"), ctx.get("job_try"))


async def on_job_end(ctx: dict) -> None:
    logger.info("job finished: %s (try %s)", ctx.get("job_id"), ctx.get("job_try"))


class WorkerSettings:
    """arq reads this class by convention when you run
    `arq worker.consumer.WorkerSettings`.
    """

    functions = [process_telemetry_batch, process_extraction_log]
    queue_name = QUEUE_NAME
    redis_settings: RedisSettings = get_redis_settings()

    # Section 8: "bounded attempts" / "exponential backoff". arq counts
    # ctx["job_try"] starting at 1; max_tries is the ceiling on total
    # attempts (first run + retries) before arq marks the job failed.
    # The actual exponential delay between attempts is computed in
    # worker/processor.py's compute_backoff_seconds() and applied there
    # by raising arq.jobs.Retry(defer=...) -- arq has no separate
    # WorkerSettings hook for per-job backoff, so it lives with the retry
    # decision itself rather than split across two files.
    max_tries = MAX_JOB_ATTEMPTS
    job_timeout = JOB_TIMEOUT_SECONDS

    # Bounds how many jobs this worker process runs concurrently. Left
    # at arq's implicit default (10) previously -- explicit now, and
    # deliberately sized to match worker/settings.py's DB_POOL_MAX_SIZE
    # (also 10). Phase 7 load testing found that letting worker
    # concurrency exceed pool size meant jobs would compete for
    # connections and fail with "couldn't get a connection after 5.00
    # sec" even under fairly modest concurrent load. Keeping these two
    # numbers matched means the worker naturally throttles itself to
    # what the database can actually serve, rather than accepting more
    # concurrent work than it can complete and discovering that as
    # connection-pool timeouts downstream.
    max_jobs = 10

    on_job_start = on_job_start
    on_job_end = on_job_end
