"""
Phase 4 queue configuration.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 8
("BullMQ + Redis Processing" — job lifecycle queued -> active ->
completed/failed -> retry/backoff -> dead-letter; "use retries for
transient failures, exponential backoff, bounded attempts, and
idempotent database writes") and section 9 (Phase 4 checklist).

FlightPulse substitutes a Python-native, Redis-backed queue (arq) for
BullMQ per the guide's own allowance in section 8: "if keeping the
project entirely Python is more important, replace BullMQ with a
Python-native queue." This keeps the queue boundary in the same
language and Docker image family as the rest of the stack, with no
extra Node.js service.
"""

import os

from arq.connections import RedisSettings

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --- Phase 5: PostgreSQL persistence ---
# Built from the same POSTGRES_* vars docker-compose.yml already uses
# (Phase 1), so compose and Python share one source of truth instead of
# maintaining two separate connection configs.
POSTGRES_USER = os.getenv("POSTGRES_USER", "flightpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "flightpulse")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flightpulse")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

# Connection pool sizing for worker/persistence.py's AsyncConnectionPool.
#
# DB_POOL_MAX_SIZE raised from 5 to 10 after a Phase 7 load-test finding:
# once replay/player.py started sending batches concurrently (see
# --concurrency), and arq's own default worker concurrency (max_jobs=10,
# see worker/consumer.py's explicit max_jobs setting added alongside this
# change) allowed multiple batches to be persisted at the same time, a
# pool of 5 connections was consistently exhausted -- "couldn't get a
# connection after 5.00 sec" -- even though only a handful of batches
# were genuinely in flight. Matched to arq's max_jobs=10 below so the
# two settings reflect the same intended concurrency instead of one
# silently bottlenecking the other.
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))

# How long to wait for a connection before giving up and raising
# PersistenceUnavailable. Kept well under JOB_TIMEOUT_SECONDS so a
# down database surfaces as our own retry/backoff (worker/processor.py)
# rather than arq's job timeout killing the job first.
DB_POOL_CONNECT_TIMEOUT_SECONDS = float(os.getenv("DB_POOL_CONNECT_TIMEOUT_SECONDS", "5"))

# Name of the arq queue. Kept explicit (rather than arq's default) so
# multiple logical queues could be introduced later without collision.
QUEUE_NAME = os.getenv("QUEUE_NAME", "flightpulse:telemetry")

# --- Retry / backoff (Section 8: "bounded attempts", "exponential backoff") ---
# Total attempts including the first (non-retry) run.
MAX_JOB_ATTEMPTS = int(os.getenv("ARQ_MAX_JOB_ATTEMPTS", "5"))

# Base delay in seconds for exponential backoff between attempts.
# Actual delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), capped
# at RETRY_BACKOFF_MAX_SECONDS. See worker/processor.py for the calculation.
RETRY_BACKOFF_BASE_SECONDS = float(os.getenv("ARQ_RETRY_BACKOFF_BASE_SECONDS", "2"))
RETRY_BACKOFF_MAX_SECONDS = float(os.getenv("ARQ_RETRY_BACKOFF_MAX_SECONDS", "60"))

# Per-job execution timeout. A batch that hangs past this is treated as a
# transient failure and retried like any other.
#
# Raised from 30s to 90s after a Phase 7 load-test finding: at 10x replay
# speed, row-by-row batch inserts (worker/persistence.py, pre-fix) took as
# long as 40-48s for an 8000-event batch, exceeding the old 30s timeout
# and causing arq to kill jobs mid-write -- one such batch was lost
# entirely (job expired before a retry completed). The persistence fix
# (chunked multi-row INSERT) addresses the root cause; this timeout bump
# is deliberately kept as a safety margin on top of that fix, not a
# substitute for it -- a slow DB or network hiccup should still have
# room to complete rather than being killed mid-write.
JOB_TIMEOUT_SECONDS = int(os.getenv("ARQ_JOB_TIMEOUT_SECONDS", "90"))

# Redis list used as the dead-letter destination once MAX_JOB_ATTEMPTS is
# exhausted (Section 8: "failed -> dead-letter handling"). A durable
# dead_letter_jobs table is a candidate for Phase 5, but a Redis list is
# enough to satisfy Phase 4's own definition of done without reaching
# into persistence work that belongs to the next phase.
DEAD_LETTER_KEY = os.getenv("DEAD_LETTER_KEY", "flightpulse:telemetry:dead_letter")

# How many worker processes this deployment expects to run concurrently.
# Not enforced here — it's informational for docker-compose / the Phase 4
# "run multiple workers and verify concurrent processing" checklist item —
# but centralized so compose and docs can reference one source.
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "2"))


def get_redis_settings() -> RedisSettings:
    """Build arq's RedisSettings from REDIS_URL so both the enqueueing
    side (ingestion/routes.py) and the consuming side (worker/consumer.py)
    connect to the same Redis instance from a single source of truth.
    """
    return RedisSettings.from_dsn(REDIS_URL)
