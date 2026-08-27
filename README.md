# FlightPulse

A production-inspired aviation telemetry platform built around OpenSky Network
data. Demonstrates live data ingestion, a custom load balancer, asynchronous
job processing, PostgreSQL storage, dbt transformations, data-quality
controls, and analytics-ready outputs.

**Core stack:** Python, FastAPI, custom Python load balancer, Redis-backed
async queue, PostgreSQL, dbt, Docker Compose. A dashboard is optional and
will be added only once the pipeline is stable.

Full design references:
- `FlightPulse_Complete_Project_Workflow_Guide.pdf` — Phases 1–4 technical spec.
- `FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf` — Phase 5
  onward: ETL specification, business objectives, data-quality rules, KPIs.

This README is kept in sync with both documents — if they disagree, the
more specific/recent continuation doc governs for Phase 5+, and the
original guide governs Phases 1–4, unless a change is explicitly agreed
and recorded here (see "Deliberate deviations from the docs" below).

## Business objective

Per `FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf`, section 1:

> Transform aviation telemetry from OpenSky Network into reliable,
> near-real-time operational intelligence that can be used to understand
> aircraft activity, airspace utilization, flight movement patterns, and
> changes in aviation traffic over time.

The business questions this pipeline is ultimately built to answer
(section 2 of the continuation doc) — aircraft counts by period, regional
density, activity trends by hour/day, most frequent aircraft/callsigns,
altitude/velocity patterns, climb/descend/stable activity, telemetry
freshness, and system behavior under load — are answered by the dbt marts
in Phase 6, not by the raw ingestion path alone. Phases 1–5 exist to get
clean, deduplicated, traceable data into PostgreSQL reliably enough for
those marts to be trustworthy.

## Target architecture

```
OpenSky Network (live aircraft states)
        ↓
Python Telemetry Collector (fetch + normalize + batch)
        ↓ HTTP
Custom Load Balancer (round-robin + healthcheck)
        ↓                    ↓
   FastAPI 1 (ingestion)  FastAPI 2 (ingestion)
        ↓                    ↓
        Async Job Queue (Redis-backed)
                ↓
        Python Workers (validate / enrich / batch persist)
                ↓
        PostgreSQL (raw + curated)
                ↓
        dbt (staging / marts)
                ↓
        Analytics / API / BI
```

## Repository structure

```
flightpulse/
├── collector/        # OpenSky client, event normalizer, batch producer
├── load_balancer/     # Custom round-robin + healthcheck reverse proxy
├── ingestion/          # FastAPI ingestion service
├── worker/             # Queue consumers, processors, persistence
├── dbt/                # staging + mart models, tests
├── sql/                # raw table migrations
├── tests/
├── docker-compose.yml
└── .env.example
```

## Status: Phase 1 — Foundation

- [x] Repository skeleton created
- [x] Docker Compose: PostgreSQL + Redis
- [x] `.env.example`
- [x] Initial `raw_telemetry` schema (`sql/001_raw_telemetry.sql`)
- [x] Minimal FastAPI ingestion service (`POST /telemetry`, `GET /health`,
      `GET /version`) — validation only, no queue/DB writes yet
- [x] Phase 2 — OpenSky collector
  - [x] `collector/opensky_client.py` — OAuth2 client-credentials auth (with
        anonymous fallback), `/states/all` fetch, 401/429 handling
  - [x] `collector/normalizer.py` — raw state vectors → canonical event schema
  - [x] `collector/producer.py` — polling loop, structured logging, sends
        batches to the ingestion service (target swaps to the load balancer
        once Phase 3 exists)
- [x] Phase 3 — Load balancer (Phase A: minimum viable round-robin)
  - [x] `load_balancer/config.py` — backend URL list, host/port, forward timeout
  - [x] `load_balancer/router.py` — thread-safe round-robin backend selection
  - [x] `load_balancer/server.py` — reverse proxy: forwards method/path/query/
        headers/body, returns backend response, logs every hop
  - [x] Phase B — health-aware routing
    - [x] `load_balancer/health.py` — background health checker polling
          each backend's `/health`, HEALTHY/UNHEALTHY/RECOVERING state
          machine (2 consecutive passes required to fully recover)
    - [x] `load_balancer/router.py` — round-robin restricted to
          currently-healthy backends only
    - [x] `GET /lb-status` — internal endpoint to inspect backend states
  - [x] Phase C — failure handling / retry / idempotency
    - [x] Connection/read timeout on every backend forward (`FORWARD_TIMEOUT_SECONDS`)
    - [x] Failed forward marks the backend unhealthy immediately (doesn't
          wait for the next periodic health-check tick)
    - [x] Retry on a different healthy backend, but only for safe methods
          (GET/PUT/DELETE/HEAD/OPTIONS) or a POST carrying an
          `Idempotency-Key` header
    - [x] `ingestion/routes.py` — in-memory idempotency-key dedupe cache
          (placeholder; durable version lands in Phase 5 via the
          `uq_raw_telemetry_ingestion_id` unique index)
    - [x] `collector/producer.py` — generates an `Idempotency-Key` per batch
  - [x] Phase D — metrics endpoint
    - [x] `load_balancer/metrics.py` — total/success/failure/retry counts,
          avg latency, per-backend selection counts, health-check
          failures, active backend count
    - [x] `GET /lb-metrics` — internal endpoint
- [x] Phase 4 — Async job queue
  - [x] `worker/settings.py` — Redis/queue config, retry/backoff constants
  - [x] `ingestion/routes.py` — enqueues one compact job per batch (arq pool)
  - [x] `worker/processor.py` — validates, normalizes, in-batch-dedupes,
        retries transient failures with exponential backoff
        (`arq.Retry(defer=...)`), dead-letters unrecoverable payloads
  - [x] `worker/consumer.py` — arq worker entrypoint
        (`arq worker.consumer.WorkerSettings`)
  - [x] Verified live: enqueue → process → complete; in-batch dedup;
        dead-letter on schema-validation failure; two concurrent workers
        splitting jobs from the same queue with no double-processing
- [x] Phase 5 — Persistence
  - [x] `worker/persistence.py` — real batched inserts into
        `raw_telemetry` (one `INSERT ... ON CONFLICT (ingestion_id) DO
        NOTHING RETURNING` per row; batches kept compact, per section 8)
  - [x] `sql/002_add_processed_at.sql` — adds `processed_at`, so
        queue-to-persisted latency is measurable in Phase 7
  - [x] `PersistenceUnavailable` on connection failure → arq retry/backoff
        (Postgres-down tested live; a 5s connect timeout keeps this
        under `JOB_TIMEOUT_SECONDS` so our own retry logic wins the race,
        not arq's job timeout)
  - [x] Batch-insert timing instrumentation — logs elapsed time and
        records/sec per batch (continuation doc, section 5.3: "measure
        insert latency and records/second before optimizing further";
        feeds the "Records processed per second" KPI in section 11)
  - [x] **Deterministic idempotency key** — `collector/normalizer.py`
        derives `ingestion_id` via `uuid5(icao24 + source observation
        timestamp)` instead of a random UUID, per continuation doc
        section 5.2 ("the key must match the semantics of the source
        rather than assuming every polling response is unique"). This is
        what makes `ON CONFLICT DO NOTHING` catch a genuine duplicate
        *observation* (same aircraft, same `last_contact`, polled twice),
        not just a duplicate job *delivery*.
  - [x] Verified live against real Postgres: fresh insert, exact
        redelivery (0 new rows), in-batch duplicate (1 row, not 2),
        Postgres-down → retry, full pipeline against real OpenSky data
        (10k+ event batches from a live poll)
  - [x] Indexes: kept to the existing `(icao24, ingested_at)` index from
        Phase 1 — no speculative geo/additional indexes added, per
        section 5.4 ("add ... only when query plans or benchmarks
        justify them")
- [ ] Phase 6 — dbt
- [ ] Phase 7 — Load testing
- [ ] Phase 8 — Analytics layer

## Local setup

```bash
cp .env.example .env
docker compose up -d postgres redis

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Running the ingestion instances + load balancer

The load balancer round-robins across multiple FastAPI ingestion instances,
so start all three before the balancer (per the guide's naming convention:
fastapi-1:8001, fastapi-2:8002, fastapi-3:8003, load-balancer:8080):

```bash
# terminal 1
uvicorn ingestion.app:app --host 0.0.0.0 --port 8001 --reload
# terminal 2
uvicorn ingestion.app:app --host 0.0.0.0 --port 8002 --reload
# terminal 3
uvicorn ingestion.app:app --host 0.0.0.0 --port 8003 --reload

# terminal 4 — the load balancer, the only component the collector/clients talk to
python -m load_balancer.server
```

Verify (via the load balancer, not a single instance directly):
```bash
curl http://localhost:8080/health
curl http://localhost:8080/version
```

You should see the load balancer's log lines rotate across `:8001`, `:8002`,
and `:8003` on successive requests.

### Running the collector

With the load balancer and all three ingestion instances running (above),
in a separate terminal:

```bash
python -m collector.producer
```

This polls OpenSky's `/states/all` on `POLL_INTERVAL_SECONDS` (default 30s),
normalizes each state vector into the canonical event schema, and POSTs
batches to `TELEMETRY_TARGET_URL` — which now points at the load balancer
(`http://localhost:8080/telemetry`) rather than any single FastAPI instance,
so traffic is distributed across all backends.

- **Anonymous mode**: leave `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`
  blank in `.env`. Works, but rate limits are materially lower.
- **Authenticated mode**: create an API client on your OpenSky account page
  and set `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`. The collector
  handles OAuth2 token fetch/refresh automatically (tokens expire after
  ~30 minutes; refreshed transparently, with 401 retry-once handling).

### Running the worker and testing the queue + persistence

Requires Redis and PostgreSQL running (`docker compose up -d postgres redis`,
or run both natively — see each phase's testing notes below), plus
`DATABASE_URL` and `REDIS_URL` set in `.env`.

```bash
# terminal 5 — the async worker (run this and the next in addition to the
# ingestion instances + load balancer above)
export PYTHONPATH=$(pwd)
arq worker.consumer.WorkerSettings
```

Run multiple instances of the same command in separate terminals to
verify concurrent processing (Phase 4's own checklist item) — arq
workers share the queue via Redis and won't double-process a job.

**Windows note:** if you see
`Psycopg cannot use the 'ProactorEventLoop' to run in async mode`,
that's a known Windows asyncio/psycopg incompatibility, already handled
in `worker/consumer.py` via `asyncio.set_event_loop_policy(
asyncio.WindowsSelectorEventLoopPolicy())`. If you still hit it, confirm
you're running the current `worker/consumer.py`.

Once a batch is enqueued (either via `POST /telemetry` directly, or by
running the collector against real OpenSky data — see above), the worker
log shows validation, in-batch dedup, and persistence timing, e.g.:

```
Processing batch attempt=1 received=10423 after_dedupe=10423
Persisted batch: 10423 event(s) submitted, 10423 newly inserted, 0 skipped as duplicate (14.8s elapsed, 704 records/sec)
```

Confirm data landed:
```bash
psql -U flightpulse -h localhost -d flightpulse -c "SELECT count(*) FROM raw_telemetry;"
```

## Deliberate deviations from the docs

Documented here so the two source PDFs and the actual codebase don't
silently drift apart.

**Async queue: `arq` instead of BullMQ.** Both source docs name
BullMQ + Redis. BullMQ is a Node.js library; this project is kept pure
Python. The original guide's section 8 explicitly permits this
substitution ("if keeping the project entirely Python is more important,
replace BullMQ with a Python-native queue"). `arq` was chosen over
Celery/RQ/Dramatiq for being asyncio-native (matches FastAPI's async
handlers) and Redis-backed (no extra broker). Every behavioral
requirement either doc actually specifies — retries, exponential
backoff, bounded attempts, dead-lettering, concurrent worker processing,
idempotent writes — is implemented and verified live regardless of which
library provides the mechanism.

**Idempotency key: deterministic hash, not the collector's original
random UUID.** Phase 4 initially generated `ingestion_id` via
`uuid.uuid4()` per event. The Phase 5 continuation doc's section 5.2
explicitly warns against this ("the key must match the semantics of the
source rather than assuming every polling response is unique"). Fixed in
`collector/normalizer.py`: `ingestion_id` is now `uuid5(source + icao24 +
last_contact)`, so the same real-world observation always produces the
same id, no matter how many times it's polled or a job is redelivered.
