# FlightPulse

A production-inspired aviation telemetry platform built around OpenSky Network
data. Demonstrates live data ingestion, a custom load balancer, asynchronous
job processing, PostgreSQL storage, dbt transformations, data-quality
controls, and analytics-ready outputs.

**Core stack:** Python, FastAPI, custom Python load balancer, Redis-backed
async queue, PostgreSQL, dbt, Docker Compose. A dashboard is optional and
will be added only once the pipeline is stable.

Full design reference: `FlightPulse_Complete_Project_Workflow_Guide.pdf`
(project docs). This README is kept in sync with that guide — if the two
disagree, the guide is treated as the source of truth unless a change is
explicitly agreed and recorded here.

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
  - [ ] Phase D — metrics endpoint
- [ ] Phase 4 — Queue
- [ ] Phase 5 — Persistence
- [ ] Phase 6 — dbt
- [ ] Phase 7 — Load testing
- [ ] Phase 8 — Documentation

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

## Note on the async queue

The original design reference names BullMQ (a Node.js library) for the
async job queue. This project is kept pure Python, so a Python-native
queue is used instead (see `worker/` and `ingestion/routes.py`), per the
guide's own guidance not to force a mismatched tool into the architecture.
