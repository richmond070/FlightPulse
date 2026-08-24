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
- [ ] Phase 3 — Load balancer
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

uvicorn ingestion.app:app --reload --port 8001
```

Verify:
```bash
curl http://localhost:8001/health
curl http://localhost:8001/version
```

### Running the collector

With the ingestion service running (above), in a separate terminal:

```bash
python -m collector.producer
```

This polls OpenSky's `/states/all` on `POLL_INTERVAL_SECONDS` (default 30s),
normalizes each state vector into the canonical event schema, and POSTs
batches to `TELEMETRY_TARGET_URL`.

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
