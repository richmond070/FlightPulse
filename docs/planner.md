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
  - [x] **Extraction-stage metadata** — `sql/003_extraction_log.sql` adds
        an `extraction_log` table (one row per collector poll cycle, not
        per event), per continuation doc section 3 ("record extraction
        failures" plus the named fields: `request_id`,
        `extraction_started_at`/`extraction_completed_at`,
        `source_observation_time`, `collector_version`, `record_count`,
        `request_scope`). `collector/producer.py` builds and POSTs one
        entry per cycle to a new `POST /extraction-log` endpoint
        (`ingestion/routes.py`), which enqueues a `process_extraction_log`
        job (`worker/processor.py`) persisted by
        `persistence.persist_extraction_log()` — same
        enqueue/validate/retry/dead-letter pattern as telemetry, so a
        burst of extraction-log writes never blocks the collector's
        polling loop. Covers both success *and* failure cycles (e.g.
        OpenSky unreachable), which is the part a stdout-only log would
        otherwise lose. Verified live end-to-end: `curl` → FastAPI →
        arq queue → worker → Postgres, plus direct success/failure job
        tests.
  - [x] **Schema/DB drift fix** — `sql/004_add_collector_id.sql` promotes
        `collector_id` from `ingestion.schemas.TelemetryEvent` to a real
        `raw_telemetry` column (with backfill from existing JSONB
        payloads). Deliberately *not* promoting the other event fields
        (`latitude`, `velocity_mps`, `baro_altitude_m`, etc.) the same
        way: those are flight measurements, not record-provenance
        metadata, and typing/promoting them is `stg_opensky_states`'s job
        in Phase 6 (dbt) per the continuation doc's "keep source-oriented
        storage separate from analytics models" (section 3, Load). They
        remain fully present in `raw_telemetry.payload` (JSONB) in the
        meantime — nothing is lost, just not yet indexed/typed at the
        raw layer.
- [x] Phase 6 — dbt
  - [x] `dbt/dbt_project.yml`, `dbt/profiles.yml` — reuses the same
        `POSTGRES_*` env vars as `.env.example` rather than introducing
        separate dbt credentials
  - [x] `dbt/models/staging/_sources.yml` — declares `raw_telemetry` and
        `extraction_log` as dbt sources (schema tracks `sql/*.sql` exactly)
  - [x] `dbt/models/staging/stg_opensky_states.sql` — types flight
        measurements out of `raw_telemetry.payload` JSONB for the first
        time (continuation doc section 7); one row in, one row out, no
        business logic yet
  - [x] `dbt/models/staging/_staging.yml` + two singular tests
        (`assert_valid_coordinates.sql`, `assert_valid_icao24.sql`) —
        continuation doc section 8 data-quality rules (coordinate bounds,
        ICAO24 format)
  - [x] `int_aircraft_activity` (intermediate business logic) —
        `altitude_km`, `velocity_kmh`, `vertical_rate_category`,
        `observation_date`/`observation_hour`, `aircraft_activity_status`,
        `telemetry_age_seconds` (source-observation-to-persistence latency,
        per continuation doc section 7)
  - [x] `dim_aircraft` — continuation doc section 7. No external aircraft
        registry exists upstream (no tail-number/manufacturer master
        data), so this is built as an aggregate over
        `int_aircraft_activity`: most-recently-observed `callsign` /
        `origin_country` plus first/last-seen bounds and an observation
        count, since callsign is a per-flight-leg telemetry field, not a
        static aircraft attribute. Documented in-model as the seam where
        a real registry would join in if one is ever sourced.
  - [x] `fact_aircraft_state` — one row per telemetry observation, same
        grain as `int_aircraft_activity`/`stg_opensky_states`/
        `raw_telemetry` (no aggregation); `ingestion_id` stays the
        natural key, `icao24` is the foreign key into `dim_aircraft`.
  - [x] `mart_aircraft_activity` — one row per
        `(icao24, observation_date, observation_hour)`. Answers section
        2's aircraft-count, activity-by-hour/day, most-frequent-callsign,
        and altitude/velocity/climb-descend-stable questions.
  - [x] `mart_airspace_activity` — geographic density/activity by time.
        "Geographic area" is approximated with a 1-degree lat/lon grid
        cell (~111km at the equator) since there's no
        administrative-boundary/airspace-sector reference data in this
        pipeline; rows with a null lat/lon (on-ground/no-position-fix)
        are excluded. Answers section 2's regional-density questions.
  - [x] `mart_telemetry_quality` — one row per
        `(observation_date, observation_hour)`, unioning fact-table
        quality signals with `extraction_log` poll-cycle outcomes (two
        different upstream grains). Covers freshness
        (`telemetry_age_seconds` percentiles), duplicate rate, and
        invalid-record rate per section 2/11 — dbt's own test pass rate
        lives in dbt's `run_results.json`, not in this mart (see Phase 7
        KPI reporting below).
  - [x] `dbt/models/marts/_marts.yml`, `_core.yml`, `_intermediate.yml` —
        schema docs + tests for every model above
- [ ] Phase 7 — Load & Resilience Testing (complete except 4.4, deliberately skipped)
  - [x] Step 1 — Fixture export: `replay/export_fixture.py` pulls a
        *contiguous* time window from real `raw_telemetry` (not a random
        sample), preserving genuine polling-cycle structure — burst
        shape, natural duplicate/redelivery patterns, realistic per-batch
        record counts — per continuation doc section 10 ("replay
        previously collected observations at controlled rates" without
        depending on OpenSky's live API limits). Output:
        `replay/fixtures/telemetry_sample.jsonl`.
  - [x] Step 2 — Replay engine: `replay/player.py` replays the fixture
        through `POST /telemetry` at a controllable rate
        (`--speed`, `--batch-size`, `--burst`), with `--mode fresh`
        (mints a fresh `run_salt` mixed into each event's `ingestion_id`
        so repeated runs don't collide with earlier ones — this is what
        fixed an earlier "0 newly inserted" bug where every replay run
        looked like a 100% duplicate of the last) vs `--mode replay`
        (byte-for-byte replay, to deliberately test redelivery/duplicate
        handling). `--concurrency` (default 3, matching the three
        ingestion backends) fans batches out across threads so
        round-robin load distribution is actually exercised, not just
        sequential single-threaded traffic.
  - [x] Step 3 — Metrics instrumentation: `replay/report.py` pulls each
        section 11 KPI from the source that already computes it
        correctly rather than recomputing anything — API latency
        (p50/p95/avg) and request/failure/retry counts from
        `GET /lb-metrics`; end-to-end freshness, duplicate rate, and
        invalid-record rate from `mart_telemetry_quality`; dbt test pass
        rate from dbt's own `run_results.json`.
  - [x] Step 4 — Fault injection: `replay/fault_injection.py`
        (subcommands: `single-api-failure`, `worker-failure`,
        `redis-failure`), each against continuation doc section 10's
        four required scenarios and measured against section 11's KPIs:
    - [x] **4.1 Single API failure** — kills one ingestion backend's
          real OS process (via `psutil`, matched by port) mid-replay,
          then restarts it. **PASS**: the killed backend is marked
          `UNHEALTHY` immediately (not on the next periodic tick, per
          Phase 3C), traffic keeps flowing on the two healthy backends
          with zero dropped batches, and the backend rejoins routing
          (`UNHEALTHY → RECOVERING → HEALTHY`) once restarted.
          **Windows gotcha**: after a hard `SIGKILL`, Windows can hold
          the TCP port in a lingering state for 30–60s before a new
          process can successfully bind and start accepting connections
          again — recovery time in a live run is dominated by this OS-level
          delay, not by the load balancer's own (fast, correct)
          detection/recovery logic. Not a bug; just a real-world number
          to expect if you see a longer-than-expected recovery time on
          Windows specifically.
    - [x] **4.2 Worker failure** — kills one `arq` worker process (found
          via `psutil` cmdline matching, since workers don't bind a
          port) while it holds a job, requires ≥2 workers running so a
          survivor exists. **PASS**, confirmed via direct worker-log
          inspection: the killed worker's in-flight job sat orphaned
          under `arq`'s own `in-progress` Redis key (TTL =
          `JOB_TIMEOUT_SECONDS` + 10s ≈ 100s) until that key expired,
          then a surviving worker picked it up as a retry and completed
          it exactly once (`ON CONFLICT DO NOTHING` correctly absorbing
          any already-inserted rows from the interrupted first attempt).
          **Known harness limitation**: `fault_injection.py`'s own
          automated pass/fail verdict for this scenario is unreliable —
          it scrapes `arq`'s `JobResult.finish_time` (a naive UTC
          datetime) and calls `.timestamp()` on it directly, which
          Python interprets as local time, not UTC; on a non-UTC
          machine this silently shifts every comparison and can produce
          a false "orphaned job never resolved" verdict even when the
          system recovered correctly. Left unfixed by choice — direct
          worker-log inspection is the accepted verification method for
          this scenario instead of trusting the harness's own summary.
    - [x] **4.3 Redis failure** — stops the Redis instance mid-replay
          (`--method docker` or `--method systemd`; the latter for a
          natively-installed Redis, e.g. via WSL2/systemd), probes
          `POST /telemetry` directly during the outage, then restarts
          Redis and confirms recovery with no process restarts needed.
          Tested against continuation doc section 9: *"Redis unavailable
          → fail visibly rather than claiming the job was queued."*
          **Result: no false success (spec's letter is satisfied), but
          two findings documented rather than fixed**:
          1. Failure is visible but slow — requests hang for
             ~10–12 seconds before failing, rather than failing fast.
             `ingestion/routes.py`'s `_get_arq_pool()`/`enqueue_job()`
             call has no explicit timeout guard around it, so the delay
             is `arq`'s/`redis-py`'s own internal connection retry/backoff
             running its course before an exception finally surfaces.
          2. The load balancer falsely marks healthy backends
             `UNHEALTHY` during a Redis outage, even though
             `GET /health` (`ingestion/routes.py`) never touches Redis
             at all — it's a one-line `{"status": "ok"}`. Root cause:
             event-loop starvation. A `/telemetry` request blocked on a
             dying Redis connection ties up the same async FastAPI
             process long enough that it can't promptly answer a
             concurrent `/health` ping either, so the load balancer's
             3-second health-check timeout trips on an otherwise-healthy
             process. Restarting that backend during a Redis outage
             would accomplish nothing — it was never actually broken.

          Both findings trace to the same missing timeout guard around
          the enqueue path. No fix applied — documented here as a known
          gap for a future session.
    - [ ] **4.4 Database slowdown** — **not done, environment
          limitation, not a failure.** The plan was a `tc`
          (Linux traffic-control) network-delay injection against the
          Postgres connection, but `tc` cannot reach this traffic: in
          this project's environment, Postgres runs natively on Windows
          and every client (backends, workers) connects via Windows'
          own `127.0.0.1`, a path WSL2's `tc` has no visibility into —
          the same class of cross-namespace gap as the Redis/WSL
          `localhost` issue below, except here there's no `wsl.exe`
          bridge available since Postgres itself isn't running inside
          WSL. Left undone rather than switching approaches (e.g. a
          `pg_sleep`-based slowdown, or a Windows-native packet-shaping
          tool) — revisit if this scenario becomes a priority later.
  - [x] Step 5 — Documentation (this section)
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

### Running dbt (Phase 6, in progress)

```bash
pip install -r requirements.txt   # now includes dbt-postgres
export DBT_PROFILES_DIR=dbt       # so you don't need --profiles-dir every time

cd dbt
dbt debug --project-dir .         # confirms the Postgres connection works
dbt run --project-dir . --select stg_opensky_states int_aircraft_activity
dbt test --project-dir . --select stg_opensky_states int_aircraft_activity
```

`dbt debug` should show `Connection test: OK`. `dbt run` should build two
views: `<schema>_staging.stg_opensky_states` and
`<schema>_intermediate.int_aircraft_activity` (dbt prefixes each model's
`+schema:` config from `dbt_project.yml` onto your profile's base
`schema: public`, e.g. `public_staging`, `public_intermediate`).
Spot-check both:

```bash
psql -U flightpulse -h localhost -d flightpulse \
  -c "SELECT icao24, callsign, latitude, longitude, last_contact_at FROM public_staging.stg_opensky_states LIMIT 5;"

psql -U flightpulse -h localhost -d flightpulse \
  -c "SELECT icao24, altitude_km, velocity_kmh, vertical_rate_category, aircraft_activity_status, telemetry_age_seconds FROM public_intermediate.int_aircraft_activity LIMIT 5;"
```

Two models exist so far — `dbt run`/`dbt test` with no `--select` will
currently build/test both of them.

### Running Phase 7 load & resilience tests

Requires the full stack running: Postgres, Redis, all three ingestion
backends, the load balancer, and at least 2 `arq` workers (some scenarios
specifically need ≥2 to have a survivor).

```bash
export PYTHONPATH=$(pwd)

# Step 1: export a fixture from real raw_telemetry
python -m replay.export_fixture

# Step 2/3: replay it and pull a KPI report
python -m replay.player --mode fresh --concurrency 3
python -m replay.report

# Step 4: fault injection (one scenario at a time)
python -m replay.fault_injection single-api-failure --backend-port 8002
python -m replay.fault_injection worker-failure
python -m replay.fault_injection redis-failure --method docker    # Redis in Docker
python -m replay.fault_injection redis-failure --method systemd   # Redis via systemd
```

**Windows + WSL2 + Redis note:** if Redis runs as a native systemd
service inside WSL2 while everything else (Postgres, FastAPI backends,
load balancer, workers) runs natively on Windows, run
`fault_injection.py` itself from **Windows** (Git Bash), not from inside
WSL. WSL2's own `localhost` is a separate network namespace from
Windows' — a script run inside WSL cannot reach the Windows-hosted load
balancer on `localhost:8080` at all, so every probe fails identically
before, during, and after the fault regardless of what's actually being
tested. `redis-failure --method systemd` handles this correctly by
shelling out to `wsl.exe sudo systemctl stop/start redis-server` from
Windows, so the script keeps running where it can actually reach the
stack while still controlling the WSL-side Redis. This requires
passwordless sudo scoped to just those two commands:

```bash
# inside WSL, one-time setup
sudo visudo -f /etc/sudoers.d/flightpulse-redis-control
# add: youruser ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop redis-server, /usr/bin/systemctl start redis-server
sudo chmod 440 /etc/sudoers.d/flightpulse-redis-control
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
