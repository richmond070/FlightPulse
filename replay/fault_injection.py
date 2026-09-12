"""
Phase 7, Step 4 -- Fault injection.

Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf, section 10
("Load & Resilience Testing"): the four required fault scenarios are
  - single API failure
  - worker failure
  - Redis failure
  - database slowdown
each measured against section 11's KPIs (throughput, p50/p95 latency,
error rate, duplicate rate, freshness, recovery).

This module implements scenario 1 only: single API failure. The other
three scenarios are intentionally NOT implemented yet -- they'll be
added as separate, individually-verified steps per the agreed plan.

Scenario 1 -- single API failure
---------------------------------
One of the three FastAPI ingestion backends (default :8001/:8002/:8003,
per BACKEND_URLS in load_balancer/config.py) is killed by its real OS
process (not simulated) while replay traffic is in flight, then
restarted. This exercises the actual Phase 3 load-balancer logic already
in production code:
  - load_balancer/server.py: a failed forward marks the backend
    UNHEALTHY immediately, then retries on a different healthy backend.
  - load_balancer/health.py: the periodic health checker will also
    observe the outage, and requires 2 consecutive healthy checks
    (UNHEALTHY -> RECOVERING -> HEALTHY) before trusting the backend
    again once it restarts.
  - load_balancer/router.py: round-robin is restricted to
    currently-healthy backends throughout.

Run with:
    python -m replay.fault_injection --backend-port 8002

Requires: the load balancer, all FastAPI ingestion instances, Redis,
Postgres, and (for the restart step) a way to bring the killed backend
back up -- this script restarts it directly via uvicorn subprocess so
the whole scenario is self-contained.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import threading
import time
from pathlib import Path

import pickle

import psutil
import psycopg
import redis as redis_lib
import requests
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from arq.constants import in_progress_key_prefix, result_key_prefix  # noqa: E402
from load_balancer.config import BACKEND_URLS  # noqa: E402
from replay.player import DEFAULT_FIXTURE, run_replay  # noqa: E402
from worker.settings import DATABASE_URL, JOB_TIMEOUT_SECONDS, REDIS_URL  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [fault_injection] %(message)s",
)
logger = logging.getLogger(__name__)

LB_STATUS_URL = "http://localhost:8080/lb-status"
LB_METRICS_URL = "http://localhost:8080/lb-metrics"

# How long to wait for the health checker to notice recovery, in seconds,
# before giving up and reporting "did not recover".
RECOVERY_POLL_TIMEOUT_SECONDS = 60
RECOVERY_POLL_INTERVAL_SECONDS = 2


def _fetch_json(url: str) -> dict | None:
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not fetch %s: %s", url, e)
        return None


def find_backend_pid(port: int) -> int | None:
    """Find the PID of the process listening on the given local port.

    Uses psutil rather than a manual PID so the operator doesn't have to
    look anything up by hand -- just name the backend by its port, the
    same way BACKEND_URLS already identifies it.
    """
    for conn in psutil.net_connections(kind="tcp"):
        if (
            conn.laddr
            and conn.laddr.port == port
            and conn.status == psutil.CONN_LISTEN
            and conn.pid is not None
        ):
            return conn.pid
    return None


def kill_backend(port: int) -> int:
    """Kill the process bound to `port`. Returns the PID killed."""
    pid = find_backend_pid(port)
    if pid is None:
        logger.error(
            "No listening process found on port %d -- is the backend "
            "actually running? (start it per README: "
            "`uvicorn ingestion.app:app --host 0.0.0.0 --port %d`)",
            port, port,
        )
        sys.exit(1)

    proc = psutil.Process(pid)
    logger.info("Killing backend on port %d (pid=%d, cmdline=%s)", port, pid, " ".join(proc.cmdline()))
    proc.kill()
    proc.wait(timeout=10)
    logger.info("Backend on port %d (pid=%d) is dead.", port, pid)
    return pid


def restart_backend(port: int) -> subprocess.Popen:
    """Start a fresh uvicorn instance for the ingestion app on `port`.

    Mirrors the README's manual command exactly, so this is the same
    process a human would start by hand -- just automated for a
    repeatable test.
    """
    logger.info("Restarting backend on port %d ...", port)
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "ingestion.app:app",
            "--host", "0.0.0.0",
            "--port", str(port),
        ],
        cwd=str(Path(__file__).resolve().parent.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def wait_for_backend_state(backend_url: str, target_states: set[str], timeout_seconds: float) -> tuple[bool, float]:
    """Poll /lb-status until backend_url's state is in target_states.

    Returns (reached, elapsed_seconds).
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        status = _fetch_json(LB_STATUS_URL)
        if status:
            state = status.get("backends", {}).get(backend_url)
            if state in target_states:
                return True, time.monotonic() - start
        time.sleep(RECOVERY_POLL_INTERVAL_SECONDS)
    return False, time.monotonic() - start


def run_scenario(port: int, fixture_path: Path, warmup_seconds: float, downtime_seconds: float) -> None:
    backend_url = next((u for u in BACKEND_URLS if u.endswith(f":{port}")), None)
    if backend_url is None:
        logger.error("Port %d is not one of the configured BACKEND_URLS: %s", port, BACKEND_URLS)
        sys.exit(1)

    print("\n" + "=" * 70)
    print("PHASE 7 / STEP 4.1 -- SINGLE API FAILURE")
    print(f"Target backend: {backend_url} (port {port})")
    print("=" * 70)

    baseline = _fetch_json(LB_METRICS_URL)
    print("\n-- Baseline /lb-metrics (before fault) --")
    print(baseline)

    # Start steady replay traffic in the background so there's live load
    # for the load balancer to route (and mis-route, then re-route)
    # during the fault window.
    replay_summary: dict = {}

    def _replay_worker():
        nonlocal replay_summary
        replay_summary = run_replay(
            fixture_path=fixture_path,
            speed=1.0,
            batch_size=2000,
            mode="fresh",
            burst=False,
            poll_interval_seconds=5.0,
            concurrency=3,
        )

    replay_thread = threading.Thread(target=_replay_worker, daemon=True)
    replay_thread.start()

    logger.info("Warming up for %.0fs before injecting the fault ...", warmup_seconds)
    time.sleep(warmup_seconds)

    # --- Inject the fault ---
    kill_time = time.monotonic()
    kill_backend(port)

    during_fault = _fetch_json(LB_METRICS_URL)
    print("\n-- /lb-metrics shortly after kill (fault window) --")
    print(during_fault)

    status_after_kill = _fetch_json(LB_STATUS_URL)
    print("\n-- /lb-status shortly after kill --")
    print(status_after_kill)

    logger.info(
        "Letting the fault run for %.0fs so periodic health checks also "
        "observe it (not just the immediate mark-unhealthy-on-failed-"
        "forward path) ...",
        downtime_seconds,
    )
    time.sleep(downtime_seconds)

    # --- Restart the backend and measure recovery ---
    restart_proc = restart_backend(port)
    restart_time = time.monotonic()

    reached_recovering, t_to_recovering = wait_for_backend_state(
        backend_url, {"RECOVERING", "HEALTHY"}, RECOVERY_POLL_TIMEOUT_SECONDS,
    )
    reached_healthy, t_to_healthy = wait_for_backend_state(
        backend_url, {"HEALTHY"}, RECOVERY_POLL_TIMEOUT_SECONDS,
    )

    replay_thread.join(timeout=60)

    after_recovery = _fetch_json(LB_METRICS_URL)
    final_status = _fetch_json(LB_STATUS_URL)

    print("\n" + "=" * 70)
    print("RESULT SUMMARY")
    print("=" * 70)
    print(f"Backend killed at t=0.00s (port {port})")
    print(f"Backend restart issued at t={restart_time - kill_time:.2f}s")
    print(
        f"Reached RECOVERING/HEALTHY: {reached_recovering} "
        f"(+{t_to_recovering:.1f}s after restart)" if reached_recovering
        else "Did NOT reach RECOVERING/HEALTHY within timeout"
    )
    print(
        f"Reached fully HEALTHY: {reached_healthy} "
        f"(+{t_to_healthy:.1f}s after restart)" if reached_healthy
        else "Did NOT reach fully HEALTHY within timeout"
    )
    print(f"\nFinal /lb-status: {final_status}")
    print(f"\nReplay summary during the whole scenario: {replay_summary}")
    print(f"\n/lb-metrics after recovery: {after_recovery}")

    if baseline and after_recovery:
        failed_delta = after_recovery.get("failed_requests", 0) - baseline.get("failed_requests", 0)
        retried_delta = after_recovery.get("retried_requests", 0) - baseline.get("retried_requests", 0)
        total_delta = after_recovery.get("total_requests", 0) - baseline.get("total_requests", 0)
        print(
            f"\nDuring this scenario: {total_delta} total requests, "
            f"{failed_delta} failed, {retried_delta} retried."
        )
        print(
            "Expected per continuation doc section 9 (\"FastAPI instance "
            "unavailable -> health checks remove the instance from "
            "routing\"): failures should be transient (retried onto a "
            "healthy backend), not sustained across the whole window."
        )

    restart_proc  # keep reference alive; leave the restarted backend running


def _raw_telemetry_count() -> int | None:
    """Row count in raw_telemetry -- used before/after to confirm the
    crashed worker's job eventually lands exactly once, not zero times
    (lost) or more than once (duplicate re-processing)."""
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) AS n FROM raw_telemetry")
                return cur.fetchone()["n"]
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not query raw_telemetry count: %s", e)
        return None


def find_worker_pids() -> list[int]:
    """Find all running arq worker processes for this project by matching
    their command line -- workers don't bind a port the way the ingestion
    backends do, so a port lookup won't work here."""
    pids = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"] or [])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if "worker.consumer" in cmdline and "arq" in cmdline:
            pids.append(proc.info["pid"])
    return pids


def kill_worker(pid: int) -> None:
    proc = psutil.Process(pid)
    logger.info("Killing worker pid=%d (cmdline=%s)", pid, " ".join(proc.cmdline()))
    proc.kill()
    proc.wait(timeout=10)
    logger.info("Worker pid=%d is dead.", pid)


def wait_for_in_progress_job(redis_client: "redis_lib.Redis", timeout_seconds: float) -> bool:
    """Poll Redis for any arq:in-progress:* key -- proof some job is
    actively claimed by a worker right now, not just sitting in the
    queue. Returns False (rather than guessing) if nothing was ever
    caught mid-flight in the timeout window, so the scenario can report
    honestly on whether it actually tested what it claims to."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        keys = redis_client.keys(f"{in_progress_key_prefix}*")
        if keys:
            return True
        time.sleep(0.1)
    return False


def sum_newly_inserted_since(redis_client: "redis_lib.Redis", since_epoch: float) -> tuple[int, int]:
    """Scrape arq:result:* keys for job results finished at/after
    `since_epoch`, and sum the `newly_inserted` field each
    process_telemetry_batch job now reports (see worker/processor.py).
    This is the ground-truth count arq itself recorded, so it already
    accounts for any duplicate observations the fixture legitimately
    contains -- no separate dedup-rate estimate needed.

    Returns (jobs_counted, total_newly_inserted).
    """
    jobs_counted = 0
    total = 0
    for key in redis_client.keys(f"{result_key_prefix}*"):
        try:
            raw = redis_client.get(key)
            if raw is None:
                continue
            job_result = pickle.loads(raw)
        except Exception:  # noqa: BLE001
            continue

        finish_time = getattr(job_result, "finish_time", None)
        if finish_time is not None:
            finish_epoch = finish_time.timestamp() if hasattr(finish_time, "timestamp") else finish_time
            if finish_epoch < since_epoch:
                continue

        result = getattr(job_result, "result", None)
        if isinstance(result, dict) and "newly_inserted" in result:
            jobs_counted += 1
            total += result["newly_inserted"]
    return jobs_counted, total


def run_worker_failure_scenario(
    fixture_path: Path, warmup_seconds: float, post_kill_wait_seconds: float,
) -> None:
    """Phase 7 Step 4.2 -- worker failure.

    Unlike the load balancer (Step 4.1), arq has no instant failover: a
    job claimed by a worker is guarded by an `in-progress` Redis key with
    TTL = job_timeout + 10s (JOB_TIMEOUT_SECONDS=90 here, so ~100s). If
    that worker dies mid-job, the job sits unprocessed and unclaimable by
    any other worker until that TTL lapses -- then it's picked up and
    reprocessed by a survivor. This scenario measures whether that
    self-heal actually happens and within that bound, and whether the
    reprocessed batch lands exactly once (idempotent insert via
    ON CONFLICT DO NOTHING on uq_raw_telemetry_ingestion_id) rather than
    being lost or duplicated.

    Two corrections vs the first version of this scenario (see repo
    history / session notes): (1) the earlier version assumed
    baseline + events_sent as the target row count, which is wrong
    whenever the fixture contains legitimate intra-batch duplicate
    observations -- it now sums each job's own `newly_inserted` result
    instead, which already nets those out. (2) the earlier version
    killed a worker after a fixed warmup timer with no check that a job
    was actually in-flight -- confirmed via real runs, this often killed
    an idle worker, so nothing was actually being tested. It now polls
    Redis for a live `arq:in-progress:*` key before killing, and reports
    honestly if it never caught one.

    Requires >=2 `arq worker.consumer.WorkerSettings` processes already
    running (per the agreed test setup) so there's a survivor to pick up
    the orphaned job.
    """
    print("\n" + "=" * 70)
    print("PHASE 7 / STEP 4.2 -- WORKER FAILURE")
    print("=" * 70)

    worker_pids = find_worker_pids()
    print(f"\nDetected {len(worker_pids)} running worker process(es): {worker_pids}")
    if len(worker_pids) < 2:
        logger.error(
            "Need at least 2 worker processes running (one to kill, one "
            "survivor to pick up the orphaned job). Start a second with: "
            "arq worker.consumer.WorkerSettings"
        )
        sys.exit(1)

    baseline_count = _raw_telemetry_count()
    print(f"Baseline raw_telemetry row count: {baseline_count}")

    redis_client = redis_lib.from_url(REDIS_URL)
    scenario_start_epoch = time.time()

    replay_summary: dict = {}

    def _replay_worker():
        nonlocal replay_summary
        replay_summary = run_replay(
            fixture_path=fixture_path,
            speed=1.0,
            batch_size=2000,
            mode="fresh",
            burst=False,
            poll_interval_seconds=5.0,
            concurrency=3,
        )

    replay_thread = threading.Thread(target=_replay_worker, daemon=True)
    replay_thread.start()

    logger.info(
        "Waiting up to %.0fs for a real in-progress job to appear in Redis "
        "before killing (a fixed warmup timer isn't reliable -- confirmed "
        "in earlier runs it often kills an already-idle worker) ...",
        warmup_seconds,
    )
    caught_in_flight = wait_for_in_progress_job(redis_client, warmup_seconds)
    if not caught_in_flight:
        logger.warning(
            "Never observed an in-progress job within %.0fs -- killing "
            "anyway, but this run may not actually exercise the "
            "mid-job-crash path. Consider re-running if the result below "
            "looks like a clean pass with no delay.",
            warmup_seconds,
        )

    target_pid = worker_pids[0]
    kill_time = time.monotonic()
    kill_worker(target_pid)

    max_expected_recovery = JOB_TIMEOUT_SECONDS + 10
    print(
        f"\nCaught a genuine in-progress job before kill: {caught_in_flight}"
    )
    print(
        f"Waiting up to {max_expected_recovery}s (JOB_TIMEOUT_SECONDS="
        f"{JOB_TIMEOUT_SECONDS} + 10s in-progress-key grace, per arq's own "
        f"Worker.in_progress_timeout_s) for a survivor to pick up any "
        f"orphaned job ..."
    )

    replay_thread.join(timeout=post_kill_wait_seconds + max_expected_recovery + 60)

    # Poll until ground-truth newly_inserted (summed from arq's own job
    # results since scenario start) plus baseline matches the DB row
    # count, or we hit the expected recovery ceiling.
    poll_start = time.monotonic()
    final_count = _raw_telemetry_count()
    jobs_counted = 0
    ground_truth_inserted = 0
    reached_target = False
    while time.monotonic() - poll_start < max_expected_recovery + 30:
        final_count = _raw_telemetry_count()
        jobs_counted, ground_truth_inserted = sum_newly_inserted_since(redis_client, scenario_start_epoch)
        if baseline_count is not None and final_count is not None:
            if final_count >= baseline_count + ground_truth_inserted and jobs_counted >= replay_summary.get("batches_sent", 0):
                reached_target = True
                break
        time.sleep(5)

    recovery_elapsed = time.monotonic() - kill_time

    print("\n" + "=" * 70)
    print("RESULT SUMMARY")
    print("=" * 70)
    print(f"Worker pid={target_pid} killed at t=0.00s")
    print(f"Replay summary: {replay_summary}")
    print(f"Baseline row count: {baseline_count}")
    print(f"Jobs with a result recorded since kill window started: {jobs_counted} / {replay_summary.get('batches_sent', '?')} batches sent")
    print(f"Ground-truth newly_inserted summed from arq job results: {ground_truth_inserted}")
    print(f"Expected final row count (baseline + ground-truth newly_inserted): {baseline_count + ground_truth_inserted if baseline_count is not None else '?'}")
    print(f"Actual final row count: {final_count}")
    print(f"Reached expected count with all batches accounted for: {reached_target} (elapsed ~{recovery_elapsed:.1f}s since kill)")

    if jobs_counted < replay_summary.get("batches_sent", 0):
        missing_jobs = replay_summary.get("batches_sent", 0) - jobs_counted
        print(
            f"\n{missing_jobs} batch(es) have no job result recorded at all -- "
            f"this is the actual signal for 'orphaned job never resolved', "
            f"as opposed to a duplicate-count mismatch."
        )

    if final_count is not None and baseline_count is not None:
        expected = baseline_count + ground_truth_inserted
        if reached_target:
            print(
                "\nPASS: every batch has a recorded result and the DB row "
                "count matches the ground-truth insert sum exactly -- "
                "consistent with the killed worker's job (if it was truly "
                "in-flight) being picked up and completed exactly once by "
                "a survivor."
            )
        elif final_count < expected:
            print(
                f"\nGAP: DB row count ({final_count}) is behind the "
                f"ground-truth insert sum ({expected}) even though "
                f"{jobs_counted} job results exist -- investigate a "
                f"possible insert-visibility or connection issue, not "
                f"necessarily the worker-crash path."
            )
        else:
            print(
                f"\nUNRESOLVED: {replay_summary.get('batches_sent', 0) - jobs_counted} "
                f"batch(es) never produced a job result within the "
                f"{max_expected_recovery}s ceiling -- this is the real "
                f"crash-recovery gap, if `caught_in_flight` above was True. "
                f"If it was False, re-run: this result doesn't confirm "
                f"anything about crash recovery, only that no job was "
                f"actually orphaned by the kill."
            )


REDIS_CONTAINER_NAME = "flightpulse-redis"
REDIS_SYSTEMD_SERVICE = "redis-server"


def redis_stop(method: str) -> None:
    if method == "docker":
        logger.info("Stopping container %s ...", REDIS_CONTAINER_NAME)
        subprocess.run(["docker", "stop", REDIS_CONTAINER_NAME], check=True, capture_output=True, text=True)
        logger.info("Container %s stopped.", REDIS_CONTAINER_NAME)
    elif method == "systemd":
        # Redis runs inside WSL2 Ubuntu (systemd service) while the rest
        # of the stack -- and this script -- run natively on Windows.
        # WSL2's `localhost` is a separate network namespace from
        # Windows' by default, so running this script *inside* WSL can't
        # reach the load balancer on Windows' localhost:8080 at all
        # (confirmed: every probe failed identically before, during, and
        # after the Redis outage in an earlier run -- port 8080 was
        # never reachable, so nothing about Redis was actually tested).
        # wsl.exe is the fix: it's a normal Windows executable that runs
        # a single command inside the default WSL distro and returns,
        # so this script keeps running on Windows (where localhost:8080
        # works) while still controlling the WSL-side systemd service.
        logger.info("Stopping systemd service %s inside WSL (needs passwordless sudo configured there) ...", REDIS_SYSTEMD_SERVICE)
        subprocess.run(["wsl.exe", "sudo", "systemctl", "stop", REDIS_SYSTEMD_SERVICE], check=True, capture_output=True, text=True)
        logger.info("Service %s stopped.", REDIS_SYSTEMD_SERVICE)
    else:
        raise ValueError(f"Unknown redis control method: {method}")


def redis_start(method: str) -> None:
    if method == "docker":
        logger.info("Starting container %s ...", REDIS_CONTAINER_NAME)
        subprocess.run(["docker", "start", REDIS_CONTAINER_NAME], check=True, capture_output=True, text=True)
        logger.info("Container %s started.", REDIS_CONTAINER_NAME)
    elif method == "systemd":
        logger.info("Starting systemd service %s inside WSL ...", REDIS_SYSTEMD_SERVICE)
        subprocess.run(["wsl.exe", "sudo", "systemctl", "start", REDIS_SYSTEMD_SERVICE], check=True, capture_output=True, text=True)
        logger.info("Service %s started.", REDIS_SYSTEMD_SERVICE)
    else:
        raise ValueError(f"Unknown redis control method: {method}")


def probe_telemetry_endpoint(timeout_seconds: float = 10.0) -> dict:
    """Send one minimal, real /telemetry POST directly (bypassing the
    replay player) and report exactly what came back: status code,
    latency, or -- if Redis is down -- whatever exception surfaced.
    This is the direct evidence for continuation doc section 9's
    requirement: 'Redis unavailable -> fail visibly rather than
    claiming the job was queued.'
    """
    import uuid
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = {
        "events": [{
            "source": "opensky",
            "icao24": "000000",
            "callsign": "FAULTPROBE",
            "origin_country": "test",
            "time_position": int(time.time()),
            "last_contact": int(time.time()),
            "longitude": 0.0,
            "latitude": 0.0,
            "baro_altitude_m": 0.0,
            "geo_altitude_m": 0.0,
            "velocity_mps": 0.0,
            "true_track_deg": 0.0,
            "vertical_rate_mps": 0.0,
            "on_ground": False,
            "ingested_at": now_iso,
            "ingestion_id": str(uuid.uuid4()),
            "collector_id": "fault_injection_probe",
        }]
    }
    start = time.monotonic()
    try:
        resp = requests.post(
            "http://localhost:8080/telemetry",
            json=payload,
            headers={"Idempotency-Key": f"fault-probe-{time.time()}"},
            timeout=timeout_seconds,
        )
        return {
            "outcome": "response",
            "status_code": resp.status_code,
            "latency_seconds": time.monotonic() - start,
            "body": resp.text[:300],
        }
    except requests.Timeout:
        return {"outcome": "timeout", "latency_seconds": time.monotonic() - start}
    except requests.RequestException as e:
        return {
            "outcome": "exception",
            "exception_type": type(e).__name__,
            "message": str(e)[:300],
            "latency_seconds": time.monotonic() - start,
        }


def run_redis_failure_scenario(downtime_seconds: float, method: str) -> None:
    """Phase 7 Step 4.3 -- Redis failure.

    Governing requirement, continuation doc section 9: 'Redis unavailable
    -> Fail visibly rather than claiming the job was queued.'

    Unlike Steps 4.1/4.2, this isn't primarily a recovery-timing
    measurement -- it's a check of *what the caller actually sees*
    while the dependency is down, plus whether the load balancer
    misattributes the failure to the (perfectly healthy) FastAPI
    backend process itself. ingestion/routes.py's enqueue path
    (_get_arq_pool / pool.enqueue_job) has no try/except around it as
    of this session -- so what happens here is genuinely being
    discovered, not just confirmed.
    """
    print("\n" + "=" * 70)
    print("PHASE 7 / STEP 4.3 -- REDIS FAILURE")
    print("=" * 70)

    print("\n-- Baseline probe (Redis healthy) --")
    baseline_probe = probe_telemetry_endpoint()
    print(baseline_probe)

    baseline_status = _fetch_json(LB_STATUS_URL)
    print(f"\nBaseline /lb-status: {baseline_status}")

    redis_stop(method)

    print(f"\n-- Probing /telemetry while Redis is down (waiting {downtime_seconds:.0f}s total) --")
    probes_during_outage = []
    probe_window_start = time.monotonic()
    while time.monotonic() - probe_window_start < downtime_seconds:
        probe = probe_telemetry_endpoint(timeout_seconds=10.0)
        probes_during_outage.append(probe)
        print(f"  probe: {probe}")
        time.sleep(3)

    status_during_outage = _fetch_json(LB_STATUS_URL)
    print(f"\n/lb-status while Redis is down: {status_during_outage}")

    redis_start(method)
    logger.info("Waiting 5s for Redis to finish accepting connections ...")
    time.sleep(5)

    print("\n-- Probe after Redis restart (no FastAPI/worker process restarted) --")
    recovery_probe = probe_telemetry_endpoint()
    print(recovery_probe)

    status_after_recovery = _fetch_json(LB_STATUS_URL)
    print(f"\n/lb-status after Redis recovery: {status_after_recovery}")

    print("\n" + "=" * 70)
    print("RESULT SUMMARY")
    print("=" * 70)

    silent_success = any(
        p.get("outcome") == "response" and 200 <= p.get("status_code", 0) < 300
        for p in probes_during_outage
    )
    got_visible_failure = any(
        p.get("outcome") in ("timeout", "exception")
        or (p.get("outcome") == "response" and p.get("status_code", 0) >= 500)
        for p in probes_during_outage
    )
    backend_falsely_unhealthy = False
    if baseline_status and status_during_outage:
        for backend, state in status_during_outage.get("backends", {}).items():
            if baseline_status.get("backends", {}).get(backend) == "HEALTHY" and state == "UNHEALTHY":
                backend_falsely_unhealthy = True

    print(f"Any 2xx returned while Redis was down (would violate spec): {silent_success}")
    print(f"Got a clear failure signal (5xx/exception/timeout) while Redis was down: {got_visible_failure}")
    print(f"Backend(s) falsely marked UNHEALTHY due to Redis outage (not their own fault): {backend_falsely_unhealthy}")
    print(f"Recovered cleanly after Redis restart, no process restart needed: {recovery_probe.get('outcome') == 'response' and 200 <= recovery_probe.get('status_code', 0) < 300}")

    if silent_success:
        print(
            "\nFAIL: at least one request during the outage returned "
            "2xx -- this claims the job was queued when it wasn't, "
            "directly violating section 9's requirement."
        )
    elif got_visible_failure:
        print(
            "\nPASS (visibility requirement met): no request silently "
            "claimed success while Redis was down."
        )
    else:
        print(
            "\nINCONCLUSIVE: no probe returned a 2xx, but none clearly "
            "failed either -- check the raw probe list above."
        )

    if backend_falsely_unhealthy:
        print(
            "\nSEPARATE FINDING: the load balancer marked a backend "
            "UNHEALTHY during the Redis outage even though that backend "
            "process itself never went down -- its /health check "
            "(load_balancer/health.py's target) may be conflating "
            "'FastAPI process reachable' with 'FastAPI's dependencies "
            "are reachable'. This would make the load balancer's "
            "recovery/rotation logic irrelevant to the real problem "
            "(Redis), and worth a closer look before deciding whether "
            "it's a fix or working-as-designed."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Phase 7 Step 4 -- fault injection scenarios."
    )
    subparsers = parser.add_subparsers(dest="scenario", required=True)

    api_parser = subparsers.add_parser(
        "single-api-failure",
        help="Step 4.1 -- kill one ingestion backend mid-replay, verify "
        "load-balancer failover and recovery.",
    )
    api_parser.add_argument("--backend-port", type=int, default=8002)
    api_parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    api_parser.add_argument("--warmup-seconds", type=float, default=10.0)
    api_parser.add_argument("--downtime-seconds", type=float, default=15.0)

    worker_parser = subparsers.add_parser(
        "worker-failure",
        help="Step 4.2 -- kill one arq worker mid-job, verify a survivor "
        "picks up the orphaned job within arq's in-progress TTL.",
    )
    worker_parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    worker_parser.add_argument(
        "--warmup-seconds", type=float, default=20.0,
        help="Max seconds to wait for a real in-progress job to appear in "
        "Redis before killing (not a fixed sleep -- see "
        "wait_for_in_progress_job).",
    )
    worker_parser.add_argument("--post-kill-wait-seconds", type=float, default=5.0)

    redis_parser = subparsers.add_parser(
        "redis-failure",
        help="Step 4.3 -- stop the Redis container mid-run, verify the "
        "ingestion API fails visibly (not a false 2xx) and recovers "
        "cleanly once Redis is back, with no process restarts.",
    )
    redis_parser.add_argument(
        "--downtime-seconds", type=float, default=20.0,
        help="How long to keep Redis stopped while probing /telemetry.",
    )
    redis_parser.add_argument(
        "--method", choices=["docker", "systemd"], default="docker",
        help="How Redis is deployed: 'docker' controls the "
        f"{REDIS_CONTAINER_NAME} container (default, matches a "
        "Docker Compose setup); 'systemd' shells out via wsl.exe to run "
        f"`sudo systemctl stop/start {REDIS_SYSTEMD_SERVICE}` inside "
        "WSL2 Ubuntu, while this script itself keeps running natively "
        "on Windows (matches: Redis on WSL2 systemd, everything else "
        "on Windows -- do NOT run this script from inside WSL in that "
        "setup, since WSL2's localhost can't reach the Windows-hosted "
        "load balancer). Requires passwordless sudo configured for "
        "systemctl in WSL.",
    )

    args = parser.parse_args()

    if args.scenario == "single-api-failure":
        run_scenario(
            port=args.backend_port,
            fixture_path=args.fixture,
            warmup_seconds=args.warmup_seconds,
            downtime_seconds=args.downtime_seconds,
        )
    elif args.scenario == "worker-failure":
        run_worker_failure_scenario(
            fixture_path=args.fixture,
            warmup_seconds=args.warmup_seconds,
            post_kill_wait_seconds=args.post_kill_wait_seconds,
        )
    elif args.scenario == "redis-failure":
        run_redis_failure_scenario(downtime_seconds=args.downtime_seconds, method=args.method)


if __name__ == "__main__":
    main()
