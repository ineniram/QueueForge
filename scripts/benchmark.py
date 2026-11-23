"""Run isolated QueueForge scaling trials and repeat the local Failure Lab drill."""

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
from pathlib import Path
import platform
import secrets
import statistics
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
WORKERS = (1, 3, 5, 10)
SLEEP = {"type": "sleep", "payload": {"duration_ms": 120}}
COMPUTE = {"type": "compute", "payload": {"limit": 2_000_000}}
CLIENTS = 8
POLL_SECONDS = .025
TIMING_TOLERANCE_SECONDS = 1.0
MAX_EXTRA_TRIALS = 4


def command(*args, env=None, quiet=False):
    return subprocess.run(args, cwd=ROOT, env=env, text=True, check=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE if quiet else None).stdout.strip()


def request(port, path, data=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                 data=json.dumps(data).encode() if data is not None else None,
                                 headers=headers)
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.load(response)


def wait_for(check, timeout, interval=.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result is not None:
            return result
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for the expected cluster or workload state")


def idle(port, count):
    workers = request(port, "/api/workers")
    live = {w["id"] for w in workers if w["health"] == "healthy"}
    metrics = request(port, "/api/metrics")
    if len(live) == count and metrics["queued"] == metrics["running"] == 0:
        return live
    return None


def workload(name, count):
    if name == "sleep":
        return [SLEEP] * count
    if name == "compute":
        return [COMPUTE] * count
    return [SLEEP if index % 2 == 0 else COMPUTE for index in range(count)]


def percentile(values):
    return sorted(values)[math.ceil(.95 * len(values)) - 1]


def latency(values):
    return {"mean_ms": statistics.mean(values), "p95_ms": percentile(values)}


def comparable(trial):
    return abs(trial["total_seconds"] - trial["observed_seconds"]) <= TIMING_TOLERANCE_SECONDS


def timestamp(value):
    return dt.datetime.fromisoformat(value).timestamp()


def measure(port, workers, inputs, pool):
    run_id = uuid.uuid4().hex
    start = time.monotonic()
    jobs = list(pool.map(lambda index: request(port, "/api/jobs", {
        **inputs[index], "retry": {"max_attempts": 1}
    }, f"bench-{run_id}-{index}"), range(len(inputs))))
    submit_seconds = time.monotonic() - start
    ids = {job["id"] for job in jobs}
    if len(ids) != len(inputs):
        raise RuntimeError("Submission did not return a distinct ID for every job")

    def finished():
        page = request(port, "/api/jobs?limit=100")["jobs"]
        selected = [job for job in page if job["id"] in ids]
        if len(selected) == len(ids) and all(job["status"] in ("completed", "failed") for job in selected):
            return selected
        return None

    terminal = wait_for(finished, 100, POLL_SECONDS)
    observed_seconds = time.monotonic() - start
    if any(job["worker_id"] not in workers or job["attempt_count"] != 1 for job in terminal):
        raise RuntimeError("Unexpected worker or attempt count during an isolated trial")

    records = [{
        "id": job["id"], "type": job["type"], "worker_id": job["worker_id"], "status": job["status"],
        "created_at": job["created_at"], "started_at": job["started_at"], "completed_at": job["completed_at"],
        "attempt_count": job["attempt_count"], "error": job["error"],
        "queue_ms": (timestamp(job["started_at"]) - timestamp(job["created_at"])) * 1000,
        "execution_ms": (timestamp(job["completed_at"]) - timestamp(job["started_at"])) * 1000,
    } for job in terminal]
    elapsed = max(timestamp(row["completed_at"]) for row in records) - min(timestamp(row["created_at"]) for row in records)
    successful = sum(row["status"] == "completed" for row in records)
    if elapsed <= 0 or any(row["queue_ms"] < 0 or row["execution_ms"] < 0 for row in records):
        raise RuntimeError("Invalid database timestamps")
    return {
        "run_id": run_id, "worker_count": len(workers), "worker_ids": sorted(workers),
        "success": successful, "failed": len(ids) - successful, "total_seconds": elapsed,
        "observed_seconds": observed_seconds, "submit_seconds": submit_seconds,
        "throughput_jobs_sec": len(ids) / elapsed,
        "queue": latency([row["queue_ms"] for row in records]),
        "execution": latency([row["execution_ms"] for row in records]),
        "lease_occupancy": sum(row["execution_ms"] for row in records) / (elapsed * 1000 * len(workers)),
        "jobs": records,
    }


def collect_scaling_trials(port, workers, name, pool, report, output):
    config = report["configuration"]
    group = [trial for trial in report["trials"]
             if trial["workload"] == name and trial["worker_count"] == len(workers)]
    comparable_count = sum(comparable(trial) for trial in group)
    if comparable_count == config["trials_per_cell"]:
        return
    warm = measure(port, workers, workload(name, config["warmup_jobs_per_workload"]), pool)
    if warm["failed"]:
        raise RuntimeError("Warm-up failed; wait for a stable environment before measuring")
    while comparable_count < config["trials_per_cell"]:
        if len(group) >= config["trials_per_cell"] + MAX_EXTRA_TRIALS:
            raise RuntimeError(f"DB and monotonic clocks diverged repeatedly for {name}, {len(workers)} workers")
        result = measure(port, workers, workload(name, config["jobs_per_trial"]), pool)
        trial = {"workload": name, "trial": len(group) + 1, **result}
        group.append(trial)
        report["trials"].append(trial)
        store_report(output, report)
        valid = comparable(trial)
        comparable_count += valid
        suffix = "" if valid else " (clock disagreement; timing excluded)"
        print(f"{len(workers):2} workers  {name:7}  trial {trial['trial']}  "
              f"{result['throughput_jobs_sec']:7.2f} jobs/s{suffix}", flush=True)


def environment(compose):
    def sysctl(name):
        try:
            return command("sysctl", "-n", name, quiet=True)
        except (OSError, subprocess.CalledProcessError):
            return None

    cpu = sysctl("machdep.cpu.brand_string")
    if not cpu and platform.system() == "Darwin":
        try:
            hardware = json.loads(command("system_profiler", "-json", "SPHardwareDataType", quiet=True))
            cpu = hardware["SPHardwareDataType"][0].get("chip_type")
        except (OSError, subprocess.CalledProcessError, ValueError, KeyError, IndexError):
            pass

    return {
        "host_os": platform.platform(), "host_arch": platform.machine(),
        "host_cpu": cpu or platform.processor() or platform.machine(),
        "host_logical_cpus": os.cpu_count(),
        "host_ram_bytes": sysctl("hw.memsize") or (
            os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") if hasattr(os, "sysconf") else None),
        "docker_engine": json.loads(command("docker", "info", "--format", "{{json .ServerVersion}}")),
        "docker_vm": json.loads(command("docker", "info", "--format",
                                        '{"cpu_count":{{.NCPU}},"memory_bytes":{{.MemTotal}},"architecture":{{json .Architecture}}}')),
        "compose": command("docker", "compose", "version", "--short"),
        "postgres": compose("exec", "-T", "postgres", "postgres", "--version"),
        "redis": compose("exec", "-T", "redis", "redis-server", "--version"),
        "go": command("docker", "run", "--rm", "golang:1.26-alpine", "go", "version"),
        "python": platform.python_version(),
    }


def store_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


def collect_crash_trials(lab, port, report, output):
    print("Starting isolated Failure Lab", flush=True)
    try:
        lab("up", "--no-build", "-d", "--wait", "worker-1", "worker-2", "worker-3")
        slots = ("worker-1", "worker-2", "worker-3")
        count = report["configuration"]["crash_trials"]
        for index in range(len(report["failure_trials"]), count):
            slot = slots[index % len(slots)]
            result = json.loads(command("python3", "scripts/verify_failure_lab.py",
                                        "--url", f"http://127.0.0.1:{port}", "--slot", slot))
            container = lab("ps", "--all", "--quiet", slot)
            exit_state = command("docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container)
            if exit_state != "exited 70":
                raise RuntimeError(f"Expected an abrupt local worker exit, observed {exit_state}")
            report["failure_trials"].append({"trial": index + 1, "slot": slot, "container_exit_code": 70, **result})
            store_report(output, report)
            print(f"Crash trial {index + 1} ({slot}): {result['elapsed_seconds']}, "
                  f"{result['duplicate_attempts']} extra attempts", flush=True)
            if index + 1 < count:
                lab("start", slot)
    finally:
        lab("down", "--volumes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--api-port", type=int, default=18081)
    parser.add_argument("--jobs", type=int, default=96)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--crash-trials", type=int, default=3)
    args = parser.parse_args()
    if not 16 <= args.jobs <= 100 or args.jobs % 2 or not 2 <= args.trials <= 10 or not 2 <= args.crash_trials <= 10:
        parser.error("use an even job count from 16–100 and 2–10 scaling and crash trials")
    if not 1024 <= args.api_port <= 65535:
        parser.error("use a local API port from 1024–65535")

    run = uuid.uuid4().hex[:10]
    env = {
        **os.environ, "POSTGRES_PASSWORD": secrets.token_hex(20), "API_PORT": str(args.api_port),
        "HEARTBEAT_INTERVAL": "750ms", "WORKER_STALE_AFTER": "2500ms",
        "WORKER_OFFLINE_AFTER": "5s", "JOB_LEASE": "6s",
    }

    def make_compose(kind):
        prefix = ["docker", "compose", "-p", f"queueforge-{kind}-{run}", "-f", "docker-compose.yml"]
        prefix += ["--profile", "benchmark"] if kind == "bench" else ["-f", "docker-compose.lab.yml"]
        return lambda *a: command(*prefix, *a, env=env)

    scaling = make_compose("bench")
    lab = make_compose("drill")
    report = {
        "schema_version": 1, "status": "active", "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "configuration": {
            "workers": WORKERS, "trials_per_cell": args.trials, "jobs_per_trial": args.jobs,
            "warmup_jobs_per_workload": 16, "worker_concurrency": 1, "submission_clients": CLIENTS,
            "terminal_poll_seconds": POLL_SECONDS, "max_attempts": 1,
            "workloads": {"sleep": SLEEP, "compute": COMPUTE, "mixed": [SLEEP, COMPUTE]},
            "crash_trials": args.crash_trials, "crash_jobs_per_trial": 9, "crash_workers": 3,
            "crash_sleep_ms": 10000, "heartbeat_ms": 750, "lease_ms": 6000,
            "stale_ms": 2500, "offline_ms": 5000,
        },
        "trials": [], "failure_trials": [],
    }
    store_report(args.output, report)
    try:
        try:
            print("Starting isolated scaling cluster", flush=True)
            scaling("up", "--build", "-d", "--wait", "api")
            report["environment"] = environment(scaling)
            with concurrent.futures.ThreadPoolExecutor(CLIENTS) as pool:
                for count in WORKERS:
                    scaling("up", "--no-build", "-d", "--wait", "--scale", f"benchmark-worker={count}", "benchmark-worker")
                    workers = wait_for(lambda: idle(args.api_port, count), 35)
                    for name in ("sleep", "compute", "mixed"):
                        collect_scaling_trials(args.api_port, workers, name, pool, report, args.output)
        finally:
            scaling("down", "--volumes")

        collect_crash_trials(lab, args.api_port, report, args.output)
    except BaseException:
        report["status"] = "interrupted"
        store_report(args.output, report)
        raise
    report["status"] = "completed"
    report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    store_report(args.output, report)


if __name__ == "__main__":
    main()
