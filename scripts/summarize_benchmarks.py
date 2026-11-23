"""Validate a completed benchmark matrix and produce its small dashboard dataset."""

import argparse
import collections
import json
import math
from pathlib import Path
import statistics

from benchmark import comparable, latency, timestamp


def distribution(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def summarize(report):
    config = report["configuration"]
    groups = collections.defaultdict(list)
    seen = set()
    for trial in report["trials"]:
        records = trial["jobs"]
        ids = {job["id"] for job in records}
        if len(ids) != config["jobs_per_trial"] or len(ids) != len(records) or seen & ids:
            raise ValueError("Trials must contain distinct job IDs and the configured number of jobs")
        seen.update(ids)
        workers = set(trial["worker_ids"])
        if len(workers) != trial["worker_count"] or any(
            job["worker_id"] not in workers or job["attempt_count"] != 1 for job in records
        ):
            raise ValueError("Job attribution or attempt count differs from the trial configuration")
        if not all(job["status"] in ("completed", "failed") for job in records):
            raise ValueError("Trial contains jobs that did not reach a terminal state")
        outcomes = collections.Counter(job["status"] for job in records)
        if outcomes != collections.Counter(completed=trial["success"], failed=trial["failed"]):
            raise ValueError("Terminal outcome counts disagree with raw job records")
        if not math.isclose(trial["throughput_jobs_sec"], len(ids) / trial["total_seconds"]):
            raise ValueError("Throughput does not match the number of terminal jobs")
        for job in records:
            created, started, completed = map(timestamp, (job["created_at"], job["started_at"], job["completed_at"]))
            if not math.isclose(job["queue_ms"], (started - created) * 1000) or not math.isclose(
                job["execution_ms"], (completed - started) * 1000
            ):
                raise ValueError("Job latency disagrees with raw timestamps")
        for metric in ("queue", "execution"):
            computed = latency([job[f"{metric}_ms"] for job in records])
            if any(not math.isclose(value, trial[metric][key]) for key, value in computed.items()):
                raise ValueError("Aggregated latency disagrees with raw job records")
        duration = max(timestamp(job["completed_at"]) for job in records) - min(timestamp(job["created_at"]) for job in records)
        if not math.isclose(trial["total_seconds"], duration):
            raise ValueError("Completion time disagrees with raw job records")
        expected = {"sleep": len(ids)} if trial["workload"] == "sleep" else (
            {"compute": len(ids)} if trial["workload"] == "compute" else
            {"sleep": len(ids) // 2, "compute": len(ids) // 2})
        if collections.Counter(job["type"] for job in records) != expected:
            raise ValueError("Workload composition differs from the expected job mix")
        groups[trial["workload"], trial["worker_count"]].append(trial)

    expected_cells = {(name, count) for name in config["workloads"] for count in config["workers"]}
    if set(groups) != expected_cells or any(
        {trial["trial"] for trial in group} != set(range(1, len(group) + 1))
        or sum(comparable(trial) for trial in group) != config["trials_per_cell"] for group in groups.values()
    ):
        raise ValueError("Benchmark matrix is incomplete or contains duplicate trial numbers")

    cells = []
    for (name, workers), all_trials in sorted(groups.items()):
        group = [trial for trial in all_trials if comparable(trial)]
        baseline_trials = [trial for trial in groups[name, 1] if comparable(trial)]
        throughput = distribution([trial["throughput_jobs_sec"] for trial in group])
        baseline = statistics.median([trial["throughput_jobs_sec"] for trial in baseline_trials])
        speedup = throughput["median"] / baseline
        cells.append({
            "workload": name, "workers": workers, "throughput": throughput,
            "speedup": speedup, "efficiency": speedup / workers,
            "total_seconds": distribution([trial["total_seconds"] for trial in group]),
            "queue_mean_ms": distribution([trial["queue"]["mean_ms"] for trial in group]),
            "queue_p95_ms": distribution([trial["queue"]["p95_ms"] for trial in group]),
            "execution_mean_ms": distribution([trial["execution"]["mean_ms"] for trial in group]),
            "execution_p95_ms": distribution([trial["execution"]["p95_ms"] for trial in group]),
            "occupancy": distribution([trial["lease_occupancy"] for trial in group]),
            "success": sum(trial["success"] for trial in all_trials),
            "failed": sum(trial["failed"] for trial in all_trials),
            "excluded_trials": len(all_trials) - len(group),
        })
    drills = report["failure_trials"]
    if len(drills) != config["crash_trials"]:
        raise ValueError("Failure drill matrix is incomplete")
    if len({trial["crash"]["job_id"] for trial in drills}) != len(drills):
        raise ValueError("Failure drills must target distinct jobs")
    lease_deadlines = []
    for trial in drills:
        claims = [event for event in trial["events"] if event["kind"] == "claimed"]
        if len(claims) < 2 or claims[0]["worker_id"] == claims[1]["worker_id"] or sum(trial["outcomes"].values()) != config["crash_jobs_per_trial"]:
            raise ValueError("Failure drill did not verify the expected reassignment and completion")
        requested = next(event for event in trial["events"] if event["kind"] == "crash_requested")
        lease_deadlines.append(timestamp(trial["lease_deadline"]) - timestamp(requested["at"]))
    timings = {}
    for key in ("stale", "offline", "lease_expired", "recovered", "reassigned"):
        observed = [trial["elapsed_seconds"][key] for trial in drills if key in trial["elapsed_seconds"]]
        if observed:
            timings[key] = distribution(observed) | {"observations": len(observed)}
    timings["lease_deadline"] = distribution(lease_deadlines) | {"observations": len(lease_deadlines)}
    return {
        key: report[key] for key in ("schema_version", "status", "started_at", "completed_at", "configuration", "environment")
    } | {
        "scaling": cells,
        "timing_exclusions": [
            {"workload": trial["workload"], "workers": trial["worker_count"], "trial": trial["trial"],
             "database_seconds": trial["total_seconds"], "client_seconds": trial["observed_seconds"]}
            for trial in report["trials"] if not comparable(trial)
        ],
        "failure": {
            "timings": timings,
            "completed": sum(trial["outcomes"].get("completed", 0) for trial in drills),
            "failed": sum(trial["outcomes"].get("failed", 0) for trial in drills),
            "duplicates": sum(trial["duplicate_attempts"] for trial in drills),
            "trials": len(drills),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = json.loads(args.raw.read_text())
    if report["status"] != "completed" or report["schema_version"] != 1:
        parser.error("expected a completed version 1 report")
    summary = summarize(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
