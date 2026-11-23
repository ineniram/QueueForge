# Benchmark methodology

Run from the QueueForge directory with Docker Compose and Python 3.11 or later.

```sh
python3 scripts/benchmark.py --output .benchmark-local/results.json
python3 scripts/summarize_benchmarks.py .benchmark-local/results.json .benchmark-local/snapshot.json
```

The runner starts separate, randomly named scaling and Failure Lab Compose projects and cleans up their disposable volumes. It binds an isolated API to loopback port 18081 by default. Use `--api-port` if that port is busy. For comparable results, leave the Docker VM resource allocation fixed and avoid other heavy workloads. The commands above leave the checked-in measurements intact. To display a new run, copy its snapshot to `frontend/public/benchmarks.json` and rebuild the frontend. Development mode reads the file directly.

## Workloads and isolation

- Worker counts are 1, 3, 5, and 10. Each worker is a separate container with one active job at a time. The API, Redis, and PostgreSQL configurations remain fixed.
- Sleep jobs wait 120 ms. Compute jobs count all primes up to 2,000,000 using a bounded sieve. Mixed workloads submit equal numbers of each, alternating in the input sequence. Eight concurrent HTTP clients can change arrival order.
- Each cell collects three comparable timing trials of 96 jobs after a 16-job workload-specific warm-up. If PostgreSQL and client monotonic durations disagree by more than one second, the runner retains that trial and takes another sample. The runner stops after four extra trials per cell. The worker count and workload order are fixed, so thermal and order effects are possible. Scaling trials have a single-attempt policy to expose failures.
- Each trial has different idempotency keys and tracks only the job IDs returned for that trial. It reads their durable terminal states and checks worker IDs, attempts, and workload composition. An accepted submission alone contributes no throughput.
- A separate three-worker lab runs three crash trials, rotating the allowlisted worker slot. Each trial starts with an idle queue, then runs nine 10-second sleep jobs, requests the crash after observing all three workers busy, and checks that the targeted container exited with code 70. It records the target's durable events, any extra retries in the rest of the batch, and final job outcomes.

## Metric definitions

| Metric | Definition |
| --- | --- |
| Completion time | Latest terminal timestamp minus earliest creation timestamp for the trial, both on PostgreSQL's clock. |
| Throughput | Number of terminal jobs divided by completion time. Completed and failed counts are also reported. |
| Queue wait | First claim timestamp minus creation timestamp. Scaling runs use one attempt per job. |
| Execution time | Terminal timestamp minus claim timestamp. Measures time in running state, including handler and persistence overhead. Lease-expiry failures can include recovery delay. |
| Mean and p95 | Arithmetic mean and nearest-rank 95th percentile within a trial. Summary values are medians across trials with min–max ranges. |
| Lease occupancy | Sum of execution durations divided by completion time × configured worker count. It is a time-in-flight proxy for utilization and includes sleeping time. |
| Speedup and efficiency | Median throughput divided by the one-worker median for the same workload. Efficiency is speedup divided by worker count. |
| Client timing | Monotonic time from the first submission to the last HTTP acceptance, or to observed terminal completion, as labeled in the raw report. The terminal poll waits 25 ms between reads. |
| Recovery timing | Monotonic client time from issuing the crash request to first observation of stale, offline, expiry, recovery, or reassignment. Polling waits 100 ms between reads. The lease deadline is also computed separately from database timestamps. |
| Duplicate attempts | Sum of lifetime attempt counts minus the number of jobs in the crash trial. A retried job can produce a single committed result. |

The [raw JSON](../benchmarks/results.json) retains individual job measurements, terminal counts, trial metadata, and the full crash timelines. The [dashboard snapshot](../frontend/public/benchmarks.json) is derived from that file. A run file records an active or interrupted status until all trials finish. The dashboard displays completed snapshots, and the CLI prints active trial progress.

## Limits

This is a single-host, short-burst experiment. It does not measure sustained throughput, saturation with production traffic, or external side effects. The compute handler, API, Redis, and PostgreSQL share VM resources. The measurements can show that contention grows but cannot attribute it to a specific service or hardware bottleneck without profiling.

## Measured results

Apple M4 MacBook Air, 10 cores and 16 GiB host RAM. Docker allocated 10 CPUs and 7.75 GiB RAM. The benchmark used Docker Engine 29.7.2, Compose 5.4.0, PostgreSQL 17.12, Redis 7.4.12, Go 1.26.11, and Python 3.14.0.

Each timing cell contains three comparable trials. Completion and failure counts include timing exclusions. Ranges are the minimum and maximum of trial measurements.

| Workload | Workers | Jobs/s median (range) | Speedup | Efficiency | Time (s) | Queue mean / p95 (ms) | Execution mean / p95 (ms) | Lease occupancy | Completed / failed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sleep | 1 | 8.00 (7.99–8.00) | 1.00× | 100.0% | 12.007 | 5907.63 / 11332.93 | 122.89 / 125.72 | 98.3% | 479 / 1 |
| Sleep | 3 | 24.02 (24.02–24.14) | 3.00× | 100.1% | 3.997 | 1903.92 / 3697.06 | 122.72 / 125.03 | 98.3% | 288 / 0 |
| Sleep | 5 | 38.50 (38.39–38.54) | 4.82× | 96.3% | 2.493 | 1111.26 / 2207.69 | 122.48 / 124.17 | 94.3% | 288 / 0 |
| Sleep | 10 | 76.80 (76.52–77.35) | 9.61× | 96.1% | 1.250 | 514.54 / 1079.51 | 122.42 / 126.20 | 94.0% | 288 / 0 |
| Compute | 1 | 194.97 (186.29–197.75) | 1.00× | 100.0% | 0.492 | 230.92 / 430.65 | 4.75 / 5.99 | 92.1% | 288 / 0 |
| Compute | 3 | 492.25 (463.38–517.62) | 2.52× | 84.2% | 0.195 | 76.25 / 135.80 | 5.41 / 7.62 | 88.8% | 288 / 0 |
| Compute | 5 | 636.87 (465.11–651.14) | 3.27× | 65.3% | 0.151 | 51.29 / 85.56 | 6.54 / 9.99 | 83.3% | 288 / 0 |
| Compute | 10 | 717.26 (621.84–729.81) | 3.68× | 36.8% | 0.134 | 18.93 / 29.87 | 10.42 / 17.24 | 74.7% | 288 / 0 |
| Mixed | 1 | 14.94 (14.92–14.98) | 1.00× | 100.0% | 6.425 | 3176.07 / 6111.39 | 65.72 / 124.80 | 98.2% | 288 / 0 |
| Mixed | 3 | 46.38 (45.84–46.43) | 3.10× | 103.5% | 2.070 | 976.40 / 1894.36 | 63.85 / 122.58 | 98.6% | 288 / 0 |
| Mixed | 5 | 74.73 (74.16–74.90) | 5.00× | 100.0% | 1.285 | 554.23 / 1113.56 | 63.70 / 122.05 | 95.1% | 288 / 0 |
| Mixed | 10 | 147.65 (146.47–147.76) | 9.88× | 98.8% | 0.650 | 237.25 / 483.81 | 63.43 / 122.05 | 93.6% | 288 / 0 |

### Scaling interpretation

- **Sleep.** Throughput rose from 8.00 to 76.80 jobs/s, with 96.1% scaling efficiency at ten workers. Queue p95 fell from 11332.93 to 1079.51 ms. Execution p95 changed from 125.72 to 126.20 ms.
- **Compute.** Throughput rose from 194.97 to 717.26 jobs/s, with 36.8% scaling efficiency at ten workers. Queue p95 fell from 430.65 to 29.87 ms. Execution p95 changed from 5.99 to 17.24 ms.
- **Mixed.** Throughput rose from 14.94 to 147.65 jobs/s, with 98.8% scaling efficiency at ten workers. Queue p95 fell from 6111.39 to 483.81 ms. Execution p95 changed from 124.80 to 122.05 ms.

The sleep-heavy workloads gained throughput with relatively stable execution p95. The compute workload showed rising execution p95 and falling lease occupancy as workers increased. Median client submission time rose from 44.6 ms at one worker to 103.6 ms at ten workers, against a 133.8 ms median burst at ten. These measurements are consistent with shared-resource contention and submission contributing materially to the short burst. The benchmark did not profile CPU scheduling, API saturation, Redis, or PostgreSQL separately.

### Timing exclusions

The summary excludes trials where the PostgreSQL and client monotonic intervals differ by more than one second. Every sample remains in the raw file. Additional trials used the same configuration and a fresh cluster to restore three comparable samples per affected cell.

- Sleep, 1 worker, trial 1: 59.63 s on PostgreSQL and 13.07 s on the client.
- Sleep, 1 worker, trial 3: 13.23 s on PostgreSQL and 12.01 s on the client.

### Failure recovery

| Event | Median (s) | Trial range (s) |
| --- | ---: | ---: |
| Worker stale | 3.16 | 2.95–3.22 (3 observed) |
| Worker offline | 5.69 | 5.50–5.78 (3 observed) |
| Lease deadline | 5.89 | 5.67–5.91 (3 observed) |
| Lease expiry observed | 6.61 | 6.58–6.68 (3 observed) |
| Recovery observed | 6.61 | 6.58–6.68 (3 observed) |
| Reassignment observed | 9.80 | 9.28–9.81 (3 observed) |

Across 3 drills, 27 jobs completed and 0 failed. The batches recorded 3 additional attempts.

Two preliminary drill attempts reached terminal job states but were interrupted by verifier assertions about fixed attempt counts before their histories were saved. The final verifier records variable attempt counts and extra retry histories. Those preliminary attempts are excluded from the reported drill statistics.
