import { useEffect, useState } from 'react';

type Workload = 'sleep' | 'compute' | 'mixed';
interface Spread { median: number; min: number; max: number }
interface Scaling {
  workload: Workload;
  workers: number;
  throughput: Spread;
  speedup: number;
  efficiency: number;
  total_seconds: Spread;
  queue_mean_ms: Spread;
  queue_p95_ms: Spread;
  execution_mean_ms: Spread;
  execution_p95_ms: Spread;
  occupancy: Spread;
  success: number;
  failed: number;
  excluded_trials: number;
}
interface Snapshot {
  schema_version: 1;
  status: 'completed';
  completed_at: string;
  configuration: {
    jobs_per_trial: number;
    trials_per_cell: number;
    worker_concurrency: number;
    submission_clients: number;
    warmup_jobs_per_workload: number;
    crash_jobs_per_trial: number;
    crash_workers: number;
    lease_ms: number;
    heartbeat_ms: number;
    workloads: {
      sleep: { payload: { duration_ms: number } };
      compute: { payload: { limit: number } };
    };
  };
  environment: { host_cpu: string; docker_vm: { cpu_count: number; memory_bytes: number } };
  scaling: Scaling[];
  timing_exclusions: { workload: Workload; workers: number; trial: number }[];
  failure: { trials: number; completed: number; failed: number; duplicates: number; timings: Partial<Record<string, Spread & { observations: number }>> };
}

const recovery = [
  ['stale', 'Worker stale'], ['offline', 'Worker offline'], ['lease_deadline', 'Lease deadline'], ['lease_expired', 'Lease expiry recorded'],
  ['recovered', 'Job recovered'], ['reassigned', 'New worker claimed'],
];

const number = (value: number, digits = 1) => value.toLocaleString(undefined, { maximumFractionDigits: digits });
const range = (spread: Spread, digits = 1) => `${number(spread.min, digits)}–${number(spread.max, digits)}`;

function Chart({ title, unit, tone, rows, median, mean }: {
  title: string; unit: string; tone: 'orange' | 'teal' | 'blue'; rows: Scaling[]; median: (row: Scaling) => Spread; mean?: (row: Scaling) => Spread;
}) {
  const max = Math.ceil(Math.max(...rows.map(row => median(row).median), 1) / 4) * 4;
  return <section className={`benchmark-chart chart-${tone}`}>
    <h3>{title}</h3>
    <ul>
      {rows.map(row => {
        const value = median(row);
        return <li key={row.workers}>
          <div className="chart-label"><span>{row.workers} {row.workers === 1 ? 'worker' : 'workers'}</span><strong>{number(value.median)} {unit}</strong></div>
          <div className="chart-track" title={`${number(value.median)} ${unit} median · ${range(value)} ${unit} trial range`} aria-hidden="true">
            <div style={{ width: `${value.median / max * 100}%` }} />
          </div>
          <small>{mean && `Mean ${number(mean(row).median)} ${unit} · `}Trial range {range(value)} {unit}</small>
        </li>;
      })}
    </ul>
    <div className="chart-axis" aria-hidden="true">
      {[0, .25, .5, .75, 1].map(fraction => <span key={fraction}>{number(max * fraction)}</span>)}
    </div>
  </section>;
}

export function Performance() {
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [workload, setWorkload] = useState<Workload>('sleep');

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      try {
        const response = await fetch('/benchmarks.json', {
          signal: AbortSignal.any([controller.signal, AbortSignal.timeout(6000)]),
        });
        if (!response.ok) throw new Error(`Benchmark data unavailable (${response.status}).`);
        if (!response.headers.get('content-type')?.includes('json')) {
          throw new Error('No completed benchmark snapshot was found.');
        }
        const result: Snapshot = await response.json();
        if (result.schema_version !== 1 || result.status !== 'completed' || result.scaling.length === 0) {
          throw new Error('No completed benchmark snapshot was found.');
        }
        if (!controller.signal.aborted) setSnapshot(result);
      } catch (err) {
        if (!controller.signal.aborted) setError((err as Error).message);
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }
    void load();
    return () => controller.abort();
  }, []);

  const rows = snapshot?.scaling.filter(row => row.workload === workload).sort((a, b) => a.workers - b.workers) ?? [];
  const config = snapshot?.configuration;
  return <section className="panel performance" id="performance">
    <div className="panel-header"><h2>Performance</h2>{snapshot && <span className="badge completed">Completed benchmark</span>}</div>
    <div className="performance-body">
      {loading && <p className="muted" role="status">Loading benchmark results…</p>}
      {error && <p className="muted" role="status">{error} Run the benchmark tool and generate a snapshot to populate this section.</p>}
      {snapshot && config && <>
        <p className="muted">Saved local results from {new Date(snapshot.completed_at).toLocaleDateString()}. Active benchmark trials run in the terminal and can be viewed here after a snapshot is generated.</p>
        <div className="workload-picker" aria-label="Benchmark workload" role="group">
          {(['sleep', 'compute', 'mixed'] as const).map(name =>
            <button className="secondary" key={name} aria-pressed={workload === name} onClick={() => setWorkload(name)}>
              {{ sleep: 'I/O-like sleep', compute: 'CPU-bound compute', mixed: 'Mixed' }[name]}
            </button>
          )}
        </div>
        <p className="muted benchmark-config">
          {config.jobs_per_trial} jobs per trial · {config.trials_per_cell} trials per worker count · {config.warmup_jobs_per_workload} warm-up jobs
          {' '}· {config.worker_concurrency} job per worker at a time · {config.submission_clients} submission clients
        </p>
        <p className="muted benchmark-config">
          {snapshot.environment.host_cpu} · Docker VM with {snapshot.environment.docker_vm.cpu_count} CPUs and {number(snapshot.environment.docker_vm.memory_bytes / 2 ** 30)} GiB RAM.
          {' '}Sleep {config.workloads.sleep.payload.duration_ms} ms. Compute primes through {number(config.workloads.compute.payload.limit, 0)}. Mixed uses half of each.
        </p>
        <div className="benchmark-charts">
          <Chart rows={rows} title="Throughput" tone="orange" unit="jobs/s" median={row => row.throughput} />
          <Chart rows={rows} title="Queue wait · p95" tone="teal" unit="ms" median={row => row.queue_p95_ms} mean={row => row.queue_mean_ms} />
          <Chart rows={rows} title="Execution time · p95" tone="blue" unit="ms" median={row => row.execution_p95_ms} mean={row => row.execution_mean_ms} />
        </div>
        <p className="muted benchmark-config">Bars show the median across trials. Throughput counts jobs that reached a terminal state. Latencies use PostgreSQL timestamps.</p>
        {snapshot.timing_exclusions.some(trial => trial.workload === workload) && <p className="banner warning">
          Timing charts exclude {snapshot.timing_exclusions.filter(trial => trial.workload === workload).map(trial => `trial ${trial.trial} at ${trial.workers} worker${trial.workers === 1 ? '' : 's'}`).join(', ')} because the database and monotonic clocks diverged by over one second. Outcome counts include every trial.
        </p>}
        <div className="table-wrap benchmark-table"><table>
          <thead><tr>{['Workers', 'Speedup', 'Efficiency', 'Completion time', 'Lease occupancy', 'Completed', 'Failed'].map(label => <th key={label} scope="col">{label}</th>)}</tr></thead>
          <tbody>{rows.map(row => <tr key={row.workers}>
            <th scope="row">{row.workers}</th><td>{number(row.speedup, 2)}×</td><td>{number(row.efficiency * 100)}%</td>
            <td>{number(row.total_seconds.median, 2)} s</td><td>{number(row.occupancy.median * 100)}%</td><td>{row.success}</td><td>{row.failed}</td>
          </tr>)}</tbody>
        </table></div>
        <h3>Failure recovery</h3>
        <p className="muted benchmark-config">
          {snapshot.failure.trials} trials, {config.crash_jobs_per_trial} jobs and {config.crash_workers} workers each. {snapshot.failure.completed} jobs completed, {snapshot.failure.failed} failed, and {snapshot.failure.duplicates} extra attempts were recorded.
          {' '}Lease {config.lease_ms / 1000} s, heartbeat {config.heartbeat_ms} ms. Times are measured from the crash request.
        </p>
        <div className="table-wrap benchmark-table"><table>
          <thead><tr><th scope="col">Event</th><th scope="col">Median</th><th scope="col">Trial range</th></tr></thead>
          <tbody>{recovery.map(([key, label]) => {
            const value = snapshot.failure.timings[key];
            return <tr key={key}><th scope="row">{label}</th><td>{value ? `${number(value.median, 2)} s` : 'Unobserved'}</td>
              <td>{value ? `${range(value, 2)} s (${value.observations}/${snapshot.failure.trials} observed)` : '—'}</td></tr>;
          })}</tbody>
        </table></div>
      </>}
    </div>
  </section>;
}
