import { useEffect, useState } from 'react';
import { formatTime, request } from './api';
import type { HistoryEvent, Worker } from './api';

const eventLabels: Record<string, string> = {
  submitted: 'Submitted', claimed: 'Claimed', completed: 'Completed', crash_requested: 'Crash requested',
  worker_stale: 'Worker became stale', lease_expired: 'Lease expired', recovered: 'Job recovered',
  retry_scheduled: 'Retry scheduled', permanently_failed: 'Dead-lettered', manual_retry: 'Manual retry',
};

export function Timeline({ events, inspect }: { events: HistoryEvent[]; inspect?: (id: string) => void }) {
  if (!events.length) return <p className="muted timeline-empty">No events yet.</p>;
  return <ol className="feed timeline">
    {events.map(event => <li key={event.sequence} className={`event-${event.kind}`}>
      <div>
        <strong>{eventLabels[event.kind] ?? event.kind}</strong>
        {event.attempt ? <span className="muted"> · attempt {event.attempt}</span> : null}
        {event.worker_id && <div className="muted mono break">{event.worker_id}</div>}
        {event.detail && <div className="muted break">{event.detail}</div>}
        <time className="muted" dateTime={event.at}>{formatTime(event.at)}</time>
      </div>
      {event.job_id && inspect && <button className="link mono" onClick={() => inspect(event.job_id!)}>{event.job_id.slice(0, 8)}</button>}
    </li>)}
  </ol>;
}

interface LabProps {
  enabled?: boolean;
  workers: Worker[];
  events: HistoryEvent[];
  inspect: (id: string) => void;
  launch: () => Promise<{ count: number }>;
  crash: (slot: string) => Promise<{ worker_id: string; job_id: string }>;
}

export function FailureLab({ enabled, workers, events, inspect, launch, crash }: LabProps) {
  const [slot, setSlot] = useState('');
  const [target, setTarget] = useState<{ worker_id: string; job_id: string }>();
  const [history, setHistory] = useState<HistoryEvent[]>();
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  const [error, setError] = useState('');
  const [historyError, setHistoryError] = useState('');
  const candidates = workers.filter(worker => worker.demo_slot && worker.health === 'healthy' &&
    worker.state === 'busy' && worker.current_job_id && !worker.crash_requested);
  const selection = candidates.find(worker => worker.demo_slot === slot);
  const jobID = target?.job_id;

  useEffect(() => {
    if (!jobID) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(6000)]);
      try {
        const result = await request<HistoryEvent[]>(`/api/jobs/${jobID}/history`, { signal });
        if (!controller.signal.aborted) {
          setHistory(result);
          setHistoryError('');
        }
      } catch (err) {
        if (!controller.signal.aborted) setHistoryError((err as Error).message);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, 1000);
      }
    }
    void poll();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [jobID]);

  async function act(action: () => Promise<string>) {
    setBusy(true);
    setNotice('');
    setError('');
    try { setNotice(await action()); }
    catch (err) { setError((err as Error).message); }
    finally { setBusy(false); }
  }

  const crashedWorker = workers.find(worker => worker.id === target?.worker_id);
  const lifecycle = events.filter(event => ['lease_expired', 'worker_stale', 'recovered', 'retry_scheduled', 'permanently_failed'].includes(event.kind));

  return <section className="panel lab" id="failure-lab">
    <div className="panel-header"><h2>Failure Lab</h2><span className="muted">Local development only</span></div>
    <div className="lab-body">
      {enabled === false && <p className="muted">Crash simulation is disabled. Start the local stack with the Failure Lab Compose override to enable it.</p>}
      {enabled && <>
        <p className="muted">Start long-running jobs, then stop an allowlisted worker while it holds a lease. Watch its job return to the queue and move to a healthy worker.</p>
        <div className="lab-controls">
          <button className="primary" disabled={busy} onClick={() => void act(async () => `${(await launch()).count} long-running demo jobs submitted.`)}>Submit recovery demo jobs</button>
          <label htmlFor="lab-worker">Busy demo worker</label>
          <select id="lab-worker" value={selection ? slot : ''} onChange={event => setSlot(event.target.value)}>
            <option value="">Select a worker</option>
            {candidates.map(worker => <option key={worker.id} value={worker.demo_slot!}>{worker.demo_slot} · job {worker.current_job_id?.slice(0, 8)}</option>)}
          </select>
          <button className="danger" disabled={busy || !selection} onClick={() => void act(async () => {
            const result = await crash(slot);
            setTarget(result);
            setHistory(undefined);
            setHistoryError('');
            return 'Crash requested. Tracking the affected job below.';
          })}>Simulate Worker Crash</button>
        </div>
      </>}
      {notice && <p className="banner" role="status">{notice}</p>}
      {error && <p className="banner error" role="alert">{error}</p>}
      {historyError && <p className="banner error" role="alert">{historyError}</p>}
      <h3>{target ? 'Affected job timeline' : 'Recent recovery events'}</h3>
      {target && <p className="muted lab-target">
        Worker <span className="mono">{crashedWorker?.demo_slot ?? target.worker_id}</span>{' '}
        <span className={`badge ${crashedWorker?.health ?? ''}`}>{crashedWorker?.health ?? 'unknown'}</span>
        {' '}<button className="link mono" onClick={() => inspect(target.job_id)}>Inspect {target.job_id.slice(0, 8)}</button>
      </p>}
      <Timeline events={target ? history ?? [] : lifecycle} inspect={inspect} />
    </div>
  </section>;
}
