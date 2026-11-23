import { useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import type { HistoryEvent, Job, JobEvent, JobType, Metrics, Page, Worker } from './api';
import { duration, formatTime } from './api';
import { Timeline } from './lab';

function Badge({ value }: { value: string }) {
  return <span className={`badge ${value}`}>{value === 'failed' ? 'Dead-lettered' : value}</span>;
}

const stats = [
  ['total', 'Total jobs'], ['queued', 'Queued'], ['running', 'Running'],
  ['completed', 'Completed'], ['failed', 'Dead-lettered'], ['active_workers', 'Active workers'],
] as const;

export function MetricCards({ metrics }: { metrics?: Metrics }) {
  return <dl className="stats">{stats.map(([key, label]) =>
    <div className={`stat ${key}`} key={key}><dt>{label}</dt><dd>{metrics?.[key].toLocaleString() ?? '—'}</dd></div>
  )}</dl>;
}

interface TableProps {
  page?: Page;
  offset: number;
  pageSize: number;
  movePage: (offset: number) => void;
  inspect: (id: string) => void;
}

export function JobTable({ page, offset, pageSize, movePage, inspect }: TableProps) {
  return <section id="jobs" className="panel">
    <div className="panel-header"><h2>Jobs</h2><span className="muted">Newest first</span></div>
    <div className="table-wrap" tabIndex={0} role="region" aria-label="Jobs table">
      <table>
        <thead><tr>{['Job ID', 'Type', 'Status', 'Worker', 'Created', 'Duration', 'Attempt'].map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
        <tbody>
          {page?.jobs.map(job => <tr key={job.id}>
            <td><button className="link mono" title={job.id} aria-label={`Inspect job ${job.id}`} onClick={() => inspect(job.id)}>{job.id.slice(0, 8)}</button></td>
            <td>{job.type}</td><td><Badge value={job.status} /></td>
            <td className="mono" title={job.worker_id ?? undefined}>{job.worker_id ?? '—'}</td>
            <td className="numeric" title={formatTime(job.created_at)}>{new Date(job.created_at).toLocaleTimeString()}</td>
            <td className="numeric">{job.started_at ? duration(Math.max(0, (job.completed_at ? Date.parse(job.completed_at) : Date.now()) - Date.parse(job.started_at))) : '—'}</td>
            <td className="numeric">{job.attempts_used} / {job.max_attempts}</td>
          </tr>)}
          {!page?.jobs.length && <tr><td colSpan={7} className="empty">{page ? (offset === 0 ? 'No jobs yet. Submit a demo workload or a single job to get started.' : 'No jobs on this page.') : 'Loading jobs…'}</td></tr>}
        </tbody>
      </table>
    </div>
    <div className="panel-footer">
      <span>{page?.jobs.length ? `Jobs ${offset + 1}–${offset + page.jobs.length}` : ' '}</span>
      <div className="pager">
        <button className="secondary" disabled={offset === 0} onClick={() => movePage(Math.max(0, offset - pageSize))}>Previous</button>
        <button className="secondary" disabled={!page?.has_more} onClick={() => movePage(offset + pageSize)}>Next</button>
      </div>
    </div>
  </section>;
}

const fields = {
  sleep: { label: 'Duration (ms)', hint: '1–10,000 milliseconds', min: 1, max: 10000, key: 'duration_ms' },
  compute: { label: 'Prime count limit', hint: 'Count primes up to this number', min: 2, max: 2000000, key: 'limit' },
  transform: { label: 'Text', hint: 'Trim whitespace and convert to uppercase', key: 'text' },
};

export function SubmitJob({ busy, submit }: { busy: boolean; submit: (type: JobType, payload: unknown) => Promise<void> }) {
  const [type, setType] = useState<JobType>('sleep');
  const [values, setValues] = useState({ sleep: '1500', compute: '1200000', transform: 'QueueForge demo' });
  const field = fields[type];
  function send(event: FormEvent) {
    event.preventDefault();
    void submit(type, { [field.key]: type === 'transform' ? values[type] : Number(values[type]) });
  }
  return <section className="panel submit">
    <div className="panel-header"><h2>Submit job</h2></div>
    <form onSubmit={send}>
      <label htmlFor="type">Job type</label>
      <select id="type" value={type} onChange={event => setType(event.target.value as JobType)}>
        <option value="sleep">Sleep</option><option value="compute">Compute</option><option value="transform">Transform</option>
      </select>
      <label htmlFor="payload">{field.label}</label>
      <input id="payload" aria-describedby="payload-hint" type={type === 'transform' ? 'text' : 'number'}
        min={'min' in field ? field.min : undefined} max={'max' in field ? field.max : undefined}
        maxLength={type === 'transform' ? 4096 : undefined} required={type !== 'transform'} value={values[type]}
        onChange={event => setValues({ ...values, [type]: event.target.value })} />
      <p id="payload-hint" className="muted hint">{field.hint}</p>
      <button className="primary" disabled={busy} type="submit">Submit job</button>
    </form>
  </section>;
}

export function WorkerActivity({ workers, events, inspect }: { workers?: Worker[]; events?: JobEvent[]; inspect: (id: string) => void }) {
  return <div className="layout lower">
    <section className="panel" id="workers">
      <div className="panel-header"><h2>Workers</h2><span className="muted">Reported state</span></div>
      <ul className="feed">
        {workers?.map(worker => <li key={worker.id}>
          <div><div className="mono break">{worker.id}</div><time className="muted" dateTime={worker.reported_at}>{formatTime(worker.reported_at)}</time>
            {worker.current_job_id && <button className="link mono" onClick={() => inspect(worker.current_job_id!)}>Job {worker.current_job_id.slice(0, 8)}</button>}
          </div>
          <div className="worker-badges"><Badge value={worker.health} /><Badge value={worker.state} /></div>
        </li>)}
        {!workers?.length && <li className="muted">{workers ? 'No workers registered.' : 'Loading workers…'}</li>}
      </ul>
    </section>
    <section className="panel" id="activity">
      <div className="panel-header"><h2>Recent activity</h2></div>
      <ul className="feed events">
        {events?.map(event => <li key={event.job_id + event.status}>
          <div><button className="link mono" onClick={() => inspect(event.job_id)} aria-label={`Inspect job ${event.job_id}`}>{event.job_id.slice(0, 8)}</button>
            <time className="muted" dateTime={event.at}>{formatTime(event.at)}</time></div>
          <Badge value={event.status} />
        </li>)}
        {!events?.length && <li className="muted">{events ? 'No job events yet.' : 'Loading activity…'}</li>}
      </ul>
    </section>
  </div>;
}

export function JobDrawer({ id, job, error, history, close, retry }: {
  id: string; job?: Job; error: string; history: HistoryEvent[]; close: () => void; retry: () => Promise<void>;
}) {
  const [retrying, setRetrying] = useState(false);
  const [retryNotice, setRetryNotice] = useState('');
  const [retryError, setRetryError] = useState('');
  async function retryJob() {
    setRetrying(true);
    setRetryNotice('');
    setRetryError('');
    try {
      await retry();
      setRetryNotice('Job queued for another retry cycle.');
    } catch (err) {
      setRetryError((err as Error).message);
    } finally {
      setRetrying(false);
    }
  }
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const element = dialog.current;
    element?.showModal();
    return () => element?.close();
  }, []);
  return <dialog className="drawer" ref={dialog} aria-labelledby="drawer-title" onCancel={event => { event.preventDefault(); close(); }}>
    <div className="panel-header"><h2 id="drawer-title">Job details</h2><button className="secondary" onClick={close}>Close</button></div>
    <div className="drawer-body">
      <p className="muted mono break">{id}</p>
      {error && <p role="alert" className="banner error">{error}</p>}
      {!job && !error && <p className="loading" role="status">Loading job details…</p>}
      {job && <>
        <dl className="details">
          <div><dt>Status</dt><dd><Badge value={job.status} /></dd></div>
          <div><dt>Type</dt><dd>{job.type}</dd></div>
          <div><dt>Worker</dt><dd className="mono break">{job.worker_id ?? '—'}</dd></div>
          <div><dt>Created</dt><dd>{formatTime(job.created_at)}</dd></div>
          <div><dt>Started</dt><dd>{formatTime(job.started_at)}</dd></div>
          <div><dt>Finished</dt><dd>{formatTime(job.completed_at)}</dd></div>
        </dl>
        <dl className="details">
          <div><dt>Attempts</dt><dd>{job.attempt_count} total · {job.attempts_used} / {job.max_attempts} in current cycle</dd></div>
          {job.lease_expires_at && <div><dt>Active lease</dt><dd>{formatTime(job.lease_expires_at)} · token {job.fencing_token}</dd></div>}
          {job.status === 'queued' && <div><dt>Eligible at</dt><dd>{formatTime(job.eligible_at)}</dd></div>}
        </dl>
        {job.status === 'failed' && <button className="secondary retry-button" disabled={retrying} onClick={() => void retryJob()}>Retry dead-lettered job</button>}
        {retryError && <p className="banner error" role="alert">{retryError}</p>}
        {retryNotice && <p className="banner" role="status">{retryNotice}</p>}
        <h3>Payload</h3><pre>{JSON.stringify(job.payload, null, 2)}</pre>
        {job.result !== null && <><h3>Result</h3><pre>{JSON.stringify(job.result, null, 2)}</pre></>}
        {job.last_error && <><h3>Last error</h3><pre>{job.last_error}</pre></>}
        <h3>Execution history</h3><Timeline events={history} />
      </>}
    </div>
  </dialog>;
}
