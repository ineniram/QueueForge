import { StrictMode, useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { duration, labPost, post, request } from './api';
import type { HistoryEvent, LabStatus, Job, JobEvent, JobType, Metrics, Page, Worker } from './api';
import { JobDrawer, JobTable, MetricCards, SubmitJob, WorkerActivity } from './panels';
import { FailureLab } from './lab';
import { Performance } from './performance';
import './styles.css';

const pageSize = 12;
const sections = [
  ['overview', 'Overview'], ['jobs', 'Jobs'], ['workers', 'Workers'],
  ['activity', 'Activity'], ['failure-lab', 'Failure Lab'], ['performance', 'Performance'],
] as const;

function activeSection() {
  const hash = window.location.hash.slice(1);
  return sections.some(([id]) => id === hash) ? hash : 'overview';
}

function Dashboard() {
  const [section, setSection] = useState(activeSection);
  const [data, setData] = useState<{ metrics: Metrics; workers: Worker[]; events: JobEvent[]; lab: LabStatus; history: HistoryEvent[] }>();
  const [page, setPage] = useState<Page>();
  const [offset, setOffset] = useState(0);
  const [revision, setRevision] = useState(0);
  const [selected, setSelected] = useState<string>();
  const [job, setJob] = useState<Job>();
  const [history, setHistory] = useState<HistoryEvent[]>([]);
  const [detailsError, setDetailsError] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [actionError, setActionError] = useState('');
  const [busy, setBusy] = useState(false);
  const drawerTrigger = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const syncSection = () => setSection(activeSection());
    window.addEventListener('hashchange', syncSection);
    return () => window.removeEventListener('hashchange', syncSection);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(6000)]);
      try {
        const [metrics, workers, events, jobs, lab, history] = await Promise.all([
          request<Metrics>('/api/metrics', { signal }),
          request<Worker[]>('/api/workers', { signal }),
          request<JobEvent[]>('/api/activity', { signal }),
          request<Page>(`/api/jobs?limit=${pageSize}&offset=${offset}`, { signal }),
          request<LabStatus>('/api/failure-lab', { signal }),
          request<HistoryEvent[]>('/api/history', { signal }),
        ]);
        if (!controller.signal.aborted) {
          setData({ metrics, workers, events, lab, history });
          setPage(jobs);
          setError('');
        }
      } catch (err) {
        if (!controller.signal.aborted) setError((err as Error).message);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, 2000);
      }
    }
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [offset, revision]);

  useEffect(() => {
    if (!selected) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(6000)]);
      try {
        const [detail, events] = await Promise.all([
          request<Job>(`/api/jobs/${selected}`, { signal }),
          request<HistoryEvent[]>(`/api/jobs/${selected}/history`, { signal }),
        ]);
        if (!controller.signal.aborted) {
          setJob(detail);
          setHistory(events);
          setDetailsError('');
        }
      } catch (err) {
        if (!controller.signal.aborted) setDetailsError((err as Error).message);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, 2000);
      }
    }
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [selected]);

  function inspect(id: string) {
    drawerTrigger.current = document.activeElement as HTMLElement | null;
    setSelected(id);
    setJob(undefined);
    setHistory([]);
    setDetailsError('');
  }

  function closeDrawer() {
    setSelected(undefined);
    drawerTrigger.current?.focus();
  }

  async function submit(type?: JobType, payload?: unknown) {
    setBusy(true);
    setNotice('');
    setActionError('');
    try {
      if (type) {
        await post<Job>('/api/jobs', { type, payload });
        setNotice('Job submitted.');
      } else {
        const result = await post<{ count: number }>('/api/demo', { count: 100 });
        setNotice(`${result.count} demo jobs submitted.`);
      }
      setOffset(0);
      setRevision((value) => value + 1);
    } catch (err) {
      setActionError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  function movePage(next: number) {
    setPage(undefined);
    setOffset(next);
  }

  return (
    <>
      <header className="topbar">
        <div className="shell topbar-content">
          <a className="brand" href="#overview"><span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>QueueForge</a>
          <nav aria-label="Dashboard sections">
            {sections.map(([id, label]) => <a key={id} href={`#${id}`} aria-current={section === id ? 'location' : undefined}>{label}</a>)}
          </nav>
          <span className={`connection ${error ? 'offline' : data ? 'online' : ''}`}>{error ? 'Reconnecting' : data ? 'Live updates' : 'Connecting'}</span>
        </div>
      </header>
      <main id="overview" className="shell">
        <section className="heading">
          <div><h1>Queue overview</h1><p className="muted">Submit background jobs and watch them move from queue to completion.</p><p className="muted intro-architecture">The default stack runs three independent workers that claim jobs from a shared queue.</p></div>
          <button className="primary demo-button" disabled={busy} onClick={() => void submit()}>{busy ? "Submitting…" : "Submit 100 demo jobs"}</button>
        </section>
        {error && <p role="alert" className="banner error">Could not refresh dashboard data. {error}</p>}
        {actionError && <p role="alert" className="banner error">{actionError}</p>}
        {notice && <p role="status" className="banner">{notice}</p>}
        <MetricCards metrics={data?.metrics} />
        <div className="layout">
          <JobTable page={page} offset={offset} pageSize={pageSize} movePage={movePage} inspect={inspect} />
          <SubmitJob busy={busy} submit={submit} />
        </div>
        <WorkerActivity workers={data?.workers} events={data?.events} inspect={inspect} />
        <FailureLab enabled={data?.lab.enabled} workers={data?.workers ?? []} events={data?.history ?? []} inspect={inspect}
          launch={() => labPost('/api/demo', { count: 9, profile: 'failure' })}
          crash={(slot) => labPost('/api/failure-lab/crash', { slot })} />
        <Performance />
        <section className="processing panel">
          <h2>Processing metrics</h2>
          <dl>
            <div><dt>Average queue wait</dt><dd>{data && data.metrics.total > data.metrics.queued ? duration(data.metrics.average_wait_ms) : '—'}</dd></div>
            <div><dt>Average run time</dt><dd>{data && data.metrics.completed + data.metrics.failed > 0 ? duration(data.metrics.average_run_ms) : '—'}</dd></div>
          </dl>
        </section>
      </main>
      {selected && <JobDrawer id={selected} job={job} error={detailsError} history={history} close={closeDrawer}
        retry={async () => {
          await post<Job>(`/api/jobs/${selected}/retry`, {});
          setRevision(value => value + 1);
        }} />}
    </>
  );
}

createRoot(document.getElementById('root')!).render(<StrictMode><Dashboard /></StrictMode>);
