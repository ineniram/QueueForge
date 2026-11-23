export type Status = 'queued' | 'running' | 'completed' | 'failed';
export type JobType = 'sleep' | 'compute' | 'transform';

export interface Job {
  attempt_count: number;
  attempts_used: number;
  max_attempts: number;
  backoff_ms: number;
  max_backoff_ms: number;
  eligible_at: string;
  lease_expires_at: string | null;
  fencing_token: string;
  last_error: string | null;
  id: string;
  type: JobType;
  payload: unknown;
  status: Status;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  worker_id: string | null;
  error: string | null;
  result: unknown;
}

export interface Metrics {
  total: number;
  queued: number;
  running: number;
  completed: number;
  failed: number;
  active_workers: number;
  average_run_ms: number;
  average_wait_ms: number;
}

export interface Worker {
  health: 'healthy' | 'stale' | 'offline';
  current_job_id: string | null;
  demo_slot: string | null;
  crash_requested: boolean;
  id: string;
  state: 'idle' | 'busy' | 'stopped';
  started_at: string;
  reported_at: string;
}

export interface JobEvent {
  job_id: string;
  status: Status;
  at: string;
  worker_id: string | null;
}

export interface Page {
  jobs: Job[];
  has_more: boolean;
}

export async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, options);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(body?.error?.message || `Request failed (${response.status})`);
  }
  return response.json();
}

export function post<T>(path: string, data: unknown) {
  return request<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
    signal: AbortSignal.timeout(12_000),
  });
}

export const formatTime = (value: string | null) =>
  value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' }) : '—';

export function duration(ms: number) {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`;
}

export interface HistoryEvent {
  sequence: number;
  job_id: string | null;
  kind: string;
  at: string;
  worker_id: string | null;
  attempt: number | null;
  detail: string | null;
}

export interface LabStatus { enabled: boolean }

export function labPost<T>(path: string, data: unknown) {
  return request<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-QueueForge-Demo': '1' },
    body: JSON.stringify(data),
    signal: AbortSignal.timeout(12_000),
  });
}
