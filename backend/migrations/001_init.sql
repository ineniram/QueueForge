CREATE TABLE IF NOT EXISTS workers (
    id text PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('idle', 'busy', 'stopped')),
    started_at timestamptz NOT NULL DEFAULT now(),
    reported_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    type text NOT NULL CHECK (type IN ('sleep', 'compute', 'transform')),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    worker_id text REFERENCES workers(id),
    error text,
    result jsonb,
    CHECK (
        (status = 'queued' AND started_at IS NULL AND completed_at IS NULL AND worker_id IS NULL AND error IS NULL AND result IS NULL)
        OR (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL AND worker_id IS NOT NULL AND error IS NULL AND result IS NULL)
        OR (status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND worker_id IS NOT NULL AND error IS NULL AND result IS NOT NULL)
        OR (status = 'failed' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND worker_id IS NOT NULL AND error IS NOT NULL AND result IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS jobs_created ON jobs (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs (created_at, id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_started ON jobs (started_at DESC, id DESC) WHERE started_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_finished ON jobs (completed_at DESC, id DESC) WHERE completed_at IS NOT NULL;
