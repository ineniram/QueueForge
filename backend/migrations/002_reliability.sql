ALTER TABLE workers ADD COLUMN IF NOT EXISTS demo_slot text;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS reported_job_id uuid;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS stale_reported boolean NOT NULL DEFAULT false;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS crash_requested boolean NOT NULL DEFAULT false;

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS demo_crash_pending boolean NOT NULL DEFAULT false;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS fence bigint NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS backoff_ms integer NOT NULL DEFAULT 500 CHECK (backoff_ms BETWEEN 100 AND 60000);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_backoff_ms integer NOT NULL DEFAULT 10000 CHECK (max_backoff_ms BETWEEN backoff_ms AND 300000);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS eligible_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_error text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retries_used integer NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS submission_key text UNIQUE;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS submission_hash text;

CREATE TABLE IF NOT EXISTS job_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid REFERENCES jobs(id),
    kind text NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    worker_id text,
    attempt integer,
    detail text
);

CREATE INDEX IF NOT EXISTS job_events_history ON job_events (job_id, sequence);
CREATE INDEX IF NOT EXISTS jobs_worker_active ON jobs (worker_id) WHERE status='running';
CREATE INDEX IF NOT EXISTS jobs_lease_expiry ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_dispatch_eligible ON jobs (eligible_at, created_at, id) WHERE status = 'queued';

-- Preserve the observable lifecycle and give legacy running rows an expired lease.
UPDATE jobs SET lease_expires_at=CASE WHEN status='running' THEN clock_timestamp() END,
    attempt_count=1,retries_used=1,fence=1,last_error=error
WHERE status IN ('running','completed','failed');

INSERT INTO job_events(job_id,kind,at,attempt)
SELECT id,'submitted',created_at,0 FROM jobs;
INSERT INTO job_events(job_id,kind,at,worker_id,attempt)
SELECT id,'claimed',started_at,worker_id,attempt_count FROM jobs WHERE started_at IS NOT NULL;
INSERT INTO job_events(job_id,kind,at,worker_id,attempt,detail)
SELECT id,CASE WHEN status='completed' THEN 'completed' ELSE 'permanently_failed' END,
    completed_at,worker_id,attempt_count,error FROM jobs WHERE completed_at IS NOT NULL;

CREATE OR REPLACE FUNCTION queueforge_job_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO job_events(job_id,kind,at,attempt) VALUES (NEW.id,'submitted',NEW.created_at,0);
    ELSIF NEW.status <> OLD.status THEN
        IF OLD.status = 'running' AND NEW.last_error = 'Worker lease expired'
           AND NEW.result IS NULL AND (NEW.status='queued' OR NEW.error='Worker lease expired') THEN
            INSERT INTO job_events(job_id,kind,worker_id,attempt,detail)
            VALUES (NEW.id,'lease_expired',OLD.worker_id,OLD.attempt_count,NEW.last_error);
            IF NEW.status = 'queued' THEN
                INSERT INTO job_events(job_id,kind,worker_id,attempt)
                VALUES (NEW.id,'recovered',OLD.worker_id,OLD.attempt_count);
            END IF;
        END IF;
        INSERT INTO job_events(job_id,kind,worker_id,attempt,detail) VALUES (
            NEW.id,
            CASE WHEN NEW.status='running' THEN 'claimed'
                 WHEN NEW.status='completed' THEN 'completed'
                 WHEN NEW.status='failed' THEN 'permanently_failed'
                 WHEN OLD.status='failed' THEN 'manual_retry'
                 ELSE 'retry_scheduled' END,
            COALESCE(NEW.worker_id,OLD.worker_id),NEW.attempt_count,
            CASE WHEN NEW.status='queued' THEN NEW.last_error WHEN NEW.status='failed' THEN NEW.error END
        );
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS queueforge_job_history ON jobs;
CREATE TRIGGER queueforge_job_history AFTER INSERT OR UPDATE OF status ON jobs
FOR EACH ROW EXECUTE FUNCTION queueforge_job_history();
