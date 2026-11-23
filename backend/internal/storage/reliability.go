package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"queueforge/internal/jobs"
)

var ErrIdempotencyConflict = errors.New("idempotency key already used for a different submission")

func (s *Store) Submit(ctx context.Context, input jobs.Input, policy jobs.Policy, key *string, hash string) (jobs.Job, bool, error) {
	job, err := scanJob(s.db.QueryRow(ctx, `
 INSERT INTO jobs(id,type,payload,max_attempts,backoff_ms,max_backoff_ms,submission_key,submission_hash)
 VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7)
 ON CONFLICT(submission_key) DO NOTHING RETURNING `+columns,
		input.Type, input.Payload, policy.MaxAttempts, policy.BackoffMS, policy.MaxBackoffMS, key, hash))
	if !IsNotFound(err) || key == nil {
		return job, true, err
	}

	// A fresh statement sees the winning row after ON CONFLICT waited for its transaction.
	job, err = scanJob(s.db.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE submission_key=$1 AND submission_hash=$2", key, hash))
	if IsNotFound(err) {
		return job, false, ErrIdempotencyConflict
	}
	return job, false, err
}

func (s *Store) Claim(ctx context.Context, id, workerID string) (jobs.Job, error) {
	return scanJob(s.db.QueryRow(ctx, `
 UPDATE jobs SET status='running',worker_id=$2,started_at=clock_timestamp(),
 lease_expires_at=clock_timestamp()+($3::bigint * interval '1 millisecond'),
 fence=fence+1,attempt_count=attempt_count+1,retries_used=retries_used+1
 WHERE id=$1 AND status='queued' AND eligible_at<=clock_timestamp() AND retries_used<max_attempts
 RETURNING `+columns, id, workerID, s.Lease.Milliseconds()))
}

// Lock before checking the deadline, including when a contender waited on this row.
const lockJob = `WITH locked AS MATERIALIZED (SELECT id FROM jobs WHERE id=$1 FOR UPDATE) `
const ownsJob = ` WHERE id=(SELECT id FROM locked) AND worker_id=$2 AND fence=$3
 AND status='running' AND NOT demo_crash_pending AND lease_expires_at>clock_timestamp()`

func (s *Store) Renew(ctx context.Context, id, workerID string, fence int64) error {
	tag, err := s.db.Exec(ctx, lockJob+`UPDATE jobs
 SET lease_expires_at=clock_timestamp()+($4::bigint * interval '1 millisecond')`+ownsJob,
		id, workerID, fence, s.Lease.Milliseconds())
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

const canRetry = `retries_used < max_attempts`
const retryTime = `clock_timestamp()+LEAST(max_backoff_ms,backoff_ms*(1 << GREATEST(retries_used-1,0))) * interval '1 millisecond'`

func (s *Store) Finish(ctx context.Context, id, workerID string, fence int64, result json.RawMessage, runErr error) error {
	query := lockJob + `UPDATE jobs SET status='completed',completed_at=clock_timestamp(),result=$4,lease_expires_at=NULL` + ownsJob
	var value any = result
	if runErr != nil {
		query = lockJob + failureUpdate + ownsJob
		value = runErr.Error()
	}
	tag, err := s.db.Exec(ctx, query, id, workerID, fence, value)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

const failureUpdate = `UPDATE jobs SET
 status=CASE WHEN ` + canRetry + ` THEN 'queued' ELSE 'failed' END,
 completed_at=CASE WHEN NOT (` + canRetry + `) THEN clock_timestamp() END,
 started_at=CASE WHEN NOT (` + canRetry + `) THEN started_at END,
 worker_id=CASE WHEN NOT (` + canRetry + `) THEN worker_id END,
 error=CASE WHEN NOT (` + canRetry + `) THEN $4::text END,
 last_error=$4::text,lease_expires_at=NULL,demo_crash_pending=false,
 eligible_at=CASE WHEN ` + canRetry + ` THEN ` + retryTime + ` ELSE eligible_at END`

func (s *Store) Recover(ctx context.Context) error {
	// Every coordinator locks a disjoint batch, then revalidates expiry.
	query := `WITH expired AS MATERIALIZED (
 SELECT id FROM jobs WHERE status='running' AND lease_expires_at<=clock_timestamp()
 ORDER BY lease_expires_at LIMIT 100 FOR UPDATE SKIP LOCKED) ` +
		strings.ReplaceAll(failureUpdate, "$4", "$1") + ` WHERE id IN (SELECT id FROM expired)
 AND status='running' AND lease_expires_at<=clock_timestamp()`
	_, err := s.db.Exec(ctx, query, "Worker lease expired")
	return err
}

func (s *Store) Retry(ctx context.Context, id string) (jobs.Job, error) {
	return scanJob(s.db.QueryRow(ctx, `UPDATE jobs SET status='queued',retries_used=0,eligible_at=clock_timestamp(),
 started_at=NULL,completed_at=NULL,worker_id=NULL,error=NULL WHERE id=$1 AND status='failed' RETURNING `+columns, id))
}

func (s *Store) ReportWorker(ctx context.Context, id, state string) error {
	_, err := s.Heartbeat(ctx, id, state, nil, nil)
	return err
}

func (s *Store) Heartbeat(ctx context.Context, id, state string, slot, jobID *string) (bool, error) {
	var crash bool
	err := s.db.QueryRow(ctx, `
 INSERT INTO workers(id,state,demo_slot,reported_job_id) VALUES($1,$2,$3,$4)
 ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,reported_at=clock_timestamp(),stale_reported=false,reported_job_id=EXCLUDED.reported_job_id
 RETURNING crash_requested AND state <> 'stopped'`, id, state, slot, jobID).Scan(&crash)
	return crash, err
}

func (s *Store) MarkStale(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `
 WITH stale AS (
 UPDATE workers SET stale_reported=true WHERE state<>'stopped' AND NOT stale_reported
 AND reported_at <= statement_timestamp()-($1::bigint * interval '1 millisecond')
 RETURNING id)
 INSERT INTO job_events(job_id,kind,worker_id,attempt)
 SELECT j.id,'worker_stale',stale.id,j.attempt_count FROM stale
 LEFT JOIN jobs j ON j.worker_id=stale.id AND j.status='running'`, s.StaleAfter.Milliseconds())
	return err
}

func (s *Store) Workers(ctx context.Context) ([]jobs.Worker, error) {
	rows, err := s.db.Query(ctx, `
 SELECT w.id,w.state,w.started_at,w.reported_at,w.demo_slot,w.crash_requested,w.reported_job_id::text,
 CASE WHEN w.state='stopped' OR w.reported_at <= statement_timestamp()-($2::bigint * interval '1 millisecond') THEN 'offline'
 WHEN w.reported_at <= statement_timestamp()-($1::bigint * interval '1 millisecond') THEN 'stale'
 ELSE 'healthy' END,
 (SELECT id::text FROM jobs WHERE worker_id=w.id AND status='running' LIMIT 1)
 FROM workers w ORDER BY w.started_at DESC,w.id`, s.StaleAfter.Milliseconds(), s.OfflineAfter.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Worker, error) {
		var w jobs.Worker
		err := row.Scan(&w.ID, &w.State, &w.StartedAt, &w.ReportedAt, &w.DemoSlot, &w.CrashRequested, &w.ReportedJob, &w.Health, &w.CurrentJob)
		return w, err
	})
}

func (s *Store) RequestCrash(ctx context.Context, slot string) (string, string, error) {
	var workerID, jobID string
	err := s.db.QueryRow(ctx, `
 WITH target AS MATERIALIZED (
 SELECT w.id AS worker_id,j.id AS job_id,j.attempt_count
 FROM workers w JOIN jobs j ON j.worker_id=w.id AND j.status='running'
 WHERE w.demo_slot=$1 AND w.state='busy' AND NOT w.crash_requested
 AND w.reported_at > statement_timestamp()-($2::bigint * interval '1 millisecond')
 AND j.lease_expires_at>clock_timestamp()
 ORDER BY w.started_at DESC LIMIT 1 FOR UPDATE OF j SKIP LOCKED
 ), requested AS (
 UPDATE workers w SET crash_requested=true FROM target
 WHERE w.id=target.worker_id AND w.state='busy' AND NOT w.crash_requested
 RETURNING w.id,target.job_id,target.attempt_count
 ), frozen AS (
 UPDATE jobs j SET demo_crash_pending=true FROM requested
 WHERE j.id=requested.job_id AND j.status='running' RETURNING j.id
 )
 INSERT INTO job_events(job_id,kind,worker_id,attempt)
 SELECT job_id,'crash_requested',requested.id,attempt_count FROM requested JOIN frozen ON frozen.id=requested.job_id
 RETURNING worker_id,job_id::text`, slot, s.StaleAfter.Milliseconds()).Scan(&workerID, &jobID)
	return workerID, jobID, err
}

func (s *Store) History(ctx context.Context, id *string) ([]jobs.HistoryEvent, error) {
	query := `SELECT sequence,job_id::text,kind,at,worker_id,attempt,detail FROM job_events`
	var args []any
	if id != nil {
		query += ` WHERE job_id=$1 ORDER BY sequence`
		args = append(args, *id)
	} else {
		query += ` ORDER BY sequence DESC LIMIT 50`
	}
	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.HistoryEvent, error) {
		var event jobs.HistoryEvent
		err := row.Scan(&event.Sequence, &event.JobID, &event.Kind, &event.At, &event.WorkerID, &event.Attempt, &event.Detail)
		return event, err
	})
}
