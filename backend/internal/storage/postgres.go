package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"queueforge/internal/jobs"
	"queueforge/migrations"
)

type Store struct {
	db           *pgxpool.Pool
	Lease        time.Duration
	StaleAfter   time.Duration
	OfflineAfter time.Duration
}

func Open(ctx context.Context, url string) (*Store, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, Lease: 6 * time.Second, StaleAfter: 2500 * time.Millisecond, OfflineAfter: 5 * time.Second}, nil
}

func (s *Store) Close()                         { s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(697105101)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS queueforge_migrations(version integer PRIMARY KEY)`); err != nil {
		return err
	}
	for index, migration := range []string{migrations.Initial, migrations.Reliability} {
		tag, err := tx.Exec(ctx, "INSERT INTO queueforge_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING", index+1)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, migration, pgx.QueryExecModeSimpleProtocol); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const columns = "id::text, type, payload, status, created_at, started_at, completed_at, worker_id, error, result, lease_expires_at, fence, attempt_count, max_attempts, backoff_ms, max_backoff_ms, eligible_at, last_error, retries_used"

func scanJob(row interface{ Scan(...any) error }) (jobs.Job, error) {
	var j jobs.Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.WorkerID, &j.Error, &j.Result, &j.LeaseExpiresAt, &j.Fence, &j.AttemptCount, &j.MaxAttempts, &j.BackoffMS, &j.MaxBackoffMS, &j.EligibleAt, &j.LastError, &j.AttemptsUsed)
	return j, err
}

func (s *Store) Create(ctx context.Context, inputs []jobs.Input) ([]jobs.Job, error) {
	body, err := json.Marshal(inputs)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `
  INSERT INTO jobs (id,type,payload)
  SELECT gen_random_uuid(), type, payload
  FROM jsonb_to_recordset($1::jsonb) AS input(type text, payload jsonb)
  RETURNING `+columns, body)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Job, error) { return scanJob(row) })
}

func (s *Store) Get(ctx context.Context, id string) (jobs.Job, error) {
	return scanJob(s.db.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1", id))
}

func (s *Store) List(ctx context.Context, limit, offset int) ([]jobs.Job, error) {
	rows, err := s.db.Query(ctx, "SELECT "+columns+" FROM jobs ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2", limit, offset)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Job, error) { return scanJob(row) })
}

func (s *Store) Pending(ctx context.Context, limit int) ([]jobs.Ref, error) {
	rows, err := s.db.Query(ctx, "SELECT id::text, created_at FROM jobs WHERE status='queued' AND eligible_at <= statement_timestamp() ORDER BY eligible_at, created_at, id LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Ref, error) {
		var ref jobs.Ref
		err := row.Scan(&ref.ID, &ref.CreatedAt)
		return ref, err
	})
}

func (s *Store) Metrics(ctx context.Context) (jobs.Metrics, error) {
	var m jobs.Metrics
	err := s.db.QueryRow(ctx, `
  SELECT count(*), count(*) FILTER (WHERE status='queued'), count(*) FILTER (WHERE status='running'),
    count(*) FILTER (WHERE status='completed'), count(*) FILTER (WHERE status='failed'),
    (SELECT count(*) FROM workers WHERE state IN ('idle','busy') AND reported_at > statement_timestamp()-($1::bigint * interval '1 millisecond')),
    COALESCE(avg(extract(epoch FROM completed_at-started_at)*1000),0)::float8,
    COALESCE(avg(extract(epoch FROM started_at-created_at)*1000),0)::float8
  FROM jobs`, s.StaleAfter.Milliseconds()).Scan(&m.Total, &m.Queued, &m.Running, &m.Completed, &m.Failed, &m.ActiveWorkers, &m.AverageRunMS, &m.AverageWaitMS)
	return m, err
}

func (s *Store) Activity(ctx context.Context) ([]jobs.Event, error) {
	rows, err := s.db.Query(ctx, `
  SELECT id::text,status,at,worker_id FROM (
    (SELECT id,'queued' AS status,created_at AS at,NULL::text AS worker_id FROM jobs ORDER BY created_at DESC,id DESC LIMIT 20)
    UNION ALL
    (SELECT id,'running',started_at,worker_id FROM jobs WHERE started_at IS NOT NULL ORDER BY started_at DESC,id DESC LIMIT 20)
    UNION ALL
    (SELECT id,status,completed_at,worker_id FROM jobs WHERE completed_at IS NOT NULL ORDER BY completed_at DESC,id DESC LIMIT 20)
  ) AS events ORDER BY at DESC,id DESC,status LIMIT 20`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.Event, error) {
		var e jobs.Event
		err := row.Scan(&e.JobID, &e.Status, &e.At, &e.WorkerID)
		return e, err
	})
}

func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
