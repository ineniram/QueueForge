package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"queueforge/internal/jobs"
	"queueforge/internal/storage"
)

type Repository interface {
	Heartbeat(context.Context, string, string, *string, *string) (bool, error)
	Renew(context.Context, string, string, int64) error
	Claim(context.Context, string, string) (jobs.Job, error)
	Finish(context.Context, string, string, int64, json.RawMessage, error) error
}

type Runner struct {
	ID    string
	Store Repository
	Queue interface {
		Pop(context.Context) (string, error)
	}
	Log      *slog.Logger
	Interval time.Duration
	Slot     *string
	Crash    func()
}

type execution struct {
	job    jobs.Job
	cancel context.CancelFunc
}

func (w Runner) Run(ctx context.Context) error {
	if _, err := w.report(ctx, "idle", nil); err != nil {
		return err
	}
	lifetime, stop := context.WithCancel(context.WithoutCancel(ctx))
	var active atomic.Pointer[execution]
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-lifetime.Done():
				return
			case <-ticker.C:
				w.beat(lifetime, active.Load())
			}
		}
	}()
	defer func() {
		stop()
		<-done
		if _, err := w.report(context.WithoutCancel(ctx), "stopped", nil); err != nil {
			w.Log.Warn("failed to report shutdown", "error", err)
		}
	}()
	w.Log.Info("worker registered")
	for ctx.Err() == nil {
		popCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		id, err := w.Queue.Pop(popCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.Log.Warn("dequeue failed", "error", err)
		} else if id == "" {
			continue
		} else if err = w.process(context.WithoutCancel(ctx), id, &active); err == nil {
			continue
		} else {
			w.Log.Error("job processing failed", "job_id", id, "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	return nil
}

func (w Runner) report(ctx context.Context, state string, jobID *string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, w.Interval)
	defer cancel()
	return w.Store.Heartbeat(ctx, w.ID, state, w.Slot, jobID)
}

func (w Runner) beat(ctx context.Context, current *execution) {
	state := "idle"
	var jobID *string
	if current != nil {
		state = "busy"
		jobID = &current.job.ID
	}
	crash, err := w.report(ctx, state, jobID)
	if err != nil {
		w.Log.Warn("heartbeat failed", "error", err)
	}
	if crash && w.Slot != nil && w.Crash != nil {
		w.Log.Warn("Failure Lab requested abrupt exit")
		w.Crash()
	}
	if current == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, w.Interval)
	defer cancel()
	if err := w.Store.Renew(ctx, current.job.ID, w.ID, current.job.Fence); err != nil {
		if storage.IsNotFound(err) {
			current.cancel()
		}
		w.Log.Warn("lease renewal rejected", "job_id", current.job.ID, "error", err)
	}
}

func (w Runner) process(ctx context.Context, id string, active *atomic.Pointer[execution]) error {
	claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	job, err := w.Store.Claim(claimCtx, id, w.ID)
	cancel()
	if storage.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	w.Log.Info("job started", "job_id", id, "type", job.Type, "attempt", job.AttemptCount, "fence", job.Fence)
	runCtx, stop := context.WithTimeout(ctx, 12*time.Second)
	defer stop()
	active.Store(&execution{job: job, cancel: stop})
	defer active.Store(nil)
	result, runErr := jobs.Execute(runCtx, job.Input)
	finishCtx, cancelFinish := context.WithTimeout(ctx, 3*time.Second)
	defer cancelFinish()
	err = w.Store.Finish(finishCtx, id, w.ID, job.Fence, result, runErr)
	if err != nil {
		return err
	}
	if runErr != nil {
		w.Log.Warn("job failed", "job_id", id, "error", runErr)
	} else {
		w.Log.Info("job completed", "job_id", id)
	}
	return nil
}
