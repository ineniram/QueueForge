package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"queueforge/internal/jobs"
	"queueforge/internal/storage"
)

type Repository interface {
	Create(context.Context, []jobs.Input) ([]jobs.Job, error)
	Submit(context.Context, jobs.Input, jobs.Policy, *string, string) (jobs.Job, bool, error)
	Recover(context.Context) error
	MarkStale(context.Context) error
	Retry(context.Context, string) (jobs.Job, error)
	History(context.Context, *string) ([]jobs.HistoryEvent, error)
	RequestCrash(context.Context, string) (string, string, error)
	Get(context.Context, string) (jobs.Job, error)
	List(context.Context, int, int) ([]jobs.Job, error)
	Pending(context.Context, int) ([]jobs.Ref, error)
	Workers(context.Context) ([]jobs.Worker, error)
	Metrics(context.Context) (jobs.Metrics, error)
	Activity(context.Context) ([]jobs.Event, error)
	Ping(context.Context) error
}

type Queue interface {
	Publish(context.Context, []jobs.Ref) error
	Ping(context.Context) error
}

type Server struct {
	Store       Repository
	Queue       Queue
	Log         *slog.Logger
	DemoEnabled bool
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/jobs", s.submit)
	mux.HandleFunc("POST /api/demo", s.demo)
	mux.HandleFunc("GET /api/jobs", s.list)
	mux.HandleFunc("GET /api/jobs/{id}", s.get)
	mux.HandleFunc("GET /api/jobs/{id}/history", s.history)
	mux.HandleFunc("POST /api/jobs/{id}/retry", s.retry)
	mux.HandleFunc("GET /api/history", read(s, func(ctx context.Context) ([]jobs.HistoryEvent, error) { return s.Store.History(ctx, nil) }))
	mux.HandleFunc("GET /api/failure-lab", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]bool{"enabled": s.DemoEnabled}) })
	mux.HandleFunc("POST /api/failure-lab/crash", s.crash)
	mux.HandleFunc("GET /api/workers", read(s, s.Store.Workers))
	mux.HandleFunc("GET /api/metrics", read(s, s.Store.Metrics))
	mux.HandleFunc("GET /api/activity", read(s, s.Store.Activity))
	mux.HandleFunc("GET /health", s.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		w.Header().Set("Cache-Control", "no-store")
		_, pattern := mux.Handler(r)
		if pattern != "" {
			// ServeMux populates path values when it dispatches the request.
			mux.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			probe := r.Clone(ctx)
			probe.Method = method
			if _, match := mux.Handler(probe); match != "" && method != r.Method {
				allow := method
				if method == http.MethodGet {
					allow += ", HEAD"
				}
				other := r.Clone(ctx)
				other.Method = http.MethodPost
				if method == http.MethodGet {
					if _, postMatch := mux.Handler(other); postMatch != "" {
						allow += ", POST"
					}
				}
				w.Header().Set("Allow", allow)
				problem(w, 405, "method_not_allowed", "Method is unavailable for this endpoint")
				return
			}
		}
		problem(w, 404, "not_found", "Endpoint not found")
	})
}

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func problem(w http.ResponseWriter, status int, code, message string) {
	write(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func (s Server) unavailable(w http.ResponseWriter, err error) {
	s.Log.Error("request failed", "error", err)
	problem(w, 503, "unavailable", "Job storage is temporarily unavailable")
}

func read[T any](s Server, load func(context.Context) (T, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value, err := load(r.Context())
		if err != nil {
			s.unavailable(w, err)
			return
		}
		write(w, 200, value)
	}
}

func body(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := jobs.Decode(http.MaxBytesReader(w, r.Body, 16<<10), target); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			problem(w, 413, "body_too_large", "Request exceeds 16 KiB")
		} else {
			problem(w, 400, "invalid_json", "Expected one JSON object with supported fields")
		}
		return false
	}
	return true
}

var submissionKey = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

func (s Server) submit(w http.ResponseWriter, r *http.Request) {
	request := struct {
		jobs.Input
		Retry *jobs.Policy `json:"retry"`
	}{Retry: &jobs.Policy{MaxAttempts: 3, BackoffMS: 500, MaxBackoffMS: 10000}}
	if !body(w, r, &request) {
		return
	}
	input, err := jobs.Normalize(request.Input)
	if err != nil {
		problem(w, 422, "invalid_job", err.Error())
		return
	}
	policy := request.Retry
	if policy == nil || policy.MaxAttempts < 1 || policy.MaxAttempts > 10 || policy.BackoffMS < 100 || policy.BackoffMS > 60000 ||
		policy.MaxBackoffMS < policy.BackoffMS || policy.MaxBackoffMS > 300000 {
		problem(w, 422, "invalid_retry_policy", "Use 1–10 attempts, a base delay of 100–60000 ms and a cap between the base and 300000 ms")
		return
	}
	var key *string
	var digest string
	if values := r.Header.Values("Idempotency-Key"); len(values) > 0 {
		if len(values) != 1 || !submissionKey.MatchString(values[0]) {
			problem(w, 400, "invalid_idempotency_key", "Expected one key of 1–128 letters, digits or ._:-")
			return
		}
		key = &values[0]
		hash := sha256.Sum256(fmt.Appendf(nil, "%s %d %d %d %s", input.Type, policy.MaxAttempts, policy.BackoffMS, policy.MaxBackoffMS, input.Payload))
		digest = hex.EncodeToString(hash[:])
	}
	job, created, err := s.Store.Submit(r.Context(), input, *policy, key, digest)
	if errors.Is(err, storage.ErrIdempotencyConflict) {
		problem(w, 409, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		s.unavailable(w, err)
		return
	}
	w.Header().Set("Location", "/api/jobs/"+job.ID)
	code := 200
	if created {
		code = 201
		s.publish(r.Context(), []jobs.Job{job})
	}
	write(w, code, job)
}

func (s Server) demo(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Count   int    `json:"count"`
		Profile string `json:"profile"`
	}
	if !body(w, r, &input) {
		return
	}
	if input.Count < 1 || input.Count > 100 {
		problem(w, 422, "invalid_count", "Count must be between 1 and 100")
		return
	}
	inputs := jobs.Demo(input.Count)
	if input.Profile != "" {
		if input.Profile != "failure" {
			problem(w, 422, "invalid_profile", "Unknown demo profile")
			return
		}
		if !s.DemoEnabled || r.Header.Get("X-QueueForge-Demo") != "1" {
			problem(w, 403, "lab_disabled", "Failure Lab requires local demo mode")
			return
		}
		for i := range inputs {
			inputs[i] = jobs.Input{Type: "sleep", Payload: json.RawMessage(`{"duration_ms":10000}`)}
		}
	}
	created, ok := s.create(w, r, inputs)
	if ok {
		write(w, 201, map[string]any{"count": len(created), "jobs": created})
	}
}

func (s Server) create(w http.ResponseWriter, r *http.Request, inputs []jobs.Input) ([]jobs.Job, bool) {
	created, err := s.Store.Create(r.Context(), inputs)
	if err != nil {
		s.unavailable(w, err)
		return nil, false
	}
	s.publish(r.Context(), created)
	return created, true
}

func (s Server) publish(parent context.Context, created []jobs.Job) {
	refs := make([]jobs.Ref, 0, len(created))
	for _, j := range created {
		refs = append(refs, jobs.Ref{ID: j.ID, CreatedAt: j.CreatedAt})
	}
	ctx, cancel := context.WithTimeout(parent, 750*time.Millisecond)
	defer cancel()
	if err := s.Queue.Publish(ctx, refs); err != nil {
		s.Log.Warn("immediate dispatch deferred", "count", len(refs), "error", err)
	}
}

func (s Server) list(w http.ResponseWriter, r *http.Request) {
	limit, offset := 25, 0
	for name, target := range map[string]*int{"limit": &limit, "offset": &offset} {
		if raw, present := r.URL.Query()[name]; present {
			value, err := strconv.Atoi(raw[0])
			if err != nil || len(raw) != 1 {
				problem(w, 400, "invalid_page", "Supply one integer per pagination parameter")
				return
			}
			*target = value
		}
	}
	if limit < 1 || limit > 100 || offset < 0 || offset > 1000000 {
		problem(w, 400, "invalid_page", "Limit must be 1–100 and offset 0–1000000")
		return
	}
	result, err := s.Store.List(r.Context(), limit+1, offset)
	if err != nil {
		s.unavailable(w, err)
		return
	}
	more := len(result) > limit
	if more {
		result = result[:limit]
	}
	write(w, 200, map[string]any{"jobs": result, "limit": limit, "offset": offset, "has_more": more})
}

var uuid = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func (s Server) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuid.MatchString(id) {
		problem(w, 400, "invalid_id", "Expected a UUID")
		return
	}
	result, err := s.Store.Get(r.Context(), id)
	switch {
	case storage.IsNotFound(err):
		problem(w, 404, "not_found", "Job not found")
	case err != nil:
		s.unavailable(w, err)
	default:
		write(w, 200, result)
	}
}

func (s Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	results := make(chan string, 2)
	go func() {
		if s.Store.Ping(ctx) != nil {
			results <- "postgres"
		} else {
			results <- ""
		}
	}()
	go func() {
		if s.Queue.Ping(ctx) != nil {
			results <- "redis"
		} else {
			results <- ""
		}
	}()
	dependencies := map[string]string{"postgres": "ok", "redis": "ok"}
	code, status := 200, "ok"
	for range 2 {
		if failed := <-results; failed != "" {
			dependencies[failed] = "unavailable"
			code, status = 503, "degraded"
		}
	}
	write(w, code, map[string]any{"status": status, "dependencies": dependencies})
}

func (s Server) Dispatch(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		cycle, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		if err := s.Store.MarkStale(cycle); err != nil && ctx.Err() == nil {
			s.Log.Warn("worker health scan failed", "error", err)
		}
		if err := s.Store.Recover(cycle); err != nil && ctx.Err() == nil {
			s.Log.Warn("lease recovery failed", "error", err)
		}
		refs, err := s.Store.Pending(cycle, 500)
		if err == nil {
			err = s.Queue.Publish(cycle, refs)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			s.Log.Warn("queued job reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s Server) history(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuid.MatchString(id) {
		problem(w, 400, "invalid_id", "Expected a UUID")
		return
	}
	if _, err := s.Store.Get(r.Context(), id); err != nil {
		if storage.IsNotFound(err) {
			problem(w, 404, "not_found", "Job not found")
		} else {
			s.unavailable(w, err)
		}
		return
	}
	read(s, func(ctx context.Context) ([]jobs.HistoryEvent, error) { return s.Store.History(ctx, &id) })(w, r)
}

func (s Server) retry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuid.MatchString(id) {
		problem(w, 400, "invalid_id", "Expected a UUID")
		return
	}
	job, err := s.Store.Retry(r.Context(), id)
	if storage.IsNotFound(err) {
		if _, err = s.Store.Get(r.Context(), id); storage.IsNotFound(err) {
			problem(w, 404, "not_found", "Job not found")
		} else if err != nil {
			s.unavailable(w, err)
		} else {
			problem(w, 409, "invalid_state", "Only permanently failed jobs can be retried")
		}
		return
	}
	if err != nil {
		s.unavailable(w, err)
		return
	}
	s.publish(r.Context(), []jobs.Job{job})
	write(w, 202, job)
}

func (s Server) crash(w http.ResponseWriter, r *http.Request) {
	if !s.DemoEnabled || r.Header.Get("X-QueueForge-Demo") != "1" {
		problem(w, 403, "lab_disabled", "Failure Lab requires local demo mode")
		return
	}
	var input struct {
		Slot string `json:"slot"`
	}
	if !body(w, r, &input) {
		return
	}
	if !jobs.DemoSlot(input.Slot) {
		problem(w, 422, "invalid_slot", "Expected an allowlisted demo worker")
		return
	}
	workerID, jobID, err := s.Store.RequestCrash(r.Context(), input.Slot)
	if storage.IsNotFound(err) {
		problem(w, 409, "worker_unavailable", "Select a healthy demo worker processing a job")
		return
	}
	if err != nil {
		s.unavailable(w, err)
		return
	}
	write(w, 202, map[string]string{"worker_id": workerID, "job_id": jobID})
}
