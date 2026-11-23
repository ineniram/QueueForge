package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"queueforge/internal/worker"
	"queueforge/migrations"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"queueforge/internal/api"
	"queueforge/internal/jobs"
	"queueforge/internal/queue"
	"queueforge/internal/storage"
)

func TestEngine(t *testing.T) {
	databaseURL, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_URL")
	if databaseURL == "" || redisURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_REDIS_URL for integration tests")
	}
	ctx := t.Context()
	var random [10]byte
	rand.Read(random[:])
	schema := "test_" + hex.EncodeToString(random[:])
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	dsn, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := dsn.Query()
	t.Run("upgrade from Phase 1", func(t *testing.T) { testUpgrade(t, admin, dsn, schema+"_legacy") })
	query.Set("search_path", schema)
	dsn.RawQuery = query.Encode()
	store, err := storage.Open(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := queue.Open(redisURL, "queueforge:test:"+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	handler := (api.Server{Store: store, Queue: pending, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()

	call := func(method, path, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
		if response.Code != status {
			t.Fatalf("%s %s: got %d, want %d: %s", method, path, response.Code, status, response.Body)
		}
		if response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("%s %s did not return JSON", method, path)
		}
		return response
	}

	call("GET", "/health", "", 200)
	call("GET", "/api/jobs?limit=-1", "", 400)
	call("GET", "/api/jobs/not-a-uuid", "", 400)
	call("GET", "/api/jobs/00000000-0000-0000-0000-000000000000", "", 404)
	call("PATCH", "/api/jobs", "", 405)
	call("POST", "/api/jobs", `{"type":"compute","payload":{"limit":1}}`, 422)
	call("POST", "/api/demo", `{"count":101}`, 422)
	response := call("POST", "/api/jobs", `{"type":"compute","payload":{"limit":100}}`, 201)
	var created jobs.Job
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil || created.ID == "" || created.Status != jobs.Queued {
		t.Fatalf("invalid creation response: %+v, %v", created, err)
	}
	if response.Header().Get("Location") != "/api/jobs/"+created.ID {
		t.Fatal("missing job location")
	}
	call("GET", "/api/jobs/"+created.ID, "", 200)
	call("GET", "/api/jobs", "", 200)

	refs, err := store.Pending(ctx, 10)
	if err != nil || len(refs) != 1 {
		t.Fatalf("pending: %v %v", refs, err)
	}
	if err := pending.Publish(ctx, refs); err != nil {
		t.Fatal(err)
	}
	first, err := pending.Pop(ctx)
	if err != nil || first != created.ID {
		t.Fatalf("first pop: %s %v", first, err)
	}
	empty, err := pending.Pop(ctx)
	if err != nil || empty != "" {
		t.Fatalf("deduplication failed: %s %v", empty, err)
	}
	// Republish after the pop to model a dispatcher racing a worker's database claim.
	if err := pending.Publish(ctx, refs); err != nil {
		t.Fatal(err)
	}
	second, err := pending.Pop(ctx)
	if err != nil || second != first {
		t.Fatalf("redelivery: %s %v", second, err)
	}

	for _, id := range []string{"worker-a", "worker-b"} {
		if err := store.ReportWorker(ctx, id, "idle"); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	type claimResult struct {
		job jobs.Job
		err error
	}
	results := make(chan claimResult, 2)
	for i, id := range []string{"worker-a", "worker-b"} {
		go func(jobID, workerID string) {
			ready.Done()
			<-start
			job, err := store.Claim(ctx, jobID, workerID)
			results <- claimResult{job, err}
		}([]string{first, second}[i], id)
	}
	ready.Wait()
	close(start)
	wins, misses := 0, 0
	var winner jobs.Job
	for range 2 {
		outcome := <-results
		switch {
		case outcome.err == nil:
			wins++
			winner = outcome.job
		case storage.IsNotFound(outcome.err):
			misses++
		default:
			t.Fatal(outcome.err)
		}
	}
	if wins != 1 || misses != 1 || winner.Status != jobs.Running || winner.StartedAt == nil || winner.WorkerID == nil {
		t.Fatalf("claims: %d successful, %d rejected, job %+v", wins, misses, winner)
	}
	if err := store.Finish(ctx, first, "wrong-owner", winner.Fence, json.RawMessage(`{}`), nil); !storage.IsNotFound(err) {
		t.Fatalf("wrong worker finish: %v", err)
	}
	result, err := jobs.Execute(ctx, winner.Input)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, first, *winner.WorkerID, winner.Fence, result, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, first, *winner.WorkerID, winner.Fence, result, nil); !storage.IsNotFound(err) {
		t.Fatalf("duplicate finish: %v", err)
	}
	completed, err := store.Get(ctx, first)
	if err != nil || completed.Status != jobs.Completed || completed.CompletedAt == nil {
		t.Fatalf("completed job: %+v %v", completed, err)
	}
	var values map[string]int
	if err := json.Unmarshal(completed.Result, &values); err != nil || values["prime_count"] != 25 {
		t.Fatalf("persisted result: %s %v", completed.Result, err)
	}

	other, _, err := store.Submit(ctx, jobs.Demo(1)[0], jobs.Policy{MaxAttempts: 1, BackoffMS: 500, MaxBackoffMS: 10000}, nil, "")
	if err != nil {
		t.Fatalf("second creation: %v %v", other, err)
	}
	attempt, err := store.Claim(ctx, other.ID, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, other.ID, "worker-a", attempt.Fence, nil, errors.New("test handler failure")); err != nil {
		t.Fatal(err)
	}
	failed, err := store.Get(ctx, other.ID)
	if err != nil || failed.Status != jobs.Failed || failed.Error == nil || *failed.Error != "test handler failure" {
		t.Fatalf("failed job: %+v %v", failed, err)
	}
	metrics, err := store.Metrics(ctx)
	if err != nil || metrics.Total != 2 || metrics.Completed != 1 || metrics.Failed != 1 || metrics.ActiveWorkers != 2 {
		t.Fatalf("metrics: %+v %v", metrics, err)
	}
	activity, err := store.Activity(ctx)
	if err != nil || len(activity) != 6 || activity[0].Status != jobs.Failed {
		t.Fatalf("activity: %+v %v", activity, err)
	}
	call("GET", "/api/workers", "", 200)
	call("GET", "/api/metrics", "", 200)
	call("GET", "/api/activity", "", 200)
	call("GET", "/api/jobs?limit=1&offset=1", "", 200)

	// Check that a standalone dispatcher repairs missing queue entries.
	lost, err := store.Create(ctx, jobs.Demo(1))
	if err != nil {
		t.Fatal(err)
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	service := api.Server{Store: store, Queue: pending, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go func() { defer close(done); service.Dispatch(dispatchCtx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(4 * time.Second)
	for {
		id, err := pending.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if id == lost[0].ID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dispatcher did not restore the missing Redis entry")
		}
	}
	cancel()
	<-done
	t.Run("reliability", func(t *testing.T) { testReliability(t, store, handler, redisURL, schema) })
	t.Run("temporary Redis outage", func(t *testing.T) { testRedisOutage(t, store, redisURL, schema) })
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(7 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func testReliability(t *testing.T, store *storage.Store, handler http.Handler, redisURL, schema string) {
	ctx := t.Context()
	store.Lease = 350 * time.Millisecond
	store.StaleAfter = 100 * time.Millisecond
	store.OfflineAfter = 225 * time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	submit := func(key, payload string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/api/jobs", bytes.NewBufferString(payload))
		req.Header.Set("Idempotency-Key", key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	const payload = `{"type":"compute","payload":{"limit":100},"retry":{"max_attempts":3,"backoff_ms":200,"max_backoff_ms":250}}`
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() { <-start; responses <- submit("stable-key", payload) }()
	}
	close(start)
	outcomes := map[int]int{}
	var job jobs.Job
	for range 2 {
		res := <-responses
		outcomes[res.Code]++
		var current jobs.Job
		if err := json.Unmarshal(res.Body.Bytes(), &current); err != nil || current.ID == "" {
			t.Fatalf("submit %d: %s", res.Code, res.Body)
		}
		if job.ID != "" && current.ID != job.ID {
			t.Fatalf("replay created another job: %s != %s", job.ID, current.ID)
		}
		job = current
	}
	if outcomes[201] != 1 || outcomes[200] != 1 {
		t.Fatalf("submission responses: %v", outcomes)
	}
	if conflict := submit("stable-key", `{"type":"compute","payload":{"limit":101}}`); conflict.Code != 409 {
		t.Fatal(conflict.Body)
	}
	disabled := httptest.NewRecorder()
	handler.ServeHTTP(disabled, httptest.NewRequest("POST", "/api/failure-lab/crash", bytes.NewBufferString(`{"slot":"worker-1"}`)))
	if disabled.Code != 403 {
		t.Fatalf("lab defaults on: %d", disabled.Code)
	}

	for _, id := range []string{"expired-owner", "healthy-owner"} {
		if err := store.ReportWorker(ctx, id, "idle"); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.Claim(ctx, job.ID, "expired-owner")
	if err != nil || first.LeaseExpiresAt == nil {
		t.Fatalf("first claim: %v", err)
	}
	time.Sleep(125 * time.Millisecond)
	if err := store.MarkStale(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		workers, err := store.Workers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range workers {
			if w.ID == "expired-owner" {
				return w.Health == "offline"
			}
		}
		return false
	})
	time.Sleep(time.Until(*first.LeaseExpiresAt) + 15*time.Millisecond)
	if err := store.Renew(ctx, job.ID, "expired-owner", first.Fence); !storage.IsNotFound(err) {
		t.Fatalf("expired lease renewed: %v", err)
	}
	if err := store.Finish(ctx, job.ID, "expired-owner", first.Fence, json.RawMessage(`{}`), nil); !storage.IsNotFound(err) {
		t.Fatalf("expired owner finished: %v", err)
	}

	recoverStart := make(chan struct{})
	recovery := make(chan error, 5)
	for range 5 {
		go func() { <-recoverStart; recovery <- store.Recover(ctx) }()
	}
	close(recoverStart)
	for range 5 {
		if err := <-recovery; err != nil {
			t.Fatal(err)
		}
	}
	queued, err := store.Get(ctx, job.ID)
	if err != nil || queued.Status != jobs.Queued || queued.WorkerID != nil || queued.LeaseExpiresAt != nil || time.Until(queued.EligibleAt) < 50*time.Millisecond {
		t.Fatalf("missing recovery backoff: %+v %v", queued, err)
	}
	if _, err := store.Claim(ctx, job.ID, "healthy-owner"); !storage.IsNotFound(err) {
		t.Fatalf("early retry: %v", err)
	}
	time.Sleep(time.Until(queued.EligibleAt) + 10*time.Millisecond)
	second, err := store.Claim(ctx, job.ID, "healthy-owner")
	if err != nil || second.Fence <= first.Fence || second.AttemptCount != 2 {
		t.Fatalf("second claim: %+v %v", second, err)
	}
	if _, err := store.Claim(ctx, job.ID, "expired-owner"); !storage.IsNotFound(err) {
		t.Fatalf("second live owner: %v", err)
	}
	if err := store.Finish(ctx, job.ID, "healthy-owner", first.Fence, json.RawMessage(`{"wrong":true}`), nil); !storage.IsNotFound(err) {
		t.Fatalf("old fence with valid worker identity finished: %v", err)
	}
	if err := store.Finish(ctx, job.ID, "healthy-owner", second.Fence, json.RawMessage(`{"prime_count":25}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, job.ID, "expired-owner", first.Fence, json.RawMessage(`{"wrong":true}`), nil); !storage.IsNotFound(err) {
		t.Fatalf("stale result overwrite: %v", err)
	}
	completed, err := store.Get(ctx, job.ID)
	var result map[string]int
	if err != nil || json.Unmarshal(completed.Result, &result) != nil || result["prime_count"] != 25 {
		t.Fatalf("winner result changed: %+v %v", completed, err)
	}
	events, err := store.History(ctx, &job.ID)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Kind]++
	}
	for _, kind := range []string{"worker_stale", "lease_expired", "recovered", "retry_scheduled", "completed"} {
		if counts[kind] != 1 {
			t.Fatalf("%s events: %v", kind, counts)
		}
	}
	if counts["claimed"] != 2 {
		t.Fatalf("claim events: %v", counts)
	}

	failing, _, err := store.Submit(ctx, jobs.Demo(1)[0], job.Policy, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		attempt, err := store.Claim(ctx, failing.ID, "healthy-owner")
		if err != nil || attempt.AttemptCount != i {
			t.Fatalf("claim %d: %+v %v", i, attempt, err)
		}
		before := time.Now()
		if err := store.Finish(ctx, failing.ID, "healthy-owner", attempt.Fence, nil, errors.New("test failure")); err != nil {
			t.Fatal(err)
		}
		state, err := store.Get(ctx, failing.ID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			if state.Status != jobs.Failed || state.Error == nil || state.LastError == nil {
				t.Fatalf("expected dead letter: %+v", state)
			}
			break
		}
		expected := time.Duration(min(200<<(i-1), 250)) * time.Millisecond
		if state.Status != jobs.Queued || state.EligibleAt.Before(before.Add(expected)) || state.EligibleAt.After(time.Now().Add(expected)) {
			t.Fatalf("backoff %d: %+v", i, state)
		}
		time.Sleep(time.Until(state.EligibleAt) + 10*time.Millisecond)
	}
	retry := httptest.NewRecorder()
	handler.ServeHTTP(retry, httptest.NewRequest("POST", "/api/jobs/"+failing.ID+"/retry", nil))
	if retry.Code != 202 {
		t.Fatal(retry.Body)
	}
	retried, err := store.Claim(ctx, failing.ID, "healthy-owner")
	if err != nil || retried.AttemptCount != 4 || retried.AttemptsUsed != 1 || retried.Fence <= 3 {
		t.Fatalf("manual retry: %+v %v", retried, err)
	}
	if err := store.Finish(ctx, failing.ID, "healthy-owner", retried.Fence, json.RawMessage(`{}`), nil); err != nil {
		t.Fatal(err)
	}

	liveQueue, err := queue.Open(redisURL, "queueforge:test:graceful:"+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer liveQueue.Close()
	realJob, _, err := store.Submit(ctx, jobs.Input{Type: "sleep", Payload: json.RawMessage(`{"duration_ms":900}`)}, job.Policy, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := liveQueue.Publish(ctx, []jobs.Ref{{ID: realJob.ID, CreatedAt: realJob.CreatedAt}}); err != nil {
		t.Fatal(err)
	}
	runnerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runnerResult := make(chan error, 1)
	go func() {
		runnerResult <- (worker.Runner{ID: "graceful-worker", Store: store, Queue: liveQueue, Log: log, Interval: 75 * time.Millisecond}).Run(runnerCtx)
	}()
	eventually(t, func() bool { j, err := store.Get(ctx, realJob.ID); return err == nil && j.Status == jobs.Running })
	cancel()
	select {
	case err := <-runnerResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down")
	}
	final, err := store.Get(ctx, realJob.ID)
	if err != nil || final.Status != jobs.Completed || final.AttemptCount != 1 {
		t.Fatalf("job lost on graceful shutdown: %+v %v", final, err)
	}
	workers, err := store.Workers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range workers {
		if w.ID == "graceful-worker" && w.State == "stopped" && w.Health == "offline" {
			return
		}
	}
	t.Fatal("graceful worker did not report stopped")
}

func testUpgrade(t *testing.T, admin *pgx.Conn, dsn *url.URL, schema string) {
	ctx := t.Context()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	urlCopy := *dsn
	params := urlCopy.Query()
	params.Set("search_path", schema)
	urlCopy.RawQuery = params.Encode()
	legacy, err := pgx.Connect(ctx, urlCopy.String())
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close(ctx)
	if _, err := legacy.Exec(ctx, migrations.Initial, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(ctx, "INSERT INTO workers (id,state) VALUES ('legacy-worker','busy')"); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := legacy.QueryRow(ctx, `INSERT INTO jobs(id,type,payload,status,worker_id,started_at)
 VALUES(gen_random_uuid(),'sleep','{"duration_ms":5}','running','legacy-worker',clock_timestamp()) RETURNING id::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(ctx, urlCopy.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for range 2 {
		if err := store.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	upgraded, err := store.Get(ctx, id)
	if err != nil || upgraded.Status != jobs.Queued || upgraded.AttemptCount != 1 || upgraded.Fence != 1 {
		t.Fatalf("legacy job: %+v %v", upgraded, err)
	}
	events, err := store.History(ctx, &id)
	if err != nil || len(events) != 5 || events[0].Kind != "submitted" || events[1].Kind != "claimed" || events[2].Kind != "lease_expired" {
		t.Fatalf("legacy history: %+v %v", events, err)
	}
}

type interruptedQueue struct {
	*queue.Queue
	unavailable atomic.Bool
}

func (q *interruptedQueue) Publish(ctx context.Context, refs []jobs.Ref) error {
	if q.unavailable.Load() {
		return errors.New("temporary Redis transport failure")
	}
	return q.Queue.Publish(ctx, refs)
}

func testRedisOutage(t *testing.T, store *storage.Store, redisURL, schema string) {
	ctx := t.Context()
	pending, err := queue.Open(redisURL, "queueforge:test:outage:"+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	ioQueue := &interruptedQueue{Queue: pending}
	ioQueue.unavailable.Store(true)
	server := api.Server{Store: store, Queue: ioQueue, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	result := httptest.NewRecorder()
	server.Handler().ServeHTTP(result, httptest.NewRequest("POST", "/api/jobs", bytes.NewBufferString(`{"type":"sleep","payload":{"duration_ms":5}}`)))
	var accepted jobs.Job
	if result.Code != 201 || json.Unmarshal(result.Body.Bytes(), &accepted) != nil || accepted.Status != jobs.Queued {
		t.Fatalf("outage submission: %s", result.Body)
	}
	id, err := pending.Pop(ctx)
	if err != nil || id != "" {
		t.Fatalf("unexpected immediate dispatch: %s %v", id, err)
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); server.Dispatch(dispatchCtx) }()
	defer func() { cancel(); <-done }()
	ioQueue.unavailable.Store(false)
	eventually(t, func() bool {
		id, err := pending.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return id == accepted.ID
	})
}
