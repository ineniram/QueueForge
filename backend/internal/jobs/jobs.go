package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Completed Status = "completed"
	Failed    Status = "failed"
)

type Input struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Policy struct {
	MaxAttempts  int `json:"max_attempts"`
	BackoffMS    int `json:"backoff_ms"`
	MaxBackoffMS int `json:"max_backoff_ms"`
}

type HistoryEvent struct {
	Sequence int64     `json:"sequence"`
	JobID    *string   `json:"job_id"`
	Kind     string    `json:"kind"`
	At       time.Time `json:"at"`
	WorkerID *string   `json:"worker_id"`
	Attempt  *int      `json:"attempt"`
	Detail   *string   `json:"detail"`
}

type Job struct {
	Policy
	LeaseExpiresAt *time.Time `json:"lease_expires_at"`
	Fence          int64      `json:"fencing_token,string"`
	AttemptCount   int        `json:"attempt_count"`
	AttemptsUsed   int        `json:"attempts_used"`
	EligibleAt     time.Time  `json:"eligible_at"`
	LastError      *string    `json:"last_error"`

	ID string `json:"id"`
	Input
	Status      Status          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at"`
	WorkerID    *string         `json:"worker_id"`
	Error       *string         `json:"error"`
	Result      json.RawMessage `json:"result"`
}

type Ref struct {
	ID        string
	CreatedAt time.Time
}

type Worker struct {
	ReportedJob    *string `json:"reported_job_id"`
	Health         string  `json:"health"`
	CurrentJob     *string `json:"current_job_id"`
	DemoSlot       *string `json:"demo_slot"`
	CrashRequested bool    `json:"crash_requested"`

	ID         string    `json:"id"`
	State      string    `json:"state"`
	StartedAt  time.Time `json:"started_at"`
	ReportedAt time.Time `json:"reported_at"`
}

type Metrics struct {
	Total         int64   `json:"total"`
	Queued        int64   `json:"queued"`
	Running       int64   `json:"running"`
	Completed     int64   `json:"completed"`
	Failed        int64   `json:"failed"`
	ActiveWorkers int64   `json:"active_workers"`
	AverageRunMS  float64 `json:"average_run_ms"`
	AverageWaitMS float64 `json:"average_wait_ms"`
}

type Event struct {
	JobID    string    `json:"job_id"`
	Status   Status    `json:"status"`
	At       time.Time `json:"at"`
	WorkerID *string   `json:"worker_id"`
}

type sleepPayload struct {
	DurationMS int `json:"duration_ms"`
}
type computePayload struct {
	Limit int `json:"limit"`
}
type transformPayload struct {
	Text *string `json:"text"`
}

func Decode(r io.Reader, target any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("expected a single JSON value")
	}
	return nil
}

func parse(input Input) (any, error) {
	payload := bytes.TrimSpace(input.Payload)
	if len(payload) == 0 || len(payload) > 8192 || payload[0] != '{' {
		return nil, errors.New("payload must be a JSON object of at most 8192 bytes")
	}
	reader := bytes.NewReader(payload)
	switch input.Type {
	case "sleep":
		var p sleepPayload
		if Decode(reader, &p) != nil || p.DurationMS < 1 || p.DurationMS > 10000 {
			return nil, errors.New("sleep requires duration_ms between 1 and 10000")
		}
		return p, nil
	case "compute":
		var p computePayload
		if Decode(reader, &p) != nil || p.Limit < 2 || p.Limit > 2000000 {
			return nil, errors.New("compute requires limit between 2 and 2000000")
		}
		return p, nil
	case "transform":
		var p transformPayload
		if Decode(reader, &p) != nil || p.Text == nil || len(*p.Text) > 4096 || strings.ContainsRune(*p.Text, 0) {
			return nil, errors.New("transform requires text of at most 4096 bytes without NUL characters")
		}
		return p, nil
	default:
		return nil, errors.New("type must be sleep, compute, or transform")
	}
}

// Normalize stores the same typed payload that validation and execution interpret.
func Normalize(input Input) (Input, error) {
	payload, err := parse(input)
	if err != nil {
		return Input{}, err
	}
	input.Payload, err = json.Marshal(payload)
	return input, err
}

func Execute(ctx context.Context, input Input) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := parse(input)
	if err != nil {
		return nil, err
	}
	switch p := payload.(type) {
	case sleepPayload:
		timer := time.NewTimer(time.Duration(p.DurationMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return json.Marshal(map[string]int{"slept_ms": p.DurationMS})
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case computePayload:
		// A bounded sieve counts primes up to and including limit.
		composite := make([]bool, p.Limit+1)
		root := int(math.Sqrt(float64(p.Limit)))
		count := 0
		for i := 2; i <= p.Limit; i++ {
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if composite[i] {
				continue
			}
			count++
			if i <= root {
				for multiple := i * i; multiple <= p.Limit; multiple += i {
					composite[multiple] = true
				}
			}
		}
		return json.Marshal(map[string]int{"prime_count": count})
	case transformPayload:
		return json.Marshal(map[string]string{"text": strings.ToUpper(strings.TrimSpace(*p.Text))})
	default:
		return nil, errors.New("unsupported job payload")
	}
}

func Demo(count int) []Input {
	inputs := make([]Input, count)
	for i := range inputs {
		switch i % 5 {
		case 3:
			inputs[i] = Input{"compute", json.RawMessage(`{"limit":1200000}`)}
		case 4:
			inputs[i] = Input{"transform", json.RawMessage(fmt.Sprintf(`{"text":"  queueforge demo %d  "}`, i+1))}
		default:
			inputs[i] = Input{"sleep", json.RawMessage(fmt.Sprintf(`{"duration_ms":%d}`, 650+i%7*100))}
		}
	}
	return inputs
}

func DemoSlot(name string) bool {
	return name == "worker-1" || name == "worker-2" || name == "worker-3"
}
