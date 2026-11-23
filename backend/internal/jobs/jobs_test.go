package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func input(kind, body string) Input { return Input{kind, json.RawMessage(body)} }

func TestValidation(t *testing.T) {
	for _, job := range []Input{
		input("shell", `{}`),
		input("sleep", `null`),
		input("sleep", `{"duration_ms":10001}`),
		input("sleep", `{"duration_ms":1.5}`),
		input("sleep", `{"duration_ms":1,"extra":1}`),
		input("compute", `{"limit":1}`),
		input("compute", `{"limit":2000001}`),
		input("transform", `{"text":null}`),
		input("transform", `{"text":"\u0000"}`),
		input("transform", `{"text":"safe"} {}`),
	} {
		if _, err := Normalize(job); err == nil {
			t.Errorf("accepted %s %s", job.Type, job.Payload)
		}
	}
	for _, job := range Demo(100) {
		if _, err := Normalize(job); err != nil {
			t.Fatalf("invalid demo job: %v", err)
		}
	}
}

func TestExecute(t *testing.T) {
	for _, tc := range []struct {
		job  Input
		want string
	}{
		{input("sleep", `{"duration_ms":1}`), `{"slept_ms":1}`},
		{input("compute", `{"limit":100}`), `{"prime_count":25}`},
		{input("transform", `{"text":"  queueforge  "}`), `{"text":"QUEUEFORGE"}`},
	} {
		result, err := Execute(t.Context(), tc.job)
		if err != nil || string(result) != tc.want {
			t.Errorf("%s: got %s, %v; want %s", tc.job.Type, result, err, tc.want)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Execute(ctx, input("sleep", `{"duration_ms":10000}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
