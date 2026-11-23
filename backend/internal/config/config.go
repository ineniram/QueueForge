package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"queueforge/internal/queue"
	"queueforge/internal/storage"
)

func Env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func Open(ctx context.Context) (*storage.Store, *queue.Queue, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, nil, errors.New("DATABASE_URL must be set")
	}
	store, err := storage.Open(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	pending, err := queue.Open(Env("REDIS_URL", "redis://127.0.0.1:6379/0"), Env("QUEUE_KEY", "queueforge:pending"))
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	return store, pending, nil
}

func Timing() (heartbeat, lease, stale, offline time.Duration, err error) {
	values := []struct {
		key, fallback string
		target        *time.Duration
	}{
		{"HEARTBEAT_INTERVAL", "750ms", &heartbeat}, {"JOB_LEASE", "6s", &lease},
		{"WORKER_STALE_AFTER", "2500ms", &stale}, {"WORKER_OFFLINE_AFTER", "5s", &offline},
	}
	for _, setting := range values {
		value, parseErr := time.ParseDuration(Env(setting.key, setting.fallback))
		if parseErr != nil || value < 100*time.Millisecond || value > time.Minute {
			err = fmt.Errorf("%s must be a duration from 100ms to 1m", setting.key)
			return
		}
		*setting.target = value
	}
	if 3*heartbeat > lease || 2*heartbeat >= stale || stale >= offline || offline >= lease {
		err = errors.New("require 3 × heartbeat <= lease and 2 × heartbeat < stale < offline < lease")
	}
	return
}

func DemoEnabled() bool { return os.Getenv("QUEUEFORGE_DEMO") == "1" }
