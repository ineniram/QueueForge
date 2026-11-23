package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"queueforge/internal/config"
	"queueforge/internal/jobs"
	"queueforge/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	store, pending, err := config.Open(startup)
	if err != nil {
		return err
	}
	cancel()
	defer store.Close()
	defer pending.Close()
	heartbeat, lease, stale, offline, timingErr := config.Timing()
	if timingErr != nil {
		return timingErr
	}
	store.Lease, store.StaleAfter, store.OfflineAfter = lease, stale, offline
	var random [8]byte
	rand.Read(random[:])
	name := config.Env("WORKER_NAME", "worker")
	id := name + "-" + hex.EncodeToString(random[:])
	var slot *string
	if config.DemoEnabled() && jobs.DemoSlot(name) {
		slot = &name
	}
	return (worker.Runner{ID: id, Interval: heartbeat, Slot: slot, Crash: func() { os.Exit(70) }, Store: store, Queue: pending, Log: log.With("worker_id", id)}).Run(ctx)
}
