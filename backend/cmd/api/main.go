package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"queueforge/internal/api"
	"queueforge/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	store, pending, err := config.Open(startup)
	if err != nil {
		return err
	}
	defer store.Close()
	defer pending.Close()
	_, lease, stale, offline, timingErr := config.Timing()
	if timingErr != nil {
		return timingErr
	}
	store.Lease, store.StaleAfter, store.OfflineAfter = lease, stale, offline
	if err := store.Migrate(startup); err != nil {
		return err
	}
	cancel()
	service := api.Server{Store: store, Queue: pending, Log: log, DemoEnabled: config.DemoEnabled()}
	dispatchCtx, stopDispatch := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Dispatch(dispatchCtx)
	}()
	defer func() { stopDispatch(); <-done }()
	server := &http.Server{
		Addr: config.Env("HTTP_ADDR", ":8080"), Handler: service.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	log.Info("API starting", "address", server.Addr)
	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		shutdown, finish := context.WithTimeout(context.Background(), 16*time.Second)
		defer finish()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-serverErr; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
