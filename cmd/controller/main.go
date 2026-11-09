// Command controller is the single coordinator of the object store. It owns
// worker membership and liveness (Phase 2); later phases add the metadata
// database, the client REST API, and the background repair and cleanup loops.
//
// Usage:
//
//	controller           run (configured by environment variables)
//	controller version   print the version
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"objstore/internal/config"
	"objstore/internal/controller/api"
	"objstore/internal/controller/registry"
	"objstore/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 {
		if args[0] == "version" {
			fmt.Println(version.String("controller"))
			return 0
		}
		fmt.Fprintf(os.Stderr, "unknown command %q (expected: version, or no arguments)\n", args[0])
		return 2
	}
	cfg, err := config.LoadController(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "controller")
	if err := serve(cfg, logger); err != nil {
		logger.Error("controller_exit", "error", err)
		return 1
	}
	return 0
}

func serve(cfg config.Controller, logger *slog.Logger) error {
	reg := registry.New(registry.Options{DeadAfter: cfg.DeadAfter, Logger: logger})
	srv := api.New(reg, api.NoopReconciler{Logger: logger}, api.Config{
		HeartbeatInterval: cfg.HeartbeatInterval,
		DeadAfter:         cfg.DeadAfter,
		RepairGrace:       cfg.RepairGrace,
		Logger:            logger,
	})

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		// No overall Read/WriteTimeout: from Phase 3 this server streams
		// arbitrarily large objects. Per-request limits live in handlers.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var bg sync.WaitGroup
	bgCtx, cancelBg := context.WithCancel(context.Background())
	defer cancelBg()
	bg.Go(func() { registry.RunFailureDetector(bgCtx, reg, cfg.FailureCheckInterval) })

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.Serve(ln) }()
	logger.Info("controller_started", "version", version.Version, "listen_addr", ln.Addr().String(),
		"heartbeat_interval", cfg.HeartbeatInterval, "dead_after", cfg.DeadAfter,
		"repair_grace", cfg.RepairGrace, "failure_check_interval", cfg.FailureCheckInterval)

	var serveErr error
	select {
	case serveErr = <-errc:
	case <-ctx.Done():
		logger.Info("controller_stopping")
	}
	cancelBg()
	bg.Wait()
	if serveErr != nil {
		return fmt.Errorf("http server: %w", serveErr)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info("controller_stopped")
	return nil
}
