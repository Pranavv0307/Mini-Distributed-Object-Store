// Command controller is the single coordinator of the object store. It owns
// worker membership and liveness (Phase 2), the metadata database and the
// upload path with its cleanup janitor (Phase 3), and the verified download
// path (Phase 4); later phases add the rest of the client REST API and the
// repair loops.
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
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"objstore/internal/config"
	"objstore/internal/controller/api"
	"objstore/internal/controller/download"
	"objstore/internal/controller/janitor"
	"objstore/internal/controller/meta"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/upload"
	"objstore/internal/controller/workerclient"
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
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}
	store, err := meta.Open(cfg.DBPath, meta.Options{})
	if err != nil {
		return err
	}
	defer store.Close()

	reg := registry.New(registry.Options{DeadAfter: cfg.DeadAfter, Logger: logger})
	workers := workerclient.New(cfg.WorkerConnectTimeout, cfg.WorkerRequestTimeout)
	jan := janitor.New(store, reg, workers, janitor.Config{Interval: cfg.JanitorInterval, Logger: logger})
	uploader := upload.New(store, reg, workers, placement.Random{}, upload.Config{
		ChunkSize:         cfg.ChunkSize,
		ReplicationFactor: cfg.ReplicationFactor,
		Window:            cfg.UploadWindow,
		MaxObjectSize:     cfg.MaxObjectSize,
		MinFreeBytes:      cfg.MinFreeBytes,
		ClientReadTimeout: cfg.ClientReadTimeout,
		Logger:            logger,
	}, jan.Kick)
	downloader := download.New(store, reg, workers, logger)
	srv := api.New(reg, api.NoopReconciler{Logger: logger}, api.Config{
		HeartbeatInterval:      cfg.HeartbeatInterval,
		DeadAfter:              cfg.DeadAfter,
		RepairGrace:            cfg.RepairGrace,
		Meta:                   store,
		Uploader:               uploader,
		MaxConcurrentUploads:   cfg.MaxConcurrentUploads,
		Downloader:             downloader,
		OnDelete:               jan.Kick,
		MaxConcurrentDownloads: cfg.MaxConcurrentDownloads,
		Logger:                 logger,
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
	bg.Go(func() { jan.Run(bgCtx) })

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.Serve(ln) }()
	logger.Info("controller_started", "version", version.Version, "listen_addr", ln.Addr().String(),
		"db_path", cfg.DBPath, "chunk_size", cfg.ChunkSize, "replication_factor", cfg.ReplicationFactor,
		"upload_window", cfg.UploadWindow, "heartbeat_interval", cfg.HeartbeatInterval,
		"dead_after", cfg.DeadAfter, "repair_grace", cfg.RepairGrace,
		"failure_check_interval", cfg.FailureCheckInterval, "janitor_interval", cfg.JanitorInterval)

	var serveErr error
	select {
	case serveErr = <-errc:
	case <-ctx.Done():
		logger.Info("controller_stopping")
	}
	if serveErr != nil {
		cancelBg()
		bg.Wait()
		return fmt.Errorf("http server: %w", serveErr)
	}
	// Stop accepting requests and let in-flight ones finish (an upload that
	// does not finish in time is left UPLOADING; Phase 9 recovery aborts it),
	// then stop the background loops, then close the database (deferred).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := httpSrv.Shutdown(shutdownCtx)
	cancelBg()
	bg.Wait()
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info("controller_stopped")
	return nil
}
