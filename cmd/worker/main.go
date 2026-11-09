// Command worker is a storage node: it stores, serves, and deletes chunk files
// on its persistent volume. It makes no decisions about anything outside its
// own disk.
//
// Usage:
//
//	worker                 run the worker (configured by environment variables)
//	worker inspect FILE... print and verify chunk file headers (debug tool)
//	worker version         print the version
//
// Phase 1: the worker runs standalone and accepts writes immediately.
// Registration and heartbeats with the controller arrive in Phase 2.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"objstore/internal/config"
	"objstore/internal/version"
	"objstore/internal/worker/server"
	"objstore/internal/worker/store"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 {
		switch args[0] {
		case "inspect":
			return inspect(args[1:])
		case "version":
			fmt.Println(version.String("worker"))
			return 0
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q (expected: inspect, version, or no arguments)\n", args[0])
			return 2
		}
	}

	cfg, err := config.LoadWorker(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "worker", "worker_id", cfg.WorkerID)

	if err := serve(cfg, logger); err != nil {
		logger.Error("worker_exit", "error", err)
		return 1
	}
	return 0
}

func serve(cfg config.Worker, logger *slog.Logger) error {
	st, err := store.Open(cfg.DataDir, store.Options{
		Fsync:          cfg.FsyncOnWrite,
		MaxPayloadSize: cfg.ChunkSize,
		Logger:         logger,
	})
	if err != nil {
		return err
	}
	srv := server.New(st, server.Config{
		WorkerID:           cfg.WorkerID,
		MaxChunkSize:       cfg.ChunkSize,
		MaxConcurrentReads: cfg.MaxConcurrentReads,
		Logger:             logger,
	})
	srv.SetAcceptingWrites(true) // standalone mode (Phase 1); see server.Server

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// A chunk is at most CHUNK_SIZE; these bound a stalled peer, not a
		// legitimate transfer.
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 2 * time.Minute,
		IdleTimeout:  2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.Serve(ln) }()
	logger.Info("worker_started", "version", version.Version, "listen_addr", ln.Addr().String(),
		"data_dir", cfg.DataDir, "chunk_size", cfg.ChunkSize, "fsync", cfg.FsyncOnWrite)

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	logger.Info("worker_stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info("worker_stopped")
	return nil
}

// inspect prints each chunk file's header and verifies the payload against
// it. Exit status is 0 if every file is valid, 1 otherwise.
func inspect(paths []string) int {
	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "usage: worker inspect FILE...")
		return 2
	}
	status := 0
	for _, p := range paths {
		rep, err := store.Inspect(p)
		if err != nil {
			fmt.Printf("%s: ERROR %v\n", p, err)
			status = 1
			continue
		}
		fmt.Printf("file:             %s\n", rep.Path)
		fmt.Printf("file_size:        %d\n", rep.FileSize)
		if rep.HeaderErr != nil {
			fmt.Printf("header:           INVALID (%v)\n", rep.HeaderErr)
		} else {
			fmt.Printf("header_version:   %d\n", rep.Header.Version)
			fmt.Printf("header_size:      %d\n", rep.Header.PayloadSize)
			fmt.Printf("header_sha256:    %s\n", hex.EncodeToString(rep.Header.SHA256[:]))
			fmt.Printf("payload_size:     %d\n", rep.PayloadSize)
			fmt.Printf("payload_sha256:   %s\n", hex.EncodeToString(rep.PayloadSHA256[:]))
		}
		if rep.Valid {
			fmt.Println("status:           VALID")
		} else {
			fmt.Printf("status:           CORRUPT (%s)\n", rep.Problem)
			status = 1
		}
		fmt.Println()
	}
	return status
}
