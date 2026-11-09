// Package config loads process configuration from environment variables.
//
// Every variable has a documented default (see .env.example and README.md).
// Loading reports all invalid variables at once rather than failing on the
// first, so a misconfigured container prints one complete error.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"objstore/internal/ids"
)

// DefaultChunkSize is the single definition of the chunk size: 1 MiB
// (DEC-005, provisional pending owner confirmation).
const DefaultChunkSize int64 = 1 << 20

// MaxConfigurableChunkSize bounds CHUNK_SIZE so a typo cannot make a worker
// accept (and buffer) enormous chunk bodies.
const MaxConfigurableChunkSize int64 = 64 << 20

// Worker is the configuration of a worker process.
type Worker struct {
	WorkerID           string     // WORKER_ID (required): stable identity
	ListenAddr         string     // LISTEN_ADDR
	DataDir            string     // DATA_DIR: root of the persistent volume
	ChunkSize          int64      // CHUNK_SIZE: maximum accepted chunk payload
	FsyncOnWrite       bool       // FSYNC_ON_WRITE
	MaxConcurrentReads int        // MAX_CONCURRENT_READS: bounds GET memory
	LogLevel           slog.Level // LOG_LEVEL: debug|info|warn|error
}

// LoadWorker reads worker configuration through getenv (os.Getenv in
// production; a map lookup in tests).
func LoadWorker(getenv func(string) string) (Worker, error) {
	l := &loader{getenv: getenv}
	c := Worker{
		WorkerID:           l.str("WORKER_ID", ""),
		ListenAddr:         l.str("LISTEN_ADDR", ":9000"),
		DataDir:            l.str("DATA_DIR", "/data"),
		ChunkSize:          l.int64("CHUNK_SIZE", DefaultChunkSize, 1, MaxConfigurableChunkSize),
		FsyncOnWrite:       l.bool("FSYNC_ON_WRITE", true),
		MaxConcurrentReads: int(l.int64("MAX_CONCURRENT_READS", 16, 1, 1024)),
		LogLevel:           l.level("LOG_LEVEL", slog.LevelInfo),
	}
	switch {
	case c.WorkerID == "":
		l.fail("WORKER_ID", "is required")
	case !ids.ValidWorkerID(c.WorkerID):
		l.fail("WORKER_ID", "must match [a-z0-9][a-z0-9_-]{0,62}, got %q", c.WorkerID)
	}
	if c.DataDir == "" {
		l.fail("DATA_DIR", "must not be empty")
	}
	return c, l.err()
}

type loader struct {
	getenv func(string) string
	errs   []error
}

func (l *loader) fail(key, format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf("%s %s", key, fmt.Sprintf(format, args...)))
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(l.errs...))
}

func (l *loader) raw(key string) (string, bool) {
	v := strings.TrimSpace(l.getenv(key))
	return v, v != ""
}

func (l *loader) str(key, def string) string {
	if v, ok := l.raw(key); ok {
		return v
	}
	return def
}

func (l *loader) int64(key string, def, lo, hi int64) int64 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		l.fail(key, "must be an integer, got %q", v)
		return def
	}
	if n < lo || n > hi {
		l.fail(key, "must be in [%d, %d], got %d", lo, hi, n)
		return def
	}
	return n
}

func (l *loader) bool(key string, def bool) bool {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, "must be true or false, got %q", v)
		return def
	}
	return b
}

func (l *loader) level(key string, def slog.Level) slog.Level {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(v)); err != nil {
		l.fail(key, "must be debug, info, warn, or error, got %q", v)
		return def
	}
	return lv
}
