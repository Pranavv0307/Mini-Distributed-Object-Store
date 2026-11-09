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
	"net"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"objstore/internal/ids"
)

// DefaultChunkSize is the single definition of the chunk size: 1 MB =
// 1,000,000 bytes, as the specification states (DEC-005, confirmed by the owner).
const DefaultChunkSize int64 = 1_000_000

// DefaultMaxObjectSize is MAX_OBJECT_SIZE's default: 5 GiB (DEC-016).
const DefaultMaxObjectSize int64 = 5 << 30

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
	// ControllerURL (CONTROLLER_URL) is the controller's base URL. Empty
	// means standalone mode: no registration, writes accepted immediately
	// (DEC-026). Compose always sets it.
	ControllerURL string
	// AdvertiseAddr (ADVERTISE_ADDR) is the base URL the controller uses to
	// reach this worker. Default: http://<WORKER_ID>:<LISTEN_ADDR port>, which
	// is the worker's DNS name on the Compose network.
	AdvertiseAddr string
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
		ControllerURL:      l.url("CONTROLLER_URL", ""),
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
	defaultAdvertise := ""
	if _, port, err := net.SplitHostPort(c.ListenAddr); err != nil || port == "" || port == "0" {
		l.fail("LISTEN_ADDR", "must be host:port with a fixed port, got %q", c.ListenAddr)
	} else if c.WorkerID != "" {
		defaultAdvertise = "http://" + net.JoinHostPort(c.WorkerID, port)
	}
	c.AdvertiseAddr = l.url("ADVERTISE_ADDR", defaultAdvertise)
	return c, l.err()
}

// Controller is the configuration of the controller process.
type Controller struct {
	ListenAddr string     // LISTEN_ADDR
	LogLevel   slog.Level // LOG_LEVEL
	// HeartbeatInterval (HEARTBEAT_INTERVAL) is sent to workers at
	// registration; the controller is the single source of this setting.
	HeartbeatInterval time.Duration
	// DeadAfter (DEAD_AFTER): a worker silent for longer is DEAD (D11).
	DeadAfter time.Duration
	// RepairGrace (REPAIR_GRACE): a worker DEAD for longer is
	// repair-eligible (D12). Reported in /cluster; used by repair (Phase 7).
	RepairGrace time.Duration
	// FailureCheckInterval (FAILURE_CHECK_INTERVAL): how often the failure
	// detector evaluates heartbeat ages. Detection latency is at most
	// DeadAfter + FailureCheckInterval after the last heartbeat.
	FailureCheckInterval time.Duration

	// ---- Metadata and write path (Phase 3) ----

	DBPath            string // DB_PATH: SQLite file on the controller volume
	ChunkSize         int64  // CHUNK_SIZE (must not exceed the workers' CHUNK_SIZE)
	ReplicationFactor int    // REPLICATION_FACTOR
	UploadWindow      int    // UPLOAD_WINDOW: chunks in flight per upload (1 = sequential)
	MaxObjectSize     int64  // MAX_OBJECT_SIZE
	// MaxConcurrentUploads (MAX_CONCURRENT_UPLOADS): further uploads get 503.
	MaxConcurrentUploads int
	// MaxConcurrentDownloads (MAX_CONCURRENT_DOWNLOADS): further downloads get 503.
	MaxConcurrentDownloads int
	// MinFreeBytes (PLACEMENT_MIN_FREE_BYTES): workers reporting less free
	// space are not chosen for new chunks (placement rule 3).
	MinFreeBytes int64
	// WorkerConnectTimeout / WorkerRequestTimeout bound every worker call.
	WorkerConnectTimeout time.Duration
	WorkerRequestTimeout time.Duration
	// ClientReadTimeout (CLIENT_READ_TIMEOUT) bounds one stalled read of an
	// upload body.
	ClientReadTimeout time.Duration
	// JanitorInterval (JANITOR_INTERVAL): cleanup round period.
	JanitorInterval time.Duration
	// ReconcilerInterval (RECONCILER_INTERVAL): repair round period.
	ReconcilerInterval time.Duration
	// MaxConcurrentRepairs (MAX_CONCURRENT_REPAIRS): copies in flight at once.
	MaxConcurrentRepairs int
}

// LoadController reads controller configuration through getenv.
func LoadController(getenv func(string) string) (Controller, error) {
	l := &loader{getenv: getenv}
	c := Controller{
		ListenAddr:             l.str("LISTEN_ADDR", ":8080"),
		LogLevel:               l.level("LOG_LEVEL", slog.LevelInfo),
		HeartbeatInterval:      l.duration("HEARTBEAT_INTERVAL", 3*time.Second, 10*time.Millisecond, time.Hour),
		DeadAfter:              l.duration("DEAD_AFTER", 10*time.Second, 10*time.Millisecond, time.Hour),
		RepairGrace:            l.duration("REPAIR_GRACE", 30*time.Second, 0, 24*time.Hour),
		FailureCheckInterval:   l.duration("FAILURE_CHECK_INTERVAL", time.Second, 5*time.Millisecond, time.Minute),
		DBPath:                 l.str("DB_PATH", "/data/meta.db"),
		ChunkSize:              l.int64("CHUNK_SIZE", DefaultChunkSize, 1, MaxConfigurableChunkSize),
		ReplicationFactor:      int(l.int64("REPLICATION_FACTOR", 2, 1, 5)),
		UploadWindow:           int(l.int64("UPLOAD_WINDOW", 4, 1, 64)),
		MaxObjectSize:          l.int64("MAX_OBJECT_SIZE", DefaultMaxObjectSize, 0, 1<<40),
		MaxConcurrentUploads:   int(l.int64("MAX_CONCURRENT_UPLOADS", 16, 1, 1024)),
		MaxConcurrentDownloads: int(l.int64("MAX_CONCURRENT_DOWNLOADS", 32, 1, 4096)),
		MinFreeBytes:           l.int64("PLACEMENT_MIN_FREE_BYTES", 100_000_000, 0, 1<<50),
		WorkerConnectTimeout:   l.duration("WORKER_CONNECT_TIMEOUT", 2*time.Second, time.Millisecond, time.Minute),
		WorkerRequestTimeout:   l.duration("WORKER_REQUEST_TIMEOUT", 10*time.Second, time.Millisecond, 10*time.Minute),
		ClientReadTimeout:      l.duration("CLIENT_READ_TIMEOUT", time.Minute, 10*time.Millisecond, time.Hour),
		JanitorInterval:        l.duration("JANITOR_INTERVAL", 10*time.Second, 5*time.Millisecond, time.Hour),
		ReconcilerInterval:     l.duration("RECONCILER_INTERVAL", 5*time.Second, 5*time.Millisecond, time.Hour),
		MaxConcurrentRepairs:   int(l.int64("MAX_CONCURRENT_REPAIRS", 4, 1, 256)),
	}
	if c.DBPath == "" {
		l.fail("DB_PATH", "must not be empty")
	}
	if c.DeadAfter <= c.HeartbeatInterval {
		l.fail("DEAD_AFTER", "(%v) must be greater than HEARTBEAT_INTERVAL (%v), or every worker would flap", c.DeadAfter, c.HeartbeatInterval)
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

func (l *loader) duration(key string, def, lo, hi time.Duration) time.Duration {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(key, "must be a duration like 500ms or 10s, got %q", v)
		return def
	}
	if d < lo || d > hi {
		l.fail(key, "must be in [%v, %v], got %v", lo, hi, d)
		return def
	}
	return d
}

// url reads an absolute http(s) base URL and strips any trailing slash.
func (l *loader) url(key, def string) string {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	u, err := neturl.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		l.fail(key, "must be an absolute http(s) URL, got %q", v)
		return def
	}
	return strings.TrimRight(v, "/")
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
