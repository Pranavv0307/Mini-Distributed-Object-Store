package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadWorkerDefaults(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{"WORKER_ID": "worker1"}))
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}
	want := Worker{
		WorkerID: "worker1", ListenAddr: ":9000", DataDir: "/data",
		ChunkSize: 1_000_000, FsyncOnWrite: true, MaxConcurrentReads: 16, LogLevel: slog.LevelInfo,
		ControllerURL: "", AdvertiseAddr: "http://worker1:9000",
	}
	if c != want {
		t.Fatalf("defaults = %+v, want %+v", c, want)
	}
}

func TestLoadWorkerOverrides(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{
		"WORKER_ID": "worker3", "LISTEN_ADDR": "127.0.0.1:9100", "DATA_DIR": "/tmp/w3",
		"CHUNK_SIZE": "4096", "FSYNC_ON_WRITE": "false", "MAX_CONCURRENT_READS": "4", "LOG_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}
	if c.ListenAddr != "127.0.0.1:9100" || c.DataDir != "/tmp/w3" || c.ChunkSize != 4096 ||
		c.FsyncOnWrite || c.MaxConcurrentReads != 4 || c.LogLevel != slog.LevelDebug {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadWorkerReportsAllErrors(t *testing.T) {
	_, err := LoadWorker(env(map[string]string{
		"WORKER_ID": "Bad/ID", "CHUNK_SIZE": "0", "FSYNC_ON_WRITE": "maybe",
		"MAX_CONCURRENT_READS": "x", "LOG_LEVEL": "loud",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"WORKER_ID", "CHUNK_SIZE", "FSYNC_ON_WRITE", "MAX_CONCURRENT_READS", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestLoadWorkerRequiresID(t *testing.T) {
	_, err := LoadWorker(env(nil))
	if err == nil || !strings.Contains(err.Error(), "WORKER_ID is required") {
		t.Fatalf("err = %v, want WORKER_ID is required", err)
	}
}

func TestLoadWorkerControllerAndAdvertise(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{
		"WORKER_ID": "worker2", "LISTEN_ADDR": ":9100", "CONTROLLER_URL": "http://controller:8080/",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.ControllerURL != "http://controller:8080" || c.AdvertiseAddr != "http://worker2:9100" {
		t.Fatalf("got controller %q advertise %q", c.ControllerURL, c.AdvertiseAddr)
	}
	c, err = LoadWorker(env(map[string]string{"WORKER_ID": "w", "ADVERTISE_ADDR": "http://10.0.0.5:9000"}))
	if err != nil || c.AdvertiseAddr != "http://10.0.0.5:9000" {
		t.Fatalf("explicit ADVERTISE_ADDR: %q, %v", c.AdvertiseAddr, err)
	}
	for _, bad := range []map[string]string{
		{"WORKER_ID": "w", "CONTROLLER_URL": "controller:8080"},
		{"WORKER_ID": "w", "ADVERTISE_ADDR": "ftp://x"},
		{"WORKER_ID": "w", "LISTEN_ADDR": "9000"},
		{"WORKER_ID": "w", "LISTEN_ADDR": "127.0.0.1:0"},
	} {
		if _, err := LoadWorker(env(bad)); err == nil {
			t.Errorf("LoadWorker(%v) succeeded, want error", bad)
		}
	}
}

func TestLoadController(t *testing.T) {
	c, err := LoadController(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Controller{ListenAddr: ":8080", LogLevel: slog.LevelInfo, HeartbeatInterval: 3 * time.Second,
		DeadAfter: 10 * time.Second, RepairGrace: 30 * time.Second, FailureCheckInterval: time.Second,
		DBPath: "/data/meta.db", ChunkSize: 1_000_000, ReplicationFactor: 2, UploadWindow: 4,
		MaxObjectSize: 5 << 30, MaxConcurrentUploads: 16, MinFreeBytes: 100_000_000,
		WorkerConnectTimeout: 2 * time.Second, WorkerRequestTimeout: 10 * time.Second,
		ClientReadTimeout: time.Minute, JanitorInterval: 10 * time.Second}
	if c != want {
		t.Fatalf("defaults = %+v, want %+v", c, want)
	}
	c, err = LoadController(env(map[string]string{"HEARTBEAT_INTERVAL": "100ms", "DEAD_AFTER": "500ms", "REPAIR_GRACE": "0s"}))
	if err != nil || c.HeartbeatInterval != 100*time.Millisecond || c.DeadAfter != 500*time.Millisecond || c.RepairGrace != 0 {
		t.Fatalf("overrides: %+v, %v", c, err)
	}
	if _, err := LoadController(env(map[string]string{"HEARTBEAT_INTERVAL": "10s", "DEAD_AFTER": "5s"})); err == nil ||
		!strings.Contains(err.Error(), "DEAD_AFTER") {
		t.Fatalf("DEAD_AFTER <= HEARTBEAT_INTERVAL accepted: %v", err)
	}
	if _, err := LoadController(env(map[string]string{"DEAD_AFTER": "soon"})); err == nil {
		t.Fatal("bad duration accepted")
	}
	for _, bad := range []map[string]string{
		{"REPLICATION_FACTOR": "0"}, {"UPLOAD_WINDOW": "0"}, {"MAX_CONCURRENT_UPLOADS": "-1"}, {"CHUNK_SIZE": "0"},
	} {
		if _, err := LoadController(env(bad)); err == nil {
			t.Errorf("LoadController(%v) succeeded", bad)
		}
	}
}
