package config

import (
	"log/slog"
	"strings"
	"testing"
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
		ChunkSize: 1 << 20, FsyncOnWrite: true, MaxConcurrentReads: 16, LogLevel: slog.LevelInfo,
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
