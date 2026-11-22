package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/workerclient"
	"objstore/internal/ids"
	"objstore/internal/wire"
)

var ctx = context.Background()

// fakeWorkers serves GetChunk from a map keyed by "addr/chunk".
type fakeWorkers struct {
	mu    sync.Mutex
	data  map[string]workerclient.Chunk
	errs  map[string]error
	calls int
}

func (f *fakeWorkers) GetChunk(_ context.Context, addr, chunk string, _ int64) (workerclient.Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	k := addr + "/" + chunk
	if err := f.errs[k]; err != nil {
		return workerclient.Chunk{}, err
	}
	return f.data[k], nil
}

type fixture struct {
	m     *meta.Store
	reg   *registry.Registry
	w     *fakeWorkers
	v     *Verifier
	chunk string
	data  []byte
	sum   string
}

func setup(t *testing.T) *fixture {
	t.Helper()
	m, err := meta.Open(filepath.Join(t.TempDir(), "m.db"), meta.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	reg := registry.New(registry.Options{DeadAfter: time.Hour})
	for _, w := range []string{"w1", "w2"} {
		reg.CompleteRegistration(w, reg.BeginRegistration(w, "http://"+w, registry.Capacity{}))
	}
	data := []byte("chunk payload")
	s := sha256.Sum256(data)
	sum := hex.EncodeToString(s[:])
	obj := ids.NewObjectID()
	c, _ := ids.ChunkID(obj, 0)
	if _, err := m.CreateObject(ctx, obj, "f", "x"); err != nil {
		t.Fatal(err)
	}
	if err := m.ClaimChunk(ctx, meta.Chunk{ID: c, ObjectID: obj, Idx: 0, Size: int64(len(data)), ChecksumSHA256: sum}, []string{"w1", "w2"}); err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"w1", "w2"} {
		if err := m.MarkReplicaOK(ctx, obj, c, w); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.CommitObject(ctx, meta.CommitInput{ObjectID: obj, Size: int64(len(data)), NumChunks: 1,
		ChecksumSHA256: sum, ReplicationFactor: 2}); err != nil {
		t.Fatal(err)
	}
	w := &fakeWorkers{data: map[string]workerclient.Chunk{}, errs: map[string]error{}}
	for _, id := range []string{"w1", "w2"} {
		w.data["http://"+id+"/"+c] = workerclient.Chunk{Data: data, StoredSHA256: sum}
	}
	return &fixture{m: m, reg: reg, w: w, v: New(m, reg, w, Config{}), chunk: c, data: data, sum: sum}
}

func (f *fixture) state(t *testing.T, worker string) string {
	t.Helper()
	cm, err := f.m.ChunkMap(ctx, f.chunk[:36])
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range cm[0].Replicas {
		if r.WorkerID == worker {
			return r.State
		}
	}
	return ""
}

func TestVerifyHealthyReportIsNotTrusted(t *testing.T) {
	f := setup(t)
	// The worker claims the chunk is bad, but it matches the controller's
	// checksum: the report is not confirmed and nothing changes.
	v, err := f.v.Verify(ctx, "w1", f.chunk)
	if err != nil || v != Healthy {
		t.Fatalf("Verify = %v, %v", v, err)
	}
	if got := f.state(t, "w1"); got != meta.ReplicaOK {
		t.Fatalf("state = %s", got)
	}
}

func TestVerifyConfirmsMismatches(t *testing.T) {
	cases := map[string]func(f *fixture){
		"checksum": func(f *fixture) {
			bad := append([]byte(nil), f.data...)
			bad[0] ^= 0xFF
			f.w.data["http://w1/"+f.chunk] = workerclient.Chunk{Data: bad, StoredSHA256: f.sum}
		},
		"size": func(f *fixture) {
			f.w.data["http://w1/"+f.chunk] = workerclient.Chunk{Data: f.data[:3], StoredSHA256: f.sum}
		},
		"header checksum": func(f *fixture) {
			f.w.data["http://w1/"+f.chunk] = workerclient.Chunk{Data: f.data, StoredSHA256: "00"}
		},
		"missing file": func(f *fixture) {
			f.w.errs["http://w1/"+f.chunk] = &workerclient.Error{Op: "get", Status: http.StatusNotFound, Code: wire.CodeChunkNotFound}
		},
		"unparseable header": func(f *fixture) {
			f.w.errs["http://w1/"+f.chunk] = &workerclient.Error{Op: "get", Status: http.StatusInternalServerError, Code: wire.CodeChunkCorrupt}
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			damage(f)
			v, err := f.v.Verify(ctx, "w1", f.chunk)
			if err != nil || v != Confirmed {
				t.Fatalf("Verify = %v, %v", v, err)
			}
			if got := f.state(t, "w1"); got != meta.ReplicaCorrupt {
				t.Fatalf("w1 state = %s", got)
			}
			if got := f.state(t, "w2"); got != meta.ReplicaOK {
				t.Fatalf("w2 state = %s", got)
			}
		})
	}
}

func TestVerifyInconclusiveAndSkipped(t *testing.T) {
	f := setup(t)
	f.w.errs["http://w1/"+f.chunk] = &workerclient.Error{Op: "get", Status: http.StatusServiceUnavailable}
	if v, _ := f.v.Verify(ctx, "w1", f.chunk); v != Inconclusive {
		t.Fatalf("5xx verdict = %v", v)
	}
	f.w.errs["http://w1/"+f.chunk] = errors.New("connection refused")
	if v, _ := f.v.Verify(ctx, "w1", f.chunk); v != Inconclusive {
		t.Fatalf("network error verdict = %v", v)
	}
	if got := f.state(t, "w1"); got != meta.ReplicaOK {
		t.Fatalf("state after inconclusive checks = %s", got)
	}
	if v, _ := f.v.Verify(ctx, "w9", f.chunk); v != Skipped {
		t.Fatalf("unknown replica verdict = %v", v)
	}
}

func TestRunProcessesReportsOnce(t *testing.T) {
	f := setup(t)
	bad := append([]byte(nil), f.data...)
	bad[1] ^= 0xFF
	f.w.data["http://w2/"+f.chunk] = workerclient.Chunk{Data: bad, StoredSHA256: f.sum}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { f.v.Run(rctx); close(done) }()
	defer func() { cancel(); <-done }()

	f.v.Report("w2", []string{f.chunk, f.chunk})
	deadline := time.Now().Add(5 * time.Second)
	for f.state(t, "w2") != meta.ReplicaCorrupt {
		if time.Now().After(deadline) {
			t.Fatal("report was not processed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.w.mu.Lock()
	calls := f.w.calls
	f.w.mu.Unlock()
	if calls != 1 {
		t.Fatalf("worker reads = %d, want 1 (duplicate report deduplicated)", calls)
	}
}
