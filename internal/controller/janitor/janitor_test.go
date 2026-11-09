package janitor

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/ids"
)

type fakeDeleter struct {
	mu    sync.Mutex
	fail  map[string]bool // worker address → fail
	calls []string
}

func (f *fakeDeleter) DeleteChunk(_ context.Context, addr, chunk string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, addr+"/"+chunk)
	if f.fail[addr] {
		return errors.New("boom")
	}
	return nil
}

func setup(t *testing.T) (*meta.Store, *registry.Registry, string) {
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
	reg.BeginRegistration("w3", "http://w3", registry.Capacity{}) // SYNCING: not eligible for deletes

	// An aborted upload with one chunk on w1, w2 and a claim moved off w3.
	ctx := context.Background()
	obj := ids.NewObjectID()
	m.CreateObject(ctx, obj, "f", "x")
	c, _ := ids.ChunkID(obj, 0)
	if err := m.ClaimChunk(ctx, meta.Chunk{ID: c, ObjectID: obj, Idx: 0, Size: 1, ChecksumSHA256: "x"}, []string{"w1", "w3"}); err != nil {
		t.Fatal(err)
	}
	if err := m.ReplaceReplica(ctx, obj, c, "w3", "w2", "put_failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MarkDeleting(ctx, obj, "upload_aborted", meta.StatusUploading); err != nil {
		t.Fatal(err)
	}
	return m, reg, obj
}

func TestRoundDeletesOnlyOnAliveWorkersAndFinalizes(t *testing.T) {
	m, reg, obj := setup(t)
	d := &fakeDeleter{}
	j := New(m, reg, d, Config{Interval: time.Second})
	ctx := context.Background()

	res, err := j.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// w1 and w2 confirmed; w3 is SYNCING, so its entry waits.
	if res.Deleted != 2 || res.Failed != 0 || len(res.Finalized) != 0 {
		t.Fatalf("round 1 = %+v", res)
	}
	if len(d.calls) != 2 {
		t.Fatalf("calls = %v", d.calls)
	}

	// w3 becomes ALIVE; the next round deletes there and finalizes the object.
	w3, _ := reg.Get("w3")
	reg.CompleteRegistration("w3", w3.Epoch)
	res, err = j.RunOnce(ctx)
	if err != nil || res.Deleted != 1 || len(res.Finalized) != 1 || res.Finalized[0] != obj {
		t.Fatalf("round 2 = %+v, %v", res, err)
	}
	if _, err := m.GetObject(ctx, obj); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("object not purged: %v", err)
	}
	// Nothing left: a further round is a no-op.
	if res, _ := j.RunOnce(ctx); res.Deleted != 0 || len(res.Finalized) != 0 {
		t.Fatalf("idle round = %+v", res)
	}
}

func TestFailedDeleteIsRetriedLater(t *testing.T) {
	m, reg, obj := setup(t)
	d := &fakeDeleter{fail: map[string]bool{"http://w2": true}}
	j := New(m, reg, d, Config{Interval: time.Hour}) // backoff ≥ 1h: not due again in this test
	ctx := context.Background()

	res, err := j.RunOnce(ctx)
	if err != nil || res.Deleted != 1 || res.Failed != 1 {
		t.Fatalf("round = %+v, %v", res, err)
	}
	pds, _ := m.PendingDeletesFor(ctx, obj)
	var w2 *meta.PendingDelete
	for i := range pds {
		if pds[i].WorkerID == "w2" {
			w2 = &pds[i]
		}
	}
	if w2 == nil || w2.Attempts != 1 {
		t.Fatalf("w2 entry = %+v", pds)
	}
	// Not due yet: the next round does not call w2 again.
	before := len(d.calls)
	j.RunOnce(ctx)
	for _, c := range d.calls[before:] {
		if c[:len("http://w2")] == "http://w2" {
			t.Fatalf("retried before backoff elapsed: %v", d.calls[before:])
		}
	}
	if o, err := m.GetObject(ctx, obj); err != nil || o.Status != meta.StatusDeleting {
		t.Fatalf("object finalized with a delete outstanding: %+v, %v", o, err)
	}
}

func TestKickTriggersRound(t *testing.T) {
	m, reg, obj := setup(t)
	w3, _ := reg.Get("w3")
	reg.CompleteRegistration("w3", w3.Epoch)
	j := New(m, reg, &fakeDeleter{}, Config{Interval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go j.Run(ctx)
	j.Kick()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := m.GetObject(ctx, obj); errors.Is(err, meta.ErrNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("kick did not trigger a round")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
