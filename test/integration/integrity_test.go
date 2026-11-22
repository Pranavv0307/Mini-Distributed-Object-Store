package integration

// Phase 8 (data integrity) and Phase 9 (crash consistency, janitor) at the
// cluster level: scrubber → heartbeat → controller cross-check → repair; the
// live inventory sweep (lost files, orphans); controller crash recovery; and
// audit v2 (I4) under concurrent writes.

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/ids"
	"objstore/internal/worker/store"
)

// heartbeatsFrom waits until the controller has received n more heartbeats
// from workerID (observable synchronisation instead of a sleep).
func (c *uploadCluster) heartbeatsFrom(workerID string, n int) {
	c.t.Helper()
	for range n {
		w, _ := c.reg.Get(workerID)
		last := w.LastHeartbeat
		waitFor(c.t, clusterLimit, "a heartbeat from "+workerID, func() bool {
			w, _ := c.reg.Get(workerID)
			return w.LastHeartbeat.After(last)
		})
	}
}

func (c *uploadCluster) i4() meta.AuditCheck {
	c.t.Helper()
	for _, ch := range c.audit().Checks {
		if ch.ID == "I4" {
			return ch
		}
	}
	c.t.Fatal("audit has no I4 check")
	return meta.AuditCheck{}
}

// TestScrubberDetectsAndRepairsWithoutClientRead is the M8 checkpoint: the
// scrubber finds damage no client has read; the controller confirms it
// against its own checksum, marks the copy CORRUPT, and repair rewrites it
// in place. Afterwards the stale suspect is not reported again, so the copy
// is repaired exactly once (regression: it used to be re-marked CORRUPT on
// every heartbeat until the next full scrub pass).
func TestScrubberDetectsAndRepairsWithoutClientRead(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	data := randomBytes(t, 5000)
	id := c.mustUpload(data)
	ch := c.chunkMap(id)[1]
	bad := ch.Replicas[0].WorkerID
	corruptFile(t, c.chunkPath(bad, ch.ID)) // same size: only a checksum finds it

	if err := c.byID[bad].scrub.ScrubOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, clusterLimit, "scrubber finding repaired", func() bool { return c.repairStats().Completed >= 1 })
	waitFor(t, clusterLimit, "steady after repair", c.steady)
	if st := c.replicasOn(bad, id)[ch.ID]; st != meta.ReplicaOK {
		t.Fatalf("replica on %s = %q, want OK", bad, st)
	}
	got, err := c.byID[bad].store.Get(ch.ID)
	if err != nil || sha(got.Payload) != ch.ChecksumSHA256 {
		t.Fatalf("file on %s not rewritten: %v", bad, err)
	}

	// No churn: several more heartbeats from that worker (each would carry
	// a stale suspect) and two repair rounds change nothing.
	completed := c.repairStats().Completed
	c.heartbeatsFrom(bad, 3)
	rounds := c.repairStats().Rounds
	waitFor(t, clusterLimit, "two more repair rounds", func() bool { return c.repairStats().Rounds >= rounds+2 })
	if now := c.repairStats().Completed; now != completed {
		t.Fatalf("repairs went from %d to %d after the fix: the stale suspect is still being re-marked", completed, now)
	}
	if st := c.replicasOn(bad, id)[ch.ID]; st != meta.ReplicaOK {
		t.Fatalf("replica on %s = %q after more heartbeats, want OK", bad, st)
	}
	c.wantDownload(id, data)
}

// TestScrubberReportIsCrossChecked: a worker that reports a healthy chunk as
// suspect (e.g. a bug, or a stale report) does not get it marked CORRUPT:
// the controller's checksum is the authority (§3.6).
func TestScrubberReportIsCrossChecked(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	id := c.mustUpload(randomBytes(t, 3000))
	ch := c.chunkMap(id)[0]
	w := ch.Replicas[0].WorkerID
	c.verifier().Report(w, []string{ch.ID})
	// The report is processed asynchronously; wait until the worker has
	// been read for it (the cross-check GET), then for a couple of rounds.
	waitFor(t, clusterLimit, "cross-check read", func() bool { return c.byID[w].faults.getsSeen.Load() > 0 })
	rounds := c.repairStats().Rounds
	waitFor(t, clusterLimit, "two repair rounds", func() bool { return c.repairStats().Rounds >= rounds+2 })
	if st := c.replicasOn(w, id)[ch.ID]; st != meta.ReplicaOK {
		t.Fatalf("healthy replica reported by %s is %q, want OK", w, st)
	}
	if n := c.repairStats().Completed; n != 0 {
		t.Fatalf("%d repairs for a false report", n)
	}
}

// TestScrubberBothCopiesCorrupt: with both copies damaged, the scrubber
// path marks both CORRUPT, nothing valid remains, and downloads fail
// cleanly with 503 (no wrong bytes); safety invariants still hold.
func TestScrubberBothCopiesCorrupt(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	id := c.mustUpload(randomBytes(t, 3000))
	ch := c.chunkMap(id)[0]
	for _, r := range ch.Replicas {
		corruptFile(t, c.chunkPath(r.WorkerID, ch.ID))
		if err := c.byID[r.WorkerID].scrub.ScrubOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, clusterLimit, "both copies CORRUPT", func() bool {
		for _, r := range c.chunkMap(id)[0].Replicas {
			if r.State != meta.ReplicaCorrupt {
				return false
			}
		}
		return true
	})
	r := c.download(id)
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != "STORAGE_UNAVAILABLE" {
		t.Fatalf("download = %d %+v, want 503 STORAGE_UNAVAILABLE", r.status, r.err)
	}
	if a := c.audit(); !a.OK {
		t.Fatalf("safety invariants violated: %+v", a)
	}
	waitFor(t, clusterLimit, "chunk reported unrecoverable", func() bool { return c.repairStats().Unrecoverable >= 1 })
}

// TestDeletedChunkFileRepairedBySweep is the M8 "deleted file" checkpoint:
// a chunk file removed from a live worker's disk is found by the periodic
// inventory sweep (no client read), and repair rewrites it in place.
func TestDeletedChunkFileRepairedBySweep(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	data := randomBytes(t, 4000)
	id := c.mustUpload(data)
	ch := c.chunkMap(id)[2]
	w := ch.Replicas[1].WorkerID
	if err := os.Remove(c.chunkPath(w, ch.ID)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, clusterLimit, "lost file rewritten", func() bool {
		got, err := c.byID[w].store.Get(ch.ID)
		return err == nil && sha(got.Payload) == ch.ChecksumSHA256
	})
	waitFor(t, clusterLimit, "steady", c.steady)
	if st := c.replicasOn(w, id)[ch.ID]; st != meta.ReplicaOK {
		t.Fatalf("replica on %s = %q, want OK", w, st)
	}
	if n := c.byID[w].faults.getsSeen.Load(); n != 0 {
		t.Fatalf("%d chunk reads from %s: detection should not need a read of that copy", n, w)
	}
	c.wantDownload(id, data)
}

// TestOrphanFileRemovedBySweep: a chunk file that no metadata claims (e.g. a
// PUT that landed after its upload was aborted and cleaned up) is queued as
// a pending delete by the sweep and removed by the janitor.
func TestOrphanFileRemovedBySweep(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	orphan, _ := ids.ChunkID(ids.NewObjectID(), 0)
	w := c.workers[0]
	if code := w.put(orphan, []byte("left behind")); code != http.StatusCreated {
		t.Fatalf("put orphan: %d", code)
	}
	waitFor(t, clusterLimit, "orphan deleted", func() bool {
		_, err := w.store.Get(orphan)
		return errors.Is(err, store.ErrNotFound)
	})
	waitFor(t, clusterLimit, "no pending deletes left", func() bool {
		return c.clusterView().Storage.PendingDeletes == 0
	})
	if ch := c.i4(); ch.Violations != 0 {
		t.Fatalf("I4 after cleanup = %+v", ch)
	}
}

// TestControllerCrashMidUploadRecovered (M9): the state a controller crash
// leaves in the middle of an upload — an UPLOADING object, an acknowledged
// copy, a claim whose write landed but was never acknowledged — is turned
// into deletion work by startup recovery, and the janitor removes every
// file. The object never becomes visible.
func TestControllerCrashMidUploadRecovered(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	ctx := context.Background()
	obj := ids.NewObjectID()
	if _, err := c.meta.CreateObject(ctx, obj, "crash.bin", "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	payload := randomBytes(t, 1000)
	chunkID, _ := ids.ChunkID(obj, 0)
	if err := c.meta.ClaimChunk(ctx, meta.Chunk{ID: chunkID, ObjectID: obj, Idx: 0, Size: 1000, ChecksumSHA256: sha(payload)},
		[]string{"worker1", "worker2"}); err != nil {
		t.Fatal(err)
	}
	for _, w := range c.workers {
		if code := w.put(chunkID, payload); code != http.StatusCreated {
			t.Fatalf("put: %d", code)
		}
	}
	if err := c.meta.MarkReplicaOK(ctx, obj, chunkID, "worker1"); err != nil {
		t.Fatal(err)
	}
	// "Crash" here: worker2's write landed but was never acknowledged.

	if n, err := c.meta.RecoverUploading(ctx); err != nil || n != 1 {
		t.Fatalf("RecoverUploading = %d, %v (want 1 object)", n, err)
	}
	if p, _ := c.meta.PendingDeletesFor(ctx, obj); len(p) != 2 {
		t.Fatalf("pending deletes = %+v, want both copies (acknowledged or not)", p)
	}
	c.jan.Kick()
	waitFor(t, clusterLimit, "object purged", func() bool {
		_, err := c.meta.GetObject(ctx, obj)
		return errors.Is(err, meta.ErrNotFound)
	})
	for _, w := range c.workers {
		if _, err := w.store.Get(chunkID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("file still on %s after recovery: %v", w.id, err)
		}
	}
	if r := c.download(obj); r.status != http.StatusNotFound {
		t.Fatalf("download of a crashed upload = %d, want 404", r.status)
	}
	a := c.audit()
	if !a.OK || !a.SteadyState {
		t.Fatalf("audit after recovery = %+v", a)
	}
}

// TestAuditInventoryDuringWrites: I4 must not report the copies of uploads
// in flight as missing or orphaned (regression: the Compose self-healing
// test saw spurious I4 violations mid-repair).
func TestAuditInventoryDuringWrites(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				data := randomBytes(t, 7000)
				r := c.upload(filePart("f.bin", data))
				if r.status != http.StatusCreated {
					errs <- r.err.Error.Message
					return
				}
				if del := c.call(http.MethodDelete, "/api/v1/objects/"+r.info.ObjectID, nil, nil); del.status != http.StatusNoContent {
					errs <- "delete failed"
					return
				}
			}
		})
	}
	for range 40 {
		a := c.audit()
		if !a.OK {
			t.Errorf("safety violated during writes: %+v", a)
			break
		}
		for _, ch := range a.Checks {
			if ch.ID == "I4" && ch.Violations != 0 {
				t.Errorf("I4 false positive during writes: %+v", ch)
			}
		}
	}
	cancel()
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent write failed: %s", e)
	}
	waitFor(t, clusterLimit, "steady", c.steady)
}

// TestInvariantsUnderChurn (M10): concurrent uploads, PATCHes and deletes
// while a worker dies, stays dead past the grace period, and returns. Safety
// invariants hold throughout; afterwards the cluster converges to steady
// state (I4 and I5) and every surviving object downloads byte-identical.
func TestInvariantsUnderChurn(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000, repair: true})
	var mu sync.Mutex
	kept := map[string][]byte{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for g := range 3 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				data := randomBytes(t, 500+(g*977+i*313)%6000)
				r := c.upload(filePart("f.bin", data))
				if r.status != http.StatusCreated {
					// 503 is acceptable while workers are changing state.
					if r.status != http.StatusServiceUnavailable {
						t.Errorf("upload: %d %+v", r.status, r.err)
						return
					}
					continue
				}
				if _, res := c.patch(r.info.ObjectID, `{"i":"x"}`, ""); res.status != http.StatusOK {
					t.Errorf("patch: %d", res.status)
					return
				}
				if i%3 == 0 {
					if del := c.call(http.MethodDelete, "/api/v1/objects/"+r.info.ObjectID, nil, nil); del.status != http.StatusNoContent {
						t.Errorf("delete: %d", del.status)
						return
					}
					continue
				}
				mu.Lock()
				kept[r.info.ObjectID] = data
				mu.Unlock()
			}
		})
	}
	stopLoad := func() { cancel(); wg.Wait() }
	defer stopLoad() // also on t.Fatal, before the cluster is torn down
	victim := c.workers[2]
	waitFor(t, clusterLimit, "some objects stored", func() bool { mu.Lock(); defer mu.Unlock(); return len(kept) >= 5 })
	victim.stop()
	c.waitWorkerState(victim.id, "DEAD")
	waitFor(t, clusterLimit, "repairs past the grace period", func() bool {
		if a := c.audit(); !a.OK {
			t.Fatalf("safety violated while %s is down: %+v", victim.id, a)
		}
		return c.repairStats().Completed > 0
	})
	victim.start()
	c.waitWorkerState(victim.id, "ALIVE")
	waitFor(t, clusterLimit, "more objects stored after the return", func() bool { mu.Lock(); defer mu.Unlock(); return len(kept) >= 15 })
	stopLoad()

	waitFor(t, 2*clusterLimit, "steady state (I4 and I5)", c.steady)
	mu.Lock()
	defer mu.Unlock()
	for id, data := range kept {
		c.wantDownload(id, data)
	}
	listed, _ := c.listAll(100)
	if len(listed) != len(kept) {
		t.Fatalf("listed %d objects, want %d", len(listed), len(kept))
	}
}
