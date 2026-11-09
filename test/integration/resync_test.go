package integration

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"objstore/internal/controller/api"
	"objstore/internal/controller/meta"
	"objstore/internal/wire"
)

// gatedReconciler wraps the real reconciler; while a test holds the gate, a
// registration stays in reconciliation, so the worker stays SYNCING.
type gatedReconciler struct {
	inner api.InventoryReconciler
	mu    sync.Mutex
	gate  chan struct{} // nil = open
	held  chan struct{} // closed when a registration is waiting at the gate
}

func (g *gatedReconciler) ReconcileInventory(ctx context.Context, workerID string, epoch uint64, inv []wire.ChunkInfo) error {
	g.mu.Lock()
	gate, held := g.gate, g.held
	g.mu.Unlock()
	if gate != nil {
		select {
		case <-held:
		default:
			close(held)
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.inner.ReconcileInventory(ctx, workerID, epoch, inv)
}

// hold closes the gate; the returned channel is closed once a registration
// is waiting at it.
func (g *gatedReconciler) hold() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gate, g.held = make(chan struct{}), make(chan struct{})
	return g.held
}

func (g *gatedReconciler) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
}

func (c *uploadCluster) clusterView() wire.ClusterView {
	c.t.Helper()
	r := c.call(http.MethodGet, "/cluster", nil, nil)
	var v wire.ClusterView
	r.decode(c.t, &v)
	if v.Storage == nil {
		c.t.Fatal("/cluster has no storage section")
	}
	return v
}

func (c *uploadCluster) audit() meta.AuditReport {
	c.t.Helper()
	r := c.call(http.MethodGet, "/admin/audit", nil, nil)
	if r.status != 200 {
		c.t.Fatalf("audit: %d", r.status)
	}
	var rep meta.AuditReport
	r.decode(c.t, &rep)
	return rep
}

func (c *uploadCluster) waitWorkerState(id, state string) {
	c.t.Helper()
	waitFor(c.t, clusterLimit, id+" "+state, func() bool {
		w, ok := c.reg.Get(id)
		return ok && string(w.State) == state
	})
}

// restart stops and starts a worker and waits until it has re-registered
// (ALIVE under a newer epoch, i.e. reconciliation has run). mutate, if not
// nil, runs while the worker is stopped (e.g. to wipe or damage its volume).
func (c *uploadCluster) restart(id string, mutate func(w *testWorker)) {
	c.t.Helper()
	before, _ := c.reg.Get(id)
	w := c.byID[id]
	w.stop()
	if mutate != nil {
		mutate(w)
	}
	w.start()
	c.waitReregistered(id, before.Epoch)
}

func (c *uploadCluster) waitReregistered(id string, oldEpoch uint64) {
	c.t.Helper()
	waitFor(c.t, clusterLimit, id+" re-registered", func() bool {
		w, ok := c.reg.Get(id)
		return ok && string(w.State) == "ALIVE" && w.Epoch > oldEpoch
	})
}

// replicasOn returns the chunk IDs (of the given objects) with a replica row
// on workerID.
func (c *uploadCluster) replicasOn(workerID string, objectIDs ...string) map[string]string {
	c.t.Helper()
	out := map[string]string{}
	for _, id := range objectIDs {
		for _, ch := range c.chunkMap(id) {
			for _, r := range ch.Replicas {
				if r.WorkerID == workerID {
					out[ch.ID] = r.State
				}
			}
		}
	}
	return out
}

func (w *testWorker) files() []string {
	inv, err := w.store.Inventory()
	if err != nil {
		w.t.Fatal(err)
	}
	var out []string
	for _, e := range inv {
		out = append(out, e.ChunkID)
	}
	sort.Strings(out)
	return out
}

func (w *testWorker) wipe() {
	w.t.Helper()
	dir := filepath.Join(w.dataDir, "chunks")
	entries, err := os.ReadDir(dir)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			w.t.Fatal(err)
		}
	}
}

// holderWithMostReplicas picks the worker holding the most replicas of the
// given objects (so a failure of it affects many chunks).
func (c *uploadCluster) holderWithMostReplicas(objectIDs ...string) string {
	best, bestN := "", -1
	for _, w := range c.workers {
		if n := len(c.replicasOn(w.id, objectIDs...)); n > bestN {
			best, bestN = w.id, n
		}
	}
	return best
}

// TestDegradedOperationAndReturnWithinGrace is the M6 checkpoint for a
// worker stop and return: uploads and downloads keep working, /cluster shows
// the under-replicated chunks, and on return everything is normal again with
// no data movement.
func TestDegradedOperationAndReturnWithinGrace(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	d1, d2 := randomBytes(t, 9000), randomBytes(t, 6500)
	o1, o2 := c.mustUpload(d1), c.mustUpload(d2)
	v := c.clusterView()
	if s := v.Storage.Chunks; s.Total != 16 || s.FullyReplicated != 16 {
		t.Fatalf("healthy totals = %+v", s)
	}
	if a := c.audit(); !a.OK || !a.SteadyState {
		t.Fatalf("healthy audit = %+v", a)
	}

	x := c.holderWithMostReplicas(o1, o2)
	onX := c.replicasOn(x, o1, o2)
	filesBefore := c.byID[x].files()
	c.byID[x].stop()
	c.waitWorkerState(x, "DEAD")

	// Degraded: reads and writes keep working.
	c.wantDownload(o1, d1)
	c.wantDownload(o2, d2)
	d3 := randomBytes(t, 3000)
	o3 := c.mustUpload(d3)
	if n := len(c.replicasOn(x, o3)); n != 0 {
		t.Fatalf("%d replicas of a new upload placed on DEAD %s", n, x)
	}
	v = c.clusterView()
	if s := v.Storage.Chunks; s.UnderReplicated != len(onX) || s.FullyReplicated != 16-len(onX)+3 || s.ZeroEffective != 0 {
		t.Fatalf("degraded totals = %+v, want %d under-replicated", s, len(onX))
	}
	a := c.audit()
	if !a.OK || a.SteadyState || checkViolations(a, "I5") != len(onX) {
		t.Fatalf("degraded audit: ok=%v steady=%v I5=%d (want %d)", a.OK, a.SteadyState, checkViolations(a, "I5"), len(onX))
	}

	// Return within the grace period: no data movement at all.
	c.byID[x].start()
	c.waitReregistered(x, 0) // it was DEAD, so any registration is new
	if s := c.clusterView().Storage; s.Chunks.FullyReplicated != 19 || s.Chunks.UnderReplicated != 0 || s.PendingDeletes != 0 {
		t.Fatalf("after return: %+v", s)
	}
	if after := c.byID[x].files(); len(after) != len(filesBefore) {
		t.Fatalf("files on %s changed: %d → %d", x, len(filesBefore), len(after))
	}
	if got := c.replicasOn(x, o1, o2); len(got) != len(onX) {
		t.Fatalf("replica rows on %s changed: %v → %v", x, onX, got)
	}
	if a := c.audit(); !a.OK || !a.SteadyState {
		t.Fatalf("audit after return = %+v", a)
	}
}

func checkViolations(r meta.AuditReport, id string) int {
	for _, c := range r.Checks {
		if c.ID == id {
			return c.Violations
		}
	}
	return -1
}

// TestWipedWorkerReturns: a worker returns with an empty volume; its missing
// replicas are detected and dropped, /cluster and the audit show the
// under-replication, and downloads still work from the other copies.
func TestWipedWorkerReturns(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	data := randomBytes(t, 10_000)
	id := c.mustUpload(data)
	x := c.holderWithMostReplicas(id)
	lost := len(c.replicasOn(x, id))
	c.restart(x, (*testWorker).wipe)

	if got := c.replicasOn(x, id); len(got) != 0 {
		t.Fatalf("rows for wiped replicas remain: %v", got)
	}
	if s := c.clusterView().Storage.Chunks; s.UnderReplicated != lost || s.FullyReplicated != 10-lost {
		t.Fatalf("totals = %+v, want %d under-replicated", s, lost)
	}
	a := c.audit()
	if !a.OK || a.SteadyState || checkViolations(a, "I5") != lost {
		t.Fatalf("audit: ok=%v steady=%v I5=%d", a.OK, a.SteadyState, checkViolations(a, "I5"))
	}
	c.wantDownload(id, data)
}

// TestSizeMismatchOnReturn: bytes appended to a chunk file while the worker
// was down are caught by the inventory size check at registration.
func TestSizeMismatchOnReturn(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	data := randomBytes(t, 4000)
	id := c.mustUpload(data)
	ch := c.chunkMap(id)[1]
	x := ch.Replicas[0].WorkerID
	c.restart(x, func(*testWorker) {
		f, err := os.OpenFile(c.chunkPath(x, ch.ID), os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("garbage")
		f.Close()
	})
	if st := c.replicasOn(x, id)[ch.ID]; st != meta.ReplicaCorrupt {
		t.Fatalf("replica state after return = %q, want CORRUPT", st)
	}
	c.wantDownload(id, data)
	if a := c.audit(); !a.OK {
		t.Fatalf("audit = %+v", a)
	}
}

// TestOrphanOnReturn: a chunk file that no metadata claims is queued for
// deletion at registration and removed by the janitor.
func TestOrphanOnReturn(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	keep := c.mustUpload(randomBytes(t, 2500))
	w := c.workers[0]
	// Plant a chunk of an object the controller has never heard of.
	orphan := "11111111-2222-4333-8444-555555555555_000000"
	c.restart(w.id, func(w *testWorker) {
		if err := putDirect(t, w, orphan, []byte("stray data")); err != nil {
			t.Fatal(err)
		}
	})
	waitFor(t, clusterLimit, "orphan removed", func() bool {
		for _, f := range w.files() {
			if f == orphan {
				return false
			}
		}
		return true
	})
	if n := len(c.replicasOn(w.id, keep)); n != 3 {
		t.Fatalf("legitimate replicas on %s: %d, want 3", w.id, n)
	}
	c.waitClean()
	if a := c.audit(); !a.OK || !a.SteadyState {
		t.Fatalf("audit = %+v", a)
	}
}

func putDirect(t *testing.T, w *testWorker, chunkID string, data []byte) error {
	t.Helper()
	_, err := w.store.Put(chunkID, int64(len(data)), sha256sum(data), bytesReader(data))
	return err
}

// TestSyncingWorkerNotUsedDuringReconciliation covers DEC-006 races 1 and 3
// with real metadata: while a returning worker is SYNCING, uploads do not
// place chunks on it and reads do not use it; when reconciliation finishes,
// none of its valid replicas is wrongly dropped.
func TestSyncingWorkerNotUsedDuringReconciliation(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	data := randomBytes(t, 8000)
	id := c.mustUpload(data)
	x := c.holderWithMostReplicas(id)
	before := c.replicasOn(x, id)
	w := c.byID[x]
	w.stop()
	c.waitWorkerState(x, "DEAD")

	held := c.gate.hold()
	w.start()
	<-held // the registration is now blocked inside reconciliation
	c.waitWorkerState(x, "SYNCING")
	gets := w.faults.getsSeen.Load()
	puts := w.faults.putsSeen.Load()
	for range 3 {
		c.wantDownload(id, data)
	}
	newID := c.mustUpload(randomBytes(t, 5000))
	if w.faults.getsSeen.Load() != gets || w.faults.putsSeen.Load() != puts {
		t.Fatalf("SYNCING worker was used: GETs %d→%d, PUTs %d→%d",
			gets, w.faults.getsSeen.Load(), puts, w.faults.putsSeen.Load())
	}
	if n := len(c.replicasOn(x, newID)); n != 0 {
		t.Fatalf("%d replicas placed on a SYNCING worker", n)
	}
	c.gate.release()
	c.waitWorkerState(x, "ALIVE")
	if after := c.replicasOn(x, id); len(after) != len(before) {
		t.Fatalf("reconciliation dropped valid replicas: %v → %v", before, after)
	}
	if a := c.audit(); !a.OK {
		t.Fatalf("audit = %+v", a)
	}
}

// TestWipedWorkerPreflightIs503 covers DEC-006 race 3: a worker returns with
// a wiped volume while the other holder is down. Because reconciliation
// drops its stale rows before it is usable, the download is refused cleanly
// at pre-flight (503) instead of starting and being cut off.
func TestWipedWorkerPreflightIs503(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	id := c.mustUpload(randomBytes(t, 3000)) // every chunk on both workers
	a, b := c.workers[0], c.workers[1]
	b.stop()
	c.waitWorkerState(b.id, "DEAD")
	c.restart(a.id, (*testWorker).wipe)
	r := c.download(id)
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeStorageUnavailable {
		t.Fatalf("got %d (%d bytes) %+v; want a clean 503", r.status, len(r.body), r.err)
	}
	if s := c.clusterView().Storage.Chunks; s.ZeroEffective != 3 {
		t.Fatalf("totals = %+v, want 3 chunks with zero effective replicas", s)
	}
	// The surviving copies on b make the object readable again when b returns.
	b.start()
	c.waitWorkerState(b.id, "ALIVE")
	if r := c.download(id); r.status != 200 {
		t.Fatalf("after b returns: %d", r.status)
	}
}
