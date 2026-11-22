package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"objstore/internal/config"
	"objstore/internal/controller/api"
	"objstore/internal/controller/registry"
	"objstore/internal/httpx"
	"objstore/internal/wire"
	"objstore/internal/worker/agent"
	"objstore/internal/worker/scrub"
	"objstore/internal/worker/server"
	"objstore/internal/worker/store"
)

// Fast timings so failure detection takes ~0.3 s instead of ~10 s.
const (
	hbInterval = 50 * time.Millisecond
	// 1 s = 20 heartbeats: robust against scheduling delays when the whole
	// suite runs in parallel (a single unexplained failure was seen in Phase 6
	// with 300 ms; see docs/DEVELOPMENT_LOG.md).
	deadAfter    = time.Second
	checkEvery   = 20 * time.Millisecond
	repairGrace  = time.Second
	clusterLimit = 10 * time.Second // generous deadline for any convergence
)

// recordingReconciler remembers the last inventory each worker registered with.
type recordingReconciler struct {
	mu   sync.Mutex
	last map[string][]wire.ChunkInfo
}

func (r *recordingReconciler) ReconcileInventory(_ context.Context, id string, _ uint64, inv []wire.ChunkInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.last == nil {
		r.last = make(map[string][]wire.ChunkInfo)
	}
	r.last[id] = inv
	return nil
}

func (r *recordingReconciler) inventory(id string) []wire.ChunkInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[id]
}

// testController serves at a stable URL; its handler can be swapped to
// simulate a controller restart (all in-memory state lost) or an outage.
type testController struct {
	t       testing.TB
	ts      *httptest.Server
	handler atomic.Pointer[http.Handler]
	stopBg  context.CancelFunc
	rec     *recordingReconciler
}

func newTestController(t testing.TB) *testController {
	c := &testController{t: t}
	c.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*c.handler.Load()).ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		if c.stopBg != nil {
			c.stopBg()
		}
		c.ts.Close()
	})
	c.start()
	return c
}

// start installs a fresh controller (new registry, new failure detector).
func (c *testController) start() {
	if c.stopBg != nil {
		c.stopBg()
	}
	reg := registry.New(registry.Options{DeadAfter: deadAfter})
	c.rec = &recordingReconciler{}
	srv := api.New(reg, c.rec, api.Config{HeartbeatInterval: hbInterval, DeadAfter: deadAfter, RepairGrace: repairGrace})
	ctx, cancel := context.WithCancel(context.Background())
	c.stopBg = cancel
	go registry.RunFailureDetector(ctx, reg, checkEvery)
	h := srv.Handler()
	c.handler.Store(&h)
}

// down makes every request fail with 503, as if the controller were gone.
func (c *testController) down() {
	if c.stopBg != nil {
		c.stopBg()
		c.stopBg = nil
	}
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInternal, "controller down")
	})
	c.handler.Store(&h)
}

func (c *testController) cluster() wire.ClusterView {
	c.t.Helper()
	resp, err := http.Get(c.ts.URL + "/cluster")
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var v wire.ClusterView
	if resp.StatusCode != http.StatusOK {
		return v // controller down: empty view
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		c.t.Fatal(err)
	}
	return v
}

func (c *testController) worker(id string) (wire.WorkerView, bool) {
	for _, w := range c.cluster().Workers {
		if w.WorkerID == id {
			return w, true
		}
	}
	return wire.WorkerView{}, false
}

func (c *testController) waitState(id, state string) wire.WorkerView {
	c.t.Helper()
	var got wire.WorkerView
	waitFor(c.t, clusterLimit, id+" "+state, func() bool {
		w, ok := c.worker(id)
		got = w
		return ok && w.State == state
	})
	return got
}

// testWorker is an in-process worker: real store, HTTP server, and agent.
type testWorker struct {
	t       testing.TB
	id      string
	dataDir string
	ctrlURL string
	ts      *httptest.Server
	srv     *server.Server
	store   *store.Store
	faults  faults // injected failures (helpers_test.go); zero value = none
	// scrub is the worker's scrubber. Tests run passes explicitly
	// (scrub.ScrubOnce); its suspects go out with every heartbeat.
	scrub  *scrub.Scrubber
	cancel context.CancelFunc
	done   chan struct{}
}

func newTestWorker(t testing.TB, id, ctrlURL string) *testWorker {
	w := &testWorker{t: t, id: id, dataDir: t.TempDir(), ctrlURL: ctrlURL}
	w.start()
	t.Cleanup(w.stop)
	return w
}

func (w *testWorker) start() {
	st, err := store.Open(w.dataDir, store.Options{MaxPayloadSize: config.DefaultChunkSize})
	if err != nil {
		w.t.Fatal(err)
	}
	w.store = st
	w.srv = server.New(st, server.Config{WorkerID: w.id, MaxChunkSize: config.DefaultChunkSize, MaxConcurrentReads: 4})
	w.ts = httptest.NewServer(w.faults.wrap(w.srv.Handler()))
	w.scrub = scrub.New(st, scrub.Options{})
	ag := agent.New(agent.Config{
		WorkerID: w.id, AdvertiseAddr: w.ts.URL, ControllerURL: w.ctrlURL, Suspects: w.scrub.Suspects,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		RegisterTimeout: 2 * time.Second, HeartbeatTimeout: time.Second,
	}, st, w.srv)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel, w.done = cancel, make(chan struct{})
	go func() { defer close(w.done); _ = ag.Run(ctx) }()
}

// stop simulates the worker process dying: heartbeats stop, HTTP goes away.
func (w *testWorker) stop() {
	if w.cancel == nil {
		return
	}
	w.cancel()
	<-w.done
	w.ts.Close()
	w.cancel = nil
}

func (w *testWorker) health() wire.WorkerHealth {
	w.t.Helper()
	resp, err := http.Get(w.ts.URL + "/healthz")
	if err != nil {
		w.t.Fatal(err)
	}
	defer resp.Body.Close()
	var h wire.WorkerHealth
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		w.t.Fatal(err)
	}
	return h
}

func (w *testWorker) put(id string, data []byte) int {
	w.t.Helper()
	sum := sha256.Sum256(data)
	req, _ := http.NewRequest(http.MethodPut, w.ts.URL+"/internal/chunks/"+id, bytes.NewReader(data))
	req.Header.Set(wire.HeaderChunkSHA256, hex.EncodeToString(sum[:]))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func startCluster(t testing.TB, n int) (*testController, []*testWorker) {
	ctrl := newTestController(t)
	workers := make([]*testWorker, n)
	for i := range workers {
		workers[i] = newTestWorker(t, fmt.Sprintf("worker%d", i+1), ctrl.ts.URL)
	}
	for _, w := range workers {
		ctrl.waitState(w.id, "ALIVE")
	}
	return ctrl, workers
}

// TestClusterMembership is the M2 checkpoint in-process: four workers become
// ALIVE; a stopped worker becomes DEAD within DEAD_AFTER (+ check interval)
// while the others stay ALIVE; a restarted worker becomes ALIVE again under
// a new epoch; repair eligibility follows REPAIR_GRACE.
func TestClusterMembership(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-based cluster test")
	}
	ctrl, workers := startCluster(t, 4)
	cv := ctrl.cluster()
	if cv.Summary != (wire.WorkerSummary{Total: 4, Alive: 4}) {
		t.Fatalf("summary = %+v", cv.Summary)
	}
	for _, w := range workers {
		if h := w.health(); !h.Registered || !h.AcceptingWrites {
			t.Fatalf("%s health = %+v", w.id, h)
		}
	}

	w2 := workers[1]
	before, _ := ctrl.worker("worker2")
	stoppedAt := time.Now()
	w2.stop()
	dead := ctrl.waitState("worker2", "DEAD")
	detection := time.Since(stoppedAt)
	t.Logf("worker2 declared DEAD after %v (DEAD_AFTER %v)", detection, deadAfter)
	if detection < deadAfter-hbInterval {
		t.Fatalf("declared dead after %v, earlier than DEAD_AFTER allows", detection)
	}
	if dead.DeadSince == nil || dead.RepairEligible {
		t.Fatalf("dead view = %+v", dead)
	}
	for _, id := range []string{"worker1", "worker3", "worker4"} {
		if w, _ := ctrl.worker(id); w.State != "ALIVE" {
			t.Fatalf("%s is %s while worker2 is down", id, w.State)
		}
	}
	waitFor(t, clusterLimit, "worker2 repair-eligible", func() bool {
		w, _ := ctrl.worker("worker2")
		return w.RepairEligible
	})

	w2.start() // same data dir, new address (like a container restart)
	after := ctrl.waitState("worker2", "ALIVE")
	if after.Epoch <= before.Epoch || after.Address != w2.ts.URL || after.DeadSince != nil {
		t.Fatalf("returned worker view = %+v (before %+v)", after, before)
	}
}

// TestControllerRestartTriggersReregistration: a restarted controller has an
// empty registry; every worker is told to re-register and returns to ALIVE
// under a fresh epoch without being touched.
func TestControllerRestartTriggersReregistration(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-based cluster test")
	}
	ctrl, workers := startCluster(t, 4)
	old := map[string]uint64{}
	for _, w := range ctrl.cluster().Workers {
		old[w.WorkerID] = w.Epoch
	}

	ctrl.down()
	// While the controller is down, workers keep serving.
	if st := workers[0].put("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000", []byte("kept")); st != http.StatusCreated {
		t.Fatalf("PUT while controller down = %d", st)
	}
	ctrl.start()
	for _, w := range workers {
		v := ctrl.waitState(w.id, "ALIVE")
		if v.Epoch <= old[w.id] {
			t.Fatalf("%s epoch %d not newer than %d", w.id, v.Epoch, old[w.id])
		}
	}
	// The re-registration carried the chunk written during the outage.
	inv := ctrl.rec.inventory("worker1")
	if len(inv) != 1 || inv[0].Size != 4 {
		t.Fatalf("worker1 re-registration inventory = %+v", inv)
	}
}

// TestWritesRefusedUntilRegistered: a worker that cannot register refuses
// chunk writes (DEC-006) but still answers reads and health checks.
func TestWritesRefusedUntilRegistered(t *testing.T) {
	ctrl := newTestController(t)
	ctrl.down()
	w := newTestWorker(t, "worker1", ctrl.ts.URL)
	if st := w.put("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000", []byte("x")); st != http.StatusServiceUnavailable {
		t.Fatalf("PUT before registration = %d, want 503", st)
	}
	if h := w.health(); h.Registered || h.AcceptingWrites {
		t.Fatalf("health before registration = %+v", h)
	}
	ctrl.start()
	ctrl.waitState("worker1", "ALIVE")
	waitFor(t, clusterLimit, "worker accepting writes", func() bool { return w.health().AcceptingWrites })
	if st := w.put("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000", []byte("x")); st != http.StatusCreated {
		t.Fatalf("PUT after registration = %d, want 201", st)
	}
}

// TestRestartedWorkerReportsItsChunks: a worker restarted on the same data
// directory registers with the chunks that survived on disk.
func TestRestartedWorkerReportsItsChunks(t *testing.T) {
	ctrl, workers := startCluster(t, 1)
	w := workers[0]
	if st := w.put("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000003", []byte("persisted")); st != http.StatusCreated {
		t.Fatalf("PUT = %d", st)
	}
	w.stop()
	w.start()
	waitFor(t, clusterLimit, "re-registration with inventory", func() bool {
		inv := ctrl.rec.inventory("worker1")
		return len(inv) == 1 && inv[0].ChunkID == "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000003" && inv[0].Size == 9
	})
	ctrl.waitState("worker1", "ALIVE")
}
