package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/wire"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type reconcilerFunc func(ctx context.Context, id string, epoch uint64, inv []wire.ChunkInfo) error

func (f reconcilerFunc) ReconcileInventory(ctx context.Context, id string, epoch uint64, inv []wire.ChunkInfo) error {
	return f(ctx, id, epoch, inv)
}

type fixture struct {
	reg *registry.Registry
	clk *clock
	ts  *httptest.Server
	api *Server
}

func newFixture(t *testing.T, rec InventoryReconciler) *fixture {
	t.Helper()
	clk := &clock{t: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	reg := registry.New(registry.Options{DeadAfter: 10 * time.Second, Now: clk.Now})
	if rec == nil {
		rec = NoopReconciler{}
	}
	a := New(reg, rec, Config{HeartbeatInterval: 3 * time.Second, DeadAfter: 10 * time.Second, RepairGrace: 30 * time.Second})
	ts := httptest.NewServer(a.Handler())
	t.Cleanup(ts.Close)
	return &fixture{reg: reg, clk: clk, ts: ts, api: a}
}

func (f *fixture) post(t *testing.T, path string, body any) *http.Response {
	t.Helper()
	var b []byte
	switch v := body.(type) {
	case string:
		b = []byte(v)
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := http.Post(f.ts.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return v
}

func regReq(id string, inv ...wire.ChunkInfo) wire.RegisterRequest {
	return wire.RegisterRequest{WorkerID: id, Address: "http://" + id + ":9000", FreeBytes: 1000, UsedBytes: 10, Inventory: inv}
}

const chunk = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000"

func TestRegisterHeartbeatCluster(t *testing.T) {
	var gotInv []wire.ChunkInfo
	var stateDuringReconcile registry.State
	var f *fixture
	f = newFixture(t, reconcilerFunc(func(_ context.Context, id string, _ uint64, inv []wire.ChunkInfo) error {
		gotInv = inv
		w, _ := f.reg.Get(id)
		stateDuringReconcile = w.State
		return nil
	}))

	resp := f.post(t, "/internal/workers/register", regReq("worker1", wire.ChunkInfo{ChunkID: chunk, Size: 5}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", resp.StatusCode)
	}
	rr := decode[wire.RegisterResponse](t, resp)
	if rr.HeartbeatIntervalMs != 3000 || rr.Epoch == 0 {
		t.Fatalf("register response = %+v", rr)
	}
	if stateDuringReconcile != registry.Syncing {
		t.Fatalf("state during reconciliation = %s, want SYNCING", stateDuringReconcile)
	}
	if len(gotInv) != 1 || gotInv[0].ChunkID != chunk {
		t.Fatalf("reconciler got inventory %+v", gotInv)
	}

	hb := func(epoch uint64) string {
		resp := f.post(t, "/internal/workers/heartbeat", wire.HeartbeatRequest{WorkerID: "worker1", Epoch: epoch, ChunkCount: 1})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("heartbeat status = %d", resp.StatusCode)
		}
		return decode[wire.HeartbeatResponse](t, resp).Action
	}
	if a := hb(rr.Epoch); a != wire.ActionOK {
		t.Fatalf("heartbeat action = %q", a)
	}
	if a := hb(rr.Epoch - 1); a != wire.ActionReregister {
		t.Fatalf("stale-epoch heartbeat action = %q", a)
	}

	f.clk.Advance(2 * time.Second)
	cv := decode[wire.ClusterView](t, mustGet(t, f.ts.URL+"/cluster"))
	if cv.Summary != (wire.WorkerSummary{Total: 1, Alive: 1}) || len(cv.Workers) != 1 {
		t.Fatalf("cluster = %+v", cv)
	}
	w := cv.Workers[0]
	if w.State != "ALIVE" || w.Epoch != rr.Epoch || w.LastHeartbeatAgeMs != 2000 || w.DeadSince != nil || w.ChunkCount != 1 {
		t.Fatalf("worker view = %+v", w)
	}
	if cv.Config != (wire.ClusterConfig{HeartbeatIntervalMs: 3000, DeadAfterMs: 10000, RepairGraceMs: 30000}) {
		t.Fatalf("config = %+v", cv.Config)
	}

	f.clk.Advance(9 * time.Second)
	f.reg.CheckLiveness()
	if a := hb(rr.Epoch); a != wire.ActionReregister {
		t.Fatalf("DEAD worker heartbeat action = %q", a)
	}
	f.clk.Advance(31 * time.Second)
	w = decode[wire.ClusterView](t, mustGet(t, f.ts.URL+"/cluster")).Workers[0]
	if w.State != "DEAD" || w.DeadSince == nil || !w.RepairEligible {
		t.Fatalf("dead worker view = %+v", w)
	}
}

func TestDashboardPageAndData(t *testing.T) {
	clk := &clock{t: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	reg := registry.New(registry.Options{DeadAfter: time.Minute, Now: clk.Now})
	m, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"), meta.Options{Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	a := New(reg, NoopReconciler{}, Config{Meta: m, ReplicationFactor: 2,
		HeartbeatInterval: time.Second, DeadAfter: time.Minute, RepairGrace: time.Minute})
	ts := httptest.NewServer(a.Handler())
	defer ts.Close()

	page, err := http.Get(ts.URL + "/dashboard/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if page.StatusCode != http.StatusOK || !strings.Contains(string(body), "Controller &amp; worker dashboard") {
		t.Fatalf("dashboard page status/body = %d/%q", page.StatusCode, string(body[:min(len(body), 80)]))
	}
	data, err := http.Get(ts.URL + "/admin/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Body.Close()
	if data.StatusCode != http.StatusOK {
		t.Fatalf("dashboard data status = %d", data.StatusCode)
	}
	var snapshot dashboardSnapshot
	if err := json.NewDecoder(data.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Audit.CheckedAt == "" || snapshot.Objects == nil || snapshot.Workers == nil {
		t.Fatalf("dashboard snapshot = %+v", snapshot)
	}
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestUnknownWorkerHeartbeatAsksToReregister(t *testing.T) {
	f := newFixture(t, nil)
	resp := f.post(t, "/internal/workers/heartbeat", wire.HeartbeatRequest{WorkerID: "worker9", Epoch: 1})
	if a := decode[wire.HeartbeatResponse](t, resp).Action; a != wire.ActionReregister {
		t.Fatalf("action = %q", a)
	}
}

func TestRegisterValidation(t *testing.T) {
	f := newFixture(t, nil)
	bad := []any{
		"{not json",
		`{"worker_id":"worker1","address":"http://w:1"} {"extra":1}`,
		regReq("Bad/ID"),
		wire.RegisterRequest{WorkerID: "worker1", Address: "worker1:9000"},
		wire.RegisterRequest{WorkerID: "worker1", Address: "http://worker1:9000/some/path"},
		regReq("worker1", wire.ChunkInfo{ChunkID: "../etc/passwd", Size: 1}),
		regReq("worker1", wire.ChunkInfo{ChunkID: chunk, Size: -1}),
	}
	for i, b := range bad {
		resp := f.post(t, "/internal/workers/register", b)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("case %d: status = %d, want 400", i, resp.StatusCode)
		}
		if e := decode[wire.ErrorResponse](t, resp); e.Error.Code != wire.CodeInvalidRequest {
			t.Errorf("case %d: code = %q", i, e.Error.Code)
		}
	}
	if n := len(f.reg.Snapshot()); n != 0 {
		t.Fatalf("invalid registrations created %d registry entries", n)
	}
	resp := f.post(t, "/internal/workers/heartbeat", wire.HeartbeatRequest{WorkerID: ""})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("heartbeat with empty id: status = %d", resp.StatusCode)
	}
}

func TestReconcileFailureLeavesWorkerSyncing(t *testing.T) {
	f := newFixture(t, reconcilerFunc(func(context.Context, string, uint64, []wire.ChunkInfo) error {
		return errors.New("db unavailable")
	}))
	resp := f.post(t, "/internal/workers/register", regReq("worker1"))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if w, _ := f.reg.Get("worker1"); w.State != registry.Syncing {
		t.Fatalf("state = %s, want SYNCING (unusable until a successful registration)", w.State)
	}
	if len(f.reg.Alive()) != 0 {
		t.Fatal("worker with failed reconciliation is ALIVE")
	}
}

// TestSupersededDuringReconcile simulates a second registration of the same
// worker arriving while the first is still reconciling: the first must not
// mark the worker ALIVE, and its caller gets 409.
func TestSupersededDuringReconcile(t *testing.T) {
	var f *fixture
	first := true
	f = newFixture(t, reconcilerFunc(func(_ context.Context, id string, _ uint64, _ []wire.ChunkInfo) error {
		if first {
			first = false
			f.reg.BeginRegistration(id, "http://"+id+":9000", registry.Capacity{}) // newer registration
		}
		return nil
	}))
	resp := f.post(t, "/internal/workers/register", regReq("worker1"))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	if e := decode[wire.ErrorResponse](t, resp); e.Error.Code != wire.CodeRegistrationSuperseded {
		t.Fatalf("code = %q", e.Error.Code)
	}
	if w, _ := f.reg.Get("worker1"); w.State != registry.Syncing {
		t.Fatalf("state = %s, want SYNCING", w.State)
	}
	// The worker retries and succeeds.
	if resp := f.post(t, "/internal/workers/register", regReq("worker1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	f := newFixture(t, nil)
	resp := mustGet(t, f.ts.URL+"/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if v := decode[map[string]string](t, resp); v["status"] != "ok" {
		t.Fatalf("body = %v", v)
	}
}
