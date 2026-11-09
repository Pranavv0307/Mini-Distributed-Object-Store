package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"objstore/internal/httpx"
	"objstore/internal/wire"
	"objstore/internal/worker/store"
)

// fakeWorker records gate operations and serves a mutable inventory.
type fakeWorker struct {
	mu         sync.Mutex
	inv        []store.Entry
	open       bool
	registered bool
	events     []string
}

func (f *fakeWorker) Inventory() ([]store.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.Entry(nil), f.inv...), nil
}
func (f *fakeWorker) Stats() (store.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return store.Stats{ChunkCount: int64(len(f.inv))}, nil
}
func (f *fakeWorker) FreeBytes() int64 { return 1234 }
func (f *fakeWorker) OpenWrites() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = true
	f.events = append(f.events, "open")
}
func (f *fakeWorker) CloseWritesAndDrain(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = false
	f.events = append(f.events, "close")
	return nil
}
func (f *fakeWorker) SetRegistered(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = v
}
func (f *fakeWorker) state() (open, registered bool, events []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open, f.registered, append([]string(nil), f.events...)
}

// fakeController scripts controller behaviour.
type fakeController struct {
	mu            sync.Mutex
	down          bool // respond 503 to everything
	rejected      int  // requests answered 503 while down
	reregister    bool // answer the next heartbeat with "reregister"
	epoch         uint64
	registrations []wire.RegisterRequest
	heartbeats    []wire.HeartbeatRequest
}

func (c *fakeController) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/workers/register", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.down {
			c.rejected++
			httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInternal, "down")
			return
		}
		var req wire.RegisterRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		c.registrations = append(c.registrations, req)
		c.epoch++
		httpx.WriteJSON(w, http.StatusOK, wire.RegisterResponse{Epoch: c.epoch, HeartbeatIntervalMs: 10})
	})
	mux.HandleFunc("POST /internal/workers/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.down {
			c.rejected++
			httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInternal, "down")
			return
		}
		var req wire.HeartbeatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		c.heartbeats = append(c.heartbeats, req)
		action := wire.ActionOK
		if c.reregister || req.Epoch != c.epoch {
			c.reregister = false
			action = wire.ActionReregister
		}
		httpx.WriteJSON(w, http.StatusOK, wire.HeartbeatResponse{Action: action})
	})
	return mux
}

func (c *fakeController) rejectedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rejected
}

func (c *fakeController) counts() (regs, hbs int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.registrations), len(c.heartbeats)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// start runs an agent against ctrl. done is closed when Run returns; *runErr
// then holds its result. (A closed channel, not a value channel, so that
// both the test and the cleanup can wait on it.)
func start(t *testing.T, ctrl *fakeController, fw *fakeWorker) (cancel func(), done <-chan struct{}, runErr *error) {
	t.Helper()
	ts := httptest.NewServer(ctrl.handler())
	t.Cleanup(ts.Close)
	a := New(Config{
		WorkerID: "worker1", AdvertiseAddr: "http://worker1:9000", ControllerURL: ts.URL,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
		RegisterTimeout: time.Second, HeartbeatTimeout: time.Second,
	}, fw, fw)
	ctx, c := context.WithCancel(context.Background())
	ch := make(chan struct{})
	var err error
	go func() { err = a.Run(ctx); close(ch) }()
	t.Cleanup(func() { c(); <-ch })
	return c, ch, &err
}

func TestRegisterThenHeartbeat(t *testing.T) {
	ctrl := &fakeController{}
	fw := &fakeWorker{inv: []store.Entry{{ChunkID: "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000", Size: 7}}}
	start(t, ctrl, fw)

	waitFor(t, "heartbeats", func() bool { _, hbs := ctrl.counts(); return hbs >= 3 })
	ctrl.mu.Lock()
	reg := ctrl.registrations[0]
	hb := ctrl.heartbeats[0]
	ctrl.mu.Unlock()
	if reg.WorkerID != "worker1" || reg.Address != "http://worker1:9000" || reg.FreeBytes != 1234 ||
		reg.UsedBytes != 7 || len(reg.Inventory) != 1 || reg.Inventory[0].Size != 7 {
		t.Fatalf("registration = %+v", reg)
	}
	if hb.Epoch != 1 || hb.ChunkCount != 1 || hb.SuspectChunks == nil {
		t.Fatalf("heartbeat = %+v", hb)
	}
	open, registered, events := fw.state()
	if !open || !registered {
		t.Fatalf("after registration: open=%v registered=%v", open, registered)
	}
	if len(events) != 2 || events[0] != "close" || events[1] != "open" {
		t.Fatalf("gate events = %v, want [close open] (snapshot taken with gate closed)", events)
	}
}

func TestReregisterOnRequest(t *testing.T) {
	ctrl := &fakeController{}
	fw := &fakeWorker{}
	start(t, ctrl, fw)
	waitFor(t, "first registration", func() bool { r, _ := ctrl.counts(); return r == 1 })

	// The worker gains a chunk; then the controller (e.g. after a restart) asks
	// for re-registration. The new registration must carry the new inventory.
	fw.mu.Lock()
	fw.inv = append(fw.inv, store.Entry{ChunkID: "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000001", Size: 3})
	fw.mu.Unlock()
	ctrl.mu.Lock()
	ctrl.reregister = true
	ctrl.mu.Unlock()

	waitFor(t, "re-registration", func() bool { r, _ := ctrl.counts(); return r == 2 })
	waitFor(t, "heartbeat with new epoch", func() bool {
		ctrl.mu.Lock()
		defer ctrl.mu.Unlock()
		last := ctrl.heartbeats[len(ctrl.heartbeats)-1]
		return last.Epoch == 2
	})
	ctrl.mu.Lock()
	inv := ctrl.registrations[1].Inventory
	ctrl.mu.Unlock()
	if len(inv) != 1 || inv[0].Size != 3 {
		t.Fatalf("re-registration inventory = %+v", inv)
	}
	_, _, events := fw.state()
	if len(events) != 4 || events[2] != "close" || events[3] != "open" {
		t.Fatalf("gate events = %v, want close/open around each registration", events)
	}
}

func TestRetriesWhileControllerDown(t *testing.T) {
	ctrl := &fakeController{down: true}
	fw := &fakeWorker{}
	start(t, ctrl, fw)
	waitFor(t, "several failed registration attempts", func() bool { return ctrl.rejectedCount() >= 3 })
	if open, registered, _ := fw.state(); open || registered {
		t.Fatal("gate opened / registered while the controller was down")
	}
	ctrl.mu.Lock()
	ctrl.down = false
	ctrl.mu.Unlock()
	waitFor(t, "registration after controller recovery", func() bool {
		open, registered, _ := fw.state()
		return open && registered
	})
}

func TestHeartbeatFailuresKeepServing(t *testing.T) {
	ctrl := &fakeController{}
	fw := &fakeWorker{}
	start(t, ctrl, fw)
	waitFor(t, "registered", func() bool { _, reg, _ := fw.state(); return reg })
	ctrl.mu.Lock()
	ctrl.down = true
	ctrl.mu.Unlock()
	waitFor(t, "several failed heartbeats", func() bool { return ctrl.rejectedCount() >= 3 })
	if open, _, _ := fw.state(); !open {
		t.Fatal("gate closed because heartbeats failed; the worker should keep serving")
	}
	r1, _ := ctrl.counts()
	ctrl.mu.Lock()
	ctrl.down = false
	ctrl.mu.Unlock()
	waitFor(t, "heartbeats resume", func() bool { _, hbs := ctrl.counts(); return hbs > 0 })
	if r2, _ := ctrl.counts(); r2 != r1 {
		t.Fatalf("re-registered (%d → %d) without being asked", r1, r2)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	ctrl := &fakeController{down: true}
	cancel, done, runErr := start(t, ctrl, &fakeWorker{})
	cancel()
	select {
	case <-done:
		if *runErr != context.Canceled {
			t.Fatalf("Run returned %v, want context.Canceled", *runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestJitterRange(t *testing.T) {
	for range 1000 {
		j := jitter(100 * time.Millisecond)
		if j < 50*time.Millisecond || j >= 100*time.Millisecond {
			t.Fatalf("jitter = %v out of [50ms, 100ms)", j)
		}
	}
}
