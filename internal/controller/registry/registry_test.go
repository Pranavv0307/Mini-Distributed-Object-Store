package registry

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTest() (*Registry, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	return New(Options{DeadAfter: 10 * time.Second, Now: clk.Now}), clk
}

func mustState(t *testing.T, r *Registry, id string, want State) Worker {
	t.Helper()
	w, ok := r.Get(id)
	if !ok {
		t.Fatalf("%s not in registry", id)
	}
	if w.State != want {
		t.Fatalf("%s state = %s, want %s", id, w.State, want)
	}
	return w
}

func TestRegistrationLifecycle(t *testing.T) {
	r, _ := newTest()
	e := r.BeginRegistration("worker1", "http://worker1:9000", Capacity{FreeBytes: 100})
	w := mustState(t, r, "worker1", Syncing)
	if w.Epoch != e || w.Address != "http://worker1:9000" || w.Capacity.FreeBytes != 100 {
		t.Fatalf("entry = %+v", w)
	}
	if len(r.Alive()) != 0 {
		t.Fatal("SYNCING worker reported as alive (must be excluded from placement and reads)")
	}
	if !r.CompleteRegistration("worker1", e) {
		t.Fatal("CompleteRegistration failed")
	}
	mustState(t, r, "worker1", Alive)
	if got := r.Alive(); len(got) != 1 || got[0].ID != "worker1" {
		t.Fatalf("Alive() = %+v", got)
	}
	if r.CompleteRegistration("worker1", e) {
		t.Fatal("second CompleteRegistration succeeded (worker no longer SYNCING)")
	}
}

func TestEpochsAreUniqueAndIncreasing(t *testing.T) {
	r, _ := newTest()
	e1 := r.BeginRegistration("a", "http://a:1", Capacity{})
	e2 := r.BeginRegistration("b", "http://b:1", Capacity{})
	e3 := r.BeginRegistration("a", "http://a:1", Capacity{})
	if !(e1 < e2 && e2 < e3) {
		t.Fatalf("epochs not increasing: %d %d %d", e1, e2, e3)
	}
	// A second registry (controller restart) starts from its boot time, so it
	// never reissues a small epoch held over from the first one.
	r2 := New(Options{DeadAfter: time.Second})
	if e := r2.BeginRegistration("a", "http://a:1", Capacity{}); e <= 1_000_000_000 {
		t.Fatalf("epoch %d does not start from boot time", e)
	}
}

func TestSupersededRegistrationCannotMarkAlive(t *testing.T) {
	r, _ := newTest()
	old := r.BeginRegistration("worker1", "http://w1:9000", Capacity{})
	newer := r.BeginRegistration("worker1", "http://w1:9000", Capacity{})
	if r.CompleteRegistration("worker1", old) {
		t.Fatal("superseded registration marked the worker ALIVE")
	}
	mustState(t, r, "worker1", Syncing)
	if !r.CompleteRegistration("worker1", newer) {
		t.Fatal("current registration could not complete")
	}
	mustState(t, r, "worker1", Alive)
}

func TestDeathDuringSyncBlocksCompletion(t *testing.T) {
	r, clk := newTest()
	e := r.BeginRegistration("worker1", "http://w1:9000", Capacity{})
	clk.Advance(11 * time.Second) // reconciliation took longer than DeadAfter, no heartbeats
	if died := r.CheckLiveness(); len(died) != 1 || died[0] != "worker1" {
		t.Fatalf("CheckLiveness = %v", died)
	}
	if r.CompleteRegistration("worker1", e) {
		t.Fatal("DEAD worker was marked ALIVE by a late CompleteRegistration")
	}
	mustState(t, r, "worker1", Dead)
}

func TestHeartbeatActions(t *testing.T) {
	r, clk := newTest()
	if !r.Heartbeat("ghost", 1, Capacity{}) {
		t.Fatal("unknown worker not told to re-register")
	}
	e := r.BeginRegistration("worker1", "http://w1:9000", Capacity{})
	// A heartbeat during SYNCING with the current epoch is accepted.
	if r.Heartbeat("worker1", e, Capacity{}) {
		t.Fatal("SYNCING worker with current epoch told to re-register")
	}
	r.CompleteRegistration("worker1", e)
	if !r.Heartbeat("worker1", e-1, Capacity{}) {
		t.Fatal("stale epoch not told to re-register")
	}
	clk.Advance(5 * time.Second)
	if r.Heartbeat("worker1", e, Capacity{FreeBytes: 7, UsedBytes: 3, ChunkCount: 1}) {
		t.Fatal("valid heartbeat told to re-register")
	}
	w := mustState(t, r, "worker1", Alive)
	if !w.LastHeartbeat.Equal(clk.Now()) || w.Capacity.ChunkCount != 1 {
		t.Fatalf("heartbeat not recorded: %+v", w)
	}
}

func TestFailureDetection(t *testing.T) {
	r, clk := newTest()
	for _, id := range []string{"w1", "w2"} {
		r.CompleteRegistration(id, r.BeginRegistration(id, "http://"+id+":9000", Capacity{}))
	}
	clk.Advance(6 * time.Second)
	e2, _ := r.Get("w2")
	r.Heartbeat("w2", e2.Epoch, Capacity{})
	clk.Advance(5 * time.Second) // w1 silent 11s, w2 silent 5s

	if died := r.CheckLiveness(); len(died) != 1 || died[0] != "w1" {
		t.Fatalf("CheckLiveness = %v, want [w1]", died)
	}
	w1 := mustState(t, r, "w1", Dead)
	if !w1.DeadSince.Equal(clk.Now()) {
		t.Fatalf("DeadSince = %v", w1.DeadSince)
	}
	mustState(t, r, "w2", Alive)
	if died := r.CheckLiveness(); len(died) != 0 {
		t.Fatalf("second CheckLiveness changed %v (must be idempotent)", died)
	}

	// Exactly DeadAfter of silence is not yet dead.
	clk.Advance(5 * time.Second) // w2 silent exactly 10s
	if died := r.CheckLiveness(); len(died) != 0 {
		t.Fatalf("worker declared dead at exactly DeadAfter: %v", died)
	}

	// A DEAD worker's heartbeat does not revive it; it must re-register.
	if !r.Heartbeat("w1", w1.Epoch, Capacity{}) {
		t.Fatal("DEAD worker's heartbeat accepted")
	}
	mustState(t, r, "w1", Dead)

	// Repair eligibility is derived from DeadSince and the grace period.
	// w1 has been dead for 5s at this point.
	clk.Advance(24 * time.Second) // dead 29s
	w1, _ = r.Get("w1")
	if w1.RepairEligible(clk.Now(), 30*time.Second) {
		t.Fatal("repair-eligible before grace elapsed")
	}
	clk.Advance(2 * time.Second)
	if !w1.RepairEligible(clk.Now(), 30*time.Second) {
		t.Fatal("not repair-eligible after grace elapsed")
	}

	// Returning: re-registration goes through SYNCING and clears DeadSince.
	e := r.BeginRegistration("w1", "http://w1:9000", Capacity{})
	w1 = mustState(t, r, "w1", Syncing)
	if !w1.DeadSince.IsZero() {
		t.Fatal("DeadSince not cleared on re-registration")
	}
	r.CompleteRegistration("w1", e)
	mustState(t, r, "w1", Alive)
}

func TestSyncingWorkerCanDie(t *testing.T) {
	r, clk := newTest()
	r.BeginRegistration("w1", "http://w1:9000", Capacity{})
	clk.Advance(10*time.Second + time.Nanosecond)
	if died := r.CheckLiveness(); len(died) != 1 {
		t.Fatalf("silent SYNCING worker not declared dead: %v", died)
	}
}

func TestRunFailureDetectorStopsOnCancel(t *testing.T) {
	r := New(Options{DeadAfter: 20 * time.Millisecond})
	r.BeginRegistration("w1", "http://w1:9000", Capacity{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { RunFailureDetector(ctx, r, 5*time.Millisecond); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if w, _ := r.Get("w1"); w.State == Dead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("detector never marked the silent worker DEAD")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("detector did not stop after cancel")
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New(Options{DeadAfter: time.Hour})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			id := []string{"a", "b", "c", "d"}[i%4]
			for range 200 {
				e := r.BeginRegistration(id, "http://x:1", Capacity{})
				r.Heartbeat(id, e, Capacity{})
				r.CompleteRegistration(id, e)
				r.CheckLiveness()
				_ = r.Snapshot()
				_ = r.Alive()
			}
		})
	}
	wg.Wait()
	if n := len(r.Snapshot()); n != 4 {
		t.Fatalf("registry has %d workers, want 4", n)
	}
}
