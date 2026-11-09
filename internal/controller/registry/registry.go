// Package registry is the controller's in-memory view of workers: identity,
// address, liveness state, registration epoch, and reported capacity.
//
// The registry is deliberately not persisted (architecture §3.5): after a
// controller restart it is empty, every heartbeat is answered "reregister",
// and the view is rebuilt from fresh registrations with fresh inventories.
//
// State machine (DEC-006):
//
//	(unknown) ──BeginRegistration──▶ SYNCING ──CompleteRegistration──▶ ALIVE
//	SYNCING|ALIVE ──no heartbeat for DeadAfter──▶ DEAD
//	any state ──BeginRegistration──▶ SYNCING (new epoch)
//
// Only heartbeats and registrations change liveness (architecture §4.6);
// data-path errors never do. All times come from the controller's own clock.
package registry

import (
	"log/slog"
	"sort"
	"sync"
	"time"
)

// State is a worker's liveness state.
type State string

// Worker states.
const (
	Syncing State = "SYNCING" // registered; inventory reconciliation in progress
	Alive   State = "ALIVE"   // usable for placement, reads, and repair
	Dead    State = "DEAD"    // no heartbeat for DeadAfter
)

// Capacity is what a worker reports about its disk.
type Capacity struct {
	FreeBytes  int64 // -1 if unknown
	UsedBytes  int64
	ChunkCount int64
}

// Worker is a snapshot of one worker's registry entry.
type Worker struct {
	ID            string
	Address       string
	State         State
	Epoch         uint64
	RegisteredAt  time.Time // start of the latest registration
	LastHeartbeat time.Time // controller receive time of the latest heartbeat or registration
	DeadSince     time.Time // zero unless State == Dead
	Capacity      Capacity
}

// RepairEligible reports whether w has been DEAD for longer than grace
// (architecture §3.3: derived, not a state).
func (w Worker) RepairEligible(now time.Time, grace time.Duration) bool {
	return w.State == Dead && now.Sub(w.DeadSince) > grace
}

// Options configures a Registry.
type Options struct {
	DeadAfter time.Duration
	// Now is the clock; defaults to time.Now. Tests inject a fake clock.
	Now    func() time.Time
	Logger *slog.Logger
}

// Registry is safe for concurrent use. All methods hold one mutex briefly and
// never perform I/O while holding it.
type Registry struct {
	mu        sync.Mutex
	workers   map[string]*Worker
	lastEpoch uint64
	deadAfter time.Duration
	now       func() time.Time
	log       *slog.Logger
}

// New creates an empty registry.
//
// Epochs start at the controller's boot time in nanoseconds and increase by
// one per registration. A worker still holding an epoch issued by a previous
// controller incarnation therefore never matches a current epoch by accident.
func New(opts Options) *Registry {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Registry{
		workers:   make(map[string]*Worker),
		lastEpoch: uint64(now().UnixNano()),
		deadAfter: opts.DeadAfter,
		now:       now,
		log:       logger,
	}
}

// BeginRegistration records a (re-)registration: the worker becomes SYNCING
// under a new epoch, which is returned. Any previous registration of the same
// worker is superseded, so its CompleteRegistration will fail.
func (r *Registry) BeginRegistration(id, address string, capacity Capacity) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.lastEpoch++
	epoch := r.lastEpoch

	w, known := r.workers[id]
	prev := State("")
	if known {
		prev = w.State
	} else {
		w = &Worker{ID: id}
		r.workers[id] = w
	}
	w.Address = address
	w.State = Syncing
	w.Epoch = epoch
	w.RegisteredAt = now
	w.LastHeartbeat = now
	w.DeadSince = time.Time{}
	w.Capacity = capacity
	r.log.Info("worker_syncing", "worker_id", id, "address", address, "epoch", epoch, "previous_state", string(prev))
	return epoch
}

// CompleteRegistration moves the worker from SYNCING to ALIVE, but only if
// epoch is still its current epoch and it is still SYNCING (it may have been
// superseded by a newer registration or declared DEAD meanwhile). It reports
// whether the transition happened.
func (r *Registry) CompleteRegistration(id string, epoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	if !ok || w.Epoch != epoch || w.State != Syncing {
		state := State("")
		var current uint64
		if ok {
			state, current = w.State, w.Epoch
		}
		r.log.Warn("worker_registration_superseded", "worker_id", id, "epoch", epoch,
			"current_epoch", current, "current_state", string(state))
		return false
	}
	w.State = Alive
	r.log.Info("worker_alive", "worker_id", id, "epoch", epoch,
		"sync_duration", r.now().Sub(w.RegisteredAt))
	return true
}

// Heartbeat records a heartbeat received now and reports whether the worker
// must re-register: it is unknown (e.g. the controller restarted), DEAD, or
// the epoch is not its current one.
func (r *Registry) Heartbeat(id string, epoch uint64, capacity Capacity) (reregister bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	switch {
	case !ok:
		r.log.Info("worker_reregister_requested", "worker_id", id, "reason", "unknown_worker")
		return true
	case w.State == Dead:
		r.log.Info("worker_reregister_requested", "worker_id", id, "reason", "worker_dead")
		return true
	case w.Epoch != epoch:
		r.log.Info("worker_reregister_requested", "worker_id", id, "reason", "stale_epoch",
			"epoch", epoch, "current_epoch", w.Epoch)
		return true
	}
	w.LastHeartbeat = r.now()
	w.Capacity = capacity
	return false
}

// CheckLiveness marks every SYNCING or ALIVE worker whose last heartbeat is
// older than DeadAfter as DEAD and returns the IDs it changed. It is called
// periodically by the failure detector and is idempotent.
func (r *Registry) CheckLiveness() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	var died []string
	for _, w := range r.workers {
		if w.State == Dead {
			continue
		}
		if silence := now.Sub(w.LastHeartbeat); silence > r.deadAfter {
			prev := w.State
			w.State = Dead
			w.DeadSince = now
			died = append(died, w.ID)
			r.log.Warn("worker_dead", "worker_id", w.ID, "previous_state", string(prev),
				"silence", silence, "epoch", w.Epoch)
		}
	}
	sort.Strings(died)
	return died
}

// Get returns a snapshot of one worker.
func (r *Registry) Get(id string) (Worker, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	if !ok {
		return Worker{}, false
	}
	return *w, true
}

// Snapshot returns all workers sorted by ID.
func (r *Registry) Snapshot() []Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Alive returns the ALIVE workers sorted by ID. Placement and read selection
// (from Phase 3) use only these; SYNCING workers are excluded (DEC-006).
func (r *Registry) Alive() []Worker {
	all := r.Snapshot()
	out := all[:0]
	for _, w := range all {
		if w.State == Alive {
			out = append(out, w)
		}
	}
	return out
}

// Now returns the registry's clock reading (for consistent age computations).
func (r *Registry) Now() time.Time { return r.now() }
