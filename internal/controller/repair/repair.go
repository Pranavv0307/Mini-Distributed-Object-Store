// Package repair is the controller's level-triggered reconciler
// (architecture §4.7, D13, D14, D16): it restores the replication factor of
// every chunk of every READY object, and trims superfluous copies.
//
// Each round compares the current metadata and registry against the desired
// state ("every chunk has RF effective replicas on distinct ALIVE workers")
// and acts on the difference; it keeps no state between rounds except
// counters, so a crash or a missed event is fixed by the next round.
//
// Replica classification (per round, from the registry):
//
//	effective  OK on an ALIVE worker
//	potential  OK on a worker that may come back soon: SYNCING, DEAD within
//	           REPAIR_GRACE, or unknown to a controller that restarted less
//	           than DEAD_AFTER+REPAIR_GRACE ago
//	lost       CORRUPT (no grace: corruption is certain), or OK on a worker
//	           DEAD for longer than REPAIR_GRACE (or unknown for that long)
//	busy       PENDING: an upload or repair owns it; the chunk is skipped
//
// A chunk is repaired when effective + potential < RF and effective ≥ 1;
// with effective = 0 nothing can be copied (it waits for a holder, or is
// reported unrecoverable). Repairs run riskiest first (fewest effective
// copies) with at most MaxConcurrent at once. Rounds never overlap, so a chunk
// is never repaired twice concurrently.
package repair

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/workerclient"
	"objstore/internal/wire"
)

// Client copies chunks between workers through the controller (D14).
type Client interface {
	GetChunk(ctx context.Context, baseURL, chunkID string, maxBytes int64) (workerclient.Chunk, error)
	PutChunk(ctx context.Context, baseURL, chunkID string, data []byte, sum [sha256.Size]byte) error
}

// Config configures a Repairer.
type Config struct {
	Interval          time.Duration // RECONCILER_INTERVAL
	MaxConcurrent     int           // MAX_CONCURRENT_REPAIRS
	RepairGrace       time.Duration // REPAIR_GRACE
	DeadAfter         time.Duration // DEAD_AFTER
	Warmup            time.Duration // delay before the first round after start
	ReplicationFactor int
	MinFreeBytes      int64
	// BatchSize bounds the candidate chunks examined per round (default 1000).
	BatchSize int
	Logger    *slog.Logger
}

// Stats are cumulative counters plus the latest round's view (for /cluster).
type Stats struct {
	InFlight          int64
	Completed         int64
	Failed            int64
	Trimmed           int64
	BytesCopied       int64
	WaitingForGrace   int64 // chunks short of RF whose missing copies may still come back
	Unrecoverable     int64 // chunks with no effective and no potential copy
	Rounds            int64
	LastRoundAt       time.Time
	LastRoundDuration time.Duration
}

// Repairer is the repair loop.
type Repairer struct {
	meta   *meta.Store
	reg    *registry.Registry
	client Client
	policy placement.Policy
	cfg    Config
	log    *slog.Logger

	round sync.Mutex // rounds never overlap

	inFlight, completed, failed, trimmed, bytes atomic.Int64

	mu            sync.Mutex
	stats         Stats
	lastFailure   map[string]string // chunk → reason, for log rate limiting
	unrecoverable map[string]bool   // chunks already reported unrecoverable
}

// New creates a Repairer.
func New(m *meta.Store, reg *registry.Registry, client Client, policy placement.Policy, cfg Config) *Repairer {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Repairer{meta: m, reg: reg, client: client, policy: policy, cfg: cfg, log: logger,
		lastFailure: map[string]string{}, unrecoverable: map[string]bool{}}
}

// Run waits for the warm-up period, then runs a round every Interval until
// ctx ends. The warm-up lets workers re-register after a controller restart
// before anything is judged missing (architecture §4.10 step 5).
func (r *Repairer) Run(ctx context.Context) {
	r.log.Info("repair_loop_waiting", "warmup", r.cfg.Warmup)
	select {
	case <-ctx.Done():
		return
	case <-time.After(r.cfg.Warmup):
	}
	r.log.Info("repair_loop_started", "interval", r.cfg.Interval, "max_concurrent", r.cfg.MaxConcurrent,
		"repair_grace", r.cfg.RepairGrace)
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("repair_round_failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Stats returns a snapshot of the counters.
func (r *Repairer) Stats() Stats {
	r.mu.Lock()
	s := r.stats
	r.mu.Unlock()
	s.InFlight, s.Completed, s.Failed = r.inFlight.Load(), r.completed.Load(), r.failed.Load()
	s.Trimmed, s.BytesCopied = r.trimmed.Load(), r.bytes.Load()
	return s
}

// RoundResult summarises one round.
type RoundResult struct {
	Candidates      int
	Repaired        int
	Failed          int
	Trimmed         int
	WaitingForGrace int
	Unrecoverable   int
	Busy            int
}

// workerView classifies one worker for this round.
type workerView struct {
	address string
	alive   bool // ALIVE: usable as source and target
	lost    bool // its OK copies count as lost
}

// workerViews is the round's snapshot of the registry. Workers it does not
// contain (e.g. not yet re-registered after a controller restart) get
// unknown: DEAD since the controller started.
type workerViews struct {
	known   map[string]workerView
	unknown workerView
}

func (w workerViews) get(id string) workerView {
	if v, ok := w.known[id]; ok {
		return v
	}
	return w.unknown
}

// job is one copy to create.
type job struct {
	chunk     meta.ChunkWithReplicas
	effective []string // ALIVE holders of an OK copy (sources)
	inPlace   string   // ALIVE worker with a CORRUPT copy to rewrite, or ""
	supersede *meta.Supersede
	exclude   map[string]bool // workers that already have a row for the chunk
}

// RunOnce performs one round and waits for its repairs. If a round is
// already running it returns immediately.
func (r *Repairer) RunOnce(ctx context.Context) (RoundResult, error) {
	var res RoundResult
	if !r.round.TryLock() {
		return res, nil
	}
	defer r.round.Unlock()
	start := time.Now()

	views, alive := r.classifyWorkers()
	rf := r.cfg.ReplicationFactor
	cands, err := r.meta.RepairCandidates(ctx, alive, rf, r.cfg.BatchSize)
	if err != nil {
		return res, err
	}
	res.Candidates = len(cands)

	var jobs []job
	type trim struct {
		objectID, chunkID, workerID, state, reason string
	}
	var trims []trim
	stillUnrecoverable := map[string]bool{}
	for _, ch := range cands {
		var effective, corruptAlive []string
		var potential int
		var lost []meta.Replica
		busy := false
		exclude := map[string]bool{}
		for _, rep := range ch.Replicas {
			exclude[rep.WorkerID] = true
			v := views.get(rep.WorkerID)
			switch {
			case rep.State == meta.ReplicaPending:
				busy = true
			case rep.State == meta.ReplicaCorrupt:
				lost = append(lost, rep)
				if v.alive {
					corruptAlive = append(corruptAlive, rep.WorkerID)
				}
			case v.alive:
				effective = append(effective, rep.WorkerID)
			case v.lost:
				lost = append(lost, rep)
			default:
				potential++
			}
		}
		switch {
		case busy:
			res.Busy++
		case len(effective) == 0:
			if potential > 0 {
				res.WaitingForGrace++ // a holder may still return; nothing to copy from
				continue
			}
			res.Unrecoverable++
			stillUnrecoverable[ch.ID] = true
			if !r.wasUnrecoverable(ch.ID) {
				r.log.Error("data_unrecoverable", "object_id", ch.ObjectID, "chunk_id", ch.ID, "chunk_index", ch.Idx,
					"detail", "no valid copy on any live or returning worker; object reads return 503")
			}
		case len(effective)+potential < rf:
			j := job{chunk: ch, effective: effective, exclude: exclude}
			if len(corruptAlive) > 0 {
				j.inPlace = corruptAlive[0] // rewrite in place (DEC-008)
			}
			for _, l := range lost { // the lost copy this repair replaces
				if l.WorkerID != j.inPlace {
					j.supersede = &meta.Supersede{WorkerID: l.WorkerID, State: l.State}
					break
				}
			}
			jobs = append(jobs, j)
		case len(effective) < rf:
			// Short now, but enough once the returning worker is back: wait.
			res.WaitingForGrace++
		default:
			// effective ≥ RF: lost copies are superfluous; extra effective
			// copies are trimmed (low priority, only if nothing needs repair).
			for _, l := range lost {
				trims = append(trims, trim{ch.ObjectID, ch.ID, l.WorkerID, l.State, "superseded"})
			}
			if extra := len(effective) - rf; extra > 0 {
				sort.Slice(effective, func(a, b int) bool { return r.load(effective[a]) > r.load(effective[b]) })
				for _, w := range effective[:extra] {
					trims = append(trims, trim{ch.ObjectID, ch.ID, w, meta.ReplicaOK, "over_replicated"})
				}
			}
		}
	}
	r.forgetRecovered(stillUnrecoverable)

	// Riskiest first: fewest effective copies, then a stable order.
	sort.SliceStable(jobs, func(a, b int) bool { return len(jobs[a].effective) < len(jobs[b].effective) })

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.MaxConcurrent)
	var repaired, failed atomic.Int64
	for _, j := range jobs {
		g.Go(func() error {
			if r.repairOne(gctx, j, views) {
				repaired.Add(1)
			} else {
				failed.Add(1)
			}
			return nil
		})
	}
	_ = g.Wait()
	res.Repaired, res.Failed = int(repaired.Load()), int(failed.Load())

	if len(jobs) == 0 && res.Unrecoverable == 0 {
		for _, t := range trims {
			moved, err := r.meta.TrimReplica(ctx, t.objectID, t.chunkID, t.workerID, t.state, t.reason, alive, rf)
			if err != nil {
				return res, err
			}
			if moved {
				res.Trimmed++
				r.trimmed.Add(1)
				r.log.Info("replica_trimmed", "object_id", t.objectID, "chunk_id", t.chunkID,
					"worker_id", t.workerID, "state", t.state, "reason", t.reason)
			}
		}
	}

	d := time.Since(start)
	r.mu.Lock()
	r.stats.Rounds++
	r.stats.LastRoundAt, r.stats.LastRoundDuration = start, d
	r.stats.WaitingForGrace, r.stats.Unrecoverable = int64(res.WaitingForGrace), int64(res.Unrecoverable)
	r.mu.Unlock()
	if res.Repaired+res.Failed+res.Trimmed > 0 {
		r.log.Info("repair_round", "candidates", res.Candidates, "repaired", res.Repaired, "failed", res.Failed,
			"trimmed", res.Trimmed, "waiting_for_grace", res.WaitingForGrace, "unrecoverable", res.Unrecoverable,
			"busy", res.Busy, "duration", d)
	}
	return res, nil
}

// classifyWorkers snapshots the registry. A worker it does not know (e.g.
// right after a controller restart) is treated as DEAD since the controller
// started: its copies count as lost once DEAD_AFTER + REPAIR_GRACE have
// passed since then (architecture §4.10).
func (r *Repairer) classifyWorkers() (workerViews, []string) {
	now := r.reg.Now()
	views := workerViews{known: map[string]workerView{}}
	var alive []string
	for _, w := range r.reg.Snapshot() {
		v := workerView{address: w.Address}
		switch w.State {
		case registry.Alive:
			v.alive = true
			alive = append(alive, w.ID)
		case registry.Dead:
			v.lost = w.RepairEligible(now, r.cfg.RepairGrace)
		}
		views.known[w.ID] = v
	}
	views.unknown = workerView{lost: now.Sub(r.reg.StartedAt()) > r.cfg.DeadAfter+r.cfg.RepairGrace}
	return views, alive
}

func (r *Repairer) load(workerID string) int64 {
	if w, ok := r.reg.Get(workerID); ok {
		return w.Capacity.ChunkCount
	}
	return 0
}

// repairOne creates one new copy of j.chunk. It reports success.
func (r *Repairer) repairOne(ctx context.Context, j job, views workerViews) bool {
	ch := j.chunk
	target, inPlace, err := r.chooseTarget(ctx, j)
	if err != nil {
		r.fail(ch, "", err)
		return false
	}
	if err := r.meta.ClaimRepair(ctx, ch.ObjectID, ch.ID, target, inPlace); err != nil {
		r.fail(ch, target, fmt.Errorf("claim: %w", err))
		return false
	}
	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)
	started := time.Now()
	r.log.Info("repair_started", "object_id", ch.ObjectID, "chunk_id", ch.ID, "chunk_index", ch.Idx,
		"target", target, "in_place", inPlace, "effective_copies", len(j.effective))

	data, source, err := r.readVerified(ctx, ch, j.effective, views)
	if err == nil {
		var sum [sha256.Size]byte
		copy(sum[:], mustHex(ch.ChecksumSHA256))
		err = r.client.PutChunk(ctx, views.get(target).address, ch.ID, data, sum)
		if err != nil {
			err = fmt.Errorf("write to %s: %w", target, err)
		}
	}
	if err != nil {
		// The write may or may not have landed: the claim becomes a pending
		// delete (DEC-007). Detached context so a cancelled round still
		// records it.
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if aerr := r.meta.AbandonRepair(actx, ch.ObjectID, ch.ID, target, "repair_failed"); aerr != nil {
			r.log.Error("repair_abandon_failed", "chunk_id", ch.ID, "target", target, "error", aerr)
		}
		r.fail(ch, target, err)
		return false
	}
	sup := j.supersede
	if err := r.meta.CompleteRepair(ctx, ch.ObjectID, ch.ID, target, sup); err != nil {
		// Typically the object was deleted meanwhile (architecture §5.6); the
		// copy just written is cleaned up as a pending delete or orphan.
		r.fail(ch, target, fmt.Errorf("complete: %w", err))
		return false
	}
	r.completed.Add(1)
	r.bytes.Add(int64(len(data)))
	r.mu.Lock()
	delete(r.lastFailure, ch.ID)
	r.mu.Unlock()
	supersededWorker := ""
	if sup != nil {
		supersededWorker = sup.WorkerID
	}
	r.log.Info("repair_completed", "object_id", ch.ObjectID, "chunk_id", ch.ID, "chunk_index", ch.Idx,
		"source", source, "target", target, "in_place", inPlace, "superseded", supersededWorker,
		"bytes", len(data), "duration", time.Since(started))
	return true
}

var errNoTarget = errors.New("no eligible target worker")

// chooseTarget prefers rewriting a CORRUPT copy in place on its (ALIVE)
// worker; otherwise picks an eligible ALIVE worker that holds no row for the
// chunk and has no pending delete for it.
func (r *Repairer) chooseTarget(ctx context.Context, j job) (string, bool, error) {
	pd, err := r.meta.PendingDeleteWorkers(ctx, j.chunk.ID)
	if err != nil {
		return "", false, err
	}
	if j.inPlace != "" && !pd[j.inPlace] {
		return j.inPlace, true, nil
	}
	exclude := map[string]bool{}
	for w := range j.exclude {
		exclude[w] = true
	}
	for w := range pd {
		exclude[w] = true
	}
	got := r.policy.Choose(j.chunk.ID, 1, placement.Eligible(r.reg.Alive(), exclude, r.cfg.MinFreeBytes))
	if len(got) == 0 {
		return "", false, errNoTarget
	}
	return got[0], false, nil
}

// readVerified fetches the chunk from one of the effective sources (random
// order) and verifies it against the controller's checksum. A source that
// returns bad data, or reports the chunk missing or its header corrupt, is
// marked CORRUPT (same rules as downloads, DEC-040).
func (r *Repairer) readVerified(ctx context.Context, ch meta.ChunkWithReplicas, sources []string, views workerViews) ([]byte, string, error) {
	order := append([]string(nil), sources...)
	rand.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
	for _, src := range order {
		got, err := r.client.GetChunk(ctx, views.get(src).address, ch.ID, ch.Size+1)
		if err != nil {
			var we *workerclient.Error
			if errors.As(err, &we) && (we.Status == http.StatusNotFound || we.Code == wire.CodeChunkCorrupt) {
				r.markSourceCorrupt(ctx, ch, src, "read_failed_"+we.Code)
			}
			continue
		}
		sum := sha256.Sum256(got.Data)
		if int64(len(got.Data)) != ch.Size || hex.EncodeToString(sum[:]) != ch.ChecksumSHA256 {
			r.markSourceCorrupt(ctx, ch, src, "checksum_mismatch")
			continue
		}
		return got.Data, src, nil
	}
	return nil, "", fmt.Errorf("no valid source among %v", sources)
}

func (r *Repairer) markSourceCorrupt(ctx context.Context, ch meta.ChunkWithReplicas, worker, reason string) {
	changed, err := r.meta.MarkReplicaCorrupt(ctx, ch.ID, worker)
	if err != nil {
		r.log.Error("mark_corrupt_failed", "chunk_id", ch.ID, "worker_id", worker, "error", err)
		return
	}
	if changed {
		r.log.Warn("corruption_detected", "object_id", ch.ObjectID, "chunk_id", ch.ID, "chunk_index", ch.Idx,
			"worker_id", worker, "reason", reason, "detected_by", "repair")
	}
}

// fail counts a failed repair and logs it unless the same chunk failed for
// the same reason in an earlier round (to keep a persistent shortage, e.g.
// too few workers, from flooding the log).
func (r *Repairer) fail(ch meta.ChunkWithReplicas, target string, err error) {
	r.failed.Add(1)
	reason := err.Error()
	r.mu.Lock()
	repeat := r.lastFailure[ch.ID] == reason
	r.lastFailure[ch.ID] = reason
	r.mu.Unlock()
	level := slog.LevelWarn
	if repeat {
		level = slog.LevelDebug
	}
	r.log.Log(context.Background(), level, "repair_failed", "object_id", ch.ObjectID, "chunk_id", ch.ID,
		"chunk_index", ch.Idx, "target", target, "error", reason)
}

func (r *Repairer) wasUnrecoverable(chunkID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	was := r.unrecoverable[chunkID]
	r.unrecoverable[chunkID] = true
	return was
}

func (r *Repairer) forgetRecovered(still map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.unrecoverable {
		if !still[id] {
			delete(r.unrecoverable, id)
		}
	}
}

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s) // checksums in the metadata are always valid hex
	return b
}
