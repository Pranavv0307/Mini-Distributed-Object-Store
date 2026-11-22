// Package integrity is the controller's cross-check of worker scrubber
// reports (architecture §3.6, §4.9 detection path 2).
//
// A worker's scrubber compares a chunk with the checksum in the chunk's own
// header; that is only a convenience. The controller's metadata checksum is
// the authority, so a reported suspect is not trusted as such: the Verifier
// reads the replica from the worker and checks it against the controller's
// record (layer 3). Only a confirmed mismatch marks the replica CORRUPT, after
// which the repair loop rewrites it in place (DEC-058).
//
// Reports arrive with heartbeats and are queued (deduplicated, bounded), so
// the heartbeat handler never waits on a worker.
package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/workerclient"
)

// ChunkGetter reads a replica from a worker (workerclient.Client).
type ChunkGetter interface {
	GetChunk(ctx context.Context, baseURL, chunkID string, maxBytes int64) (workerclient.Chunk, error)
}

// Verdict is the outcome of one cross-check.
type Verdict string

const (
	// Confirmed: the replica does not match the controller's record; it was
	// marked CORRUPT (or had already changed state meanwhile).
	Confirmed Verdict = "confirmed"
	// Healthy: the replica matches the controller's record.
	Healthy Verdict = "healthy"
	// Skipped: nothing to check (no OK row of a READY object on that worker,
	// or the worker is not ALIVE).
	Skipped Verdict = "skipped"
	// Inconclusive: the worker could not be read (timeout, 5xx); the
	// suspect will be checked again when it is reported again.
	Inconclusive Verdict = "inconclusive"
)

type key struct{ worker, chunk string }

// Verifier queues and cross-checks scrubber suspects.
type Verifier struct {
	meta        *meta.Store
	reg         *registry.Registry
	client      ChunkGetter
	log         *slog.Logger
	concurrency int
	maxQueue    int

	mu     sync.Mutex
	queued map[key]bool // queued or in flight
	order  []key
	wake   chan struct{}
}

// Config configures a Verifier.
type Config struct {
	Concurrency int // parallel cross-checks (default 2)
	MaxQueue    int // reports beyond this are dropped until the queue drains (default 1024)
	Logger      *slog.Logger
}

// New creates a Verifier.
func New(m *meta.Store, reg *registry.Registry, c ChunkGetter, cfg Config) *Verifier {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1024
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Verifier{meta: m, reg: reg, client: c, log: logger, concurrency: cfg.Concurrency,
		maxQueue: cfg.MaxQueue, queued: map[key]bool{}, wake: make(chan struct{}, 1)}
}

// Report queues the suspects one worker reported. It never blocks; a suspect
// already queued or in flight is not queued twice.
func (v *Verifier) Report(workerID string, chunkIDs []string) {
	if len(chunkIDs) == 0 {
		return
	}
	v.mu.Lock()
	added := false
	for _, id := range chunkIDs {
		k := key{workerID, id}
		if v.queued[k] {
			continue
		}
		if len(v.queued) >= v.maxQueue {
			v.log.Warn("scrub_suspect_dropped", "worker_id", workerID, "chunk_id", id, "reason", "queue_full")
			continue
		}
		v.queued[k] = true
		v.order = append(v.order, k)
		added = true
	}
	v.mu.Unlock()
	if added {
		select {
		case v.wake <- struct{}{}:
		default:
		}
	}
}

func (v *Verifier) next() (key, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.order) == 0 {
		return key{}, false
	}
	k := v.order[0]
	v.order = v.order[1:]
	return k, true
}

func (v *Verifier) done(k key) {
	v.mu.Lock()
	delete(v.queued, k)
	v.mu.Unlock()
}

// Run processes reports until ctx ends.
func (v *Verifier) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range v.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k, ok := v.next()
				if !ok {
					select {
					case <-ctx.Done():
						return
					case <-v.wake:
						// Pass the wake-up on so idle siblings also look.
						select {
						case v.wake <- struct{}{}:
						default:
						}
						continue
					}
				}
				_, _ = v.Verify(ctx, k.worker, k.chunk)
				v.done(k)
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Verify cross-checks one reported replica and records a confirmed mismatch.
func (v *Verifier) Verify(ctx context.Context, workerID, chunkID string) (Verdict, error) {
	target, err := v.meta.ReplicaToVerify(ctx, chunkID, workerID)
	if errors.Is(err, meta.ErrNotFound) {
		return Skipped, nil
	}
	if err != nil {
		v.log.Error("scrub_crosscheck_failed", "worker_id", workerID, "chunk_id", chunkID, "error", err)
		return Inconclusive, err
	}
	w, ok := v.reg.Get(workerID)
	if !ok || w.State != registry.Alive {
		return Skipped, nil
	}
	got, err := v.client.GetChunk(ctx, w.Address, chunkID, target.Size+1)
	reason := ""
	if err != nil {
		r, bad := workerclient.ProvesBadReplica(err)
		if !bad {
			v.log.Warn("scrub_crosscheck_inconclusive", "worker_id", workerID, "chunk_id", chunkID, "error", err)
			return Inconclusive, nil
		}
		reason = r
	} else {
		reason = mismatch(got, target)
	}
	if reason == "" {
		v.log.Info("scrub_suspect_not_confirmed", "worker_id", workerID, "chunk_id", chunkID)
		return Healthy, nil
	}
	changed, err := v.meta.MarkSuspectCorrupt(ctx, chunkID, workerID, target.UpdatedAt)
	if err != nil {
		v.log.Error("scrub_crosscheck_failed", "worker_id", workerID, "chunk_id", chunkID, "error", err)
		return Inconclusive, err
	}
	if changed {
		v.log.Warn("corruption_detected", "object_id", target.ObjectID, "chunk_id", chunkID,
			"worker_id", workerID, "reason", reason, "detected_by", "scrubber", "marked_corrupt", true)
	}
	return Confirmed, nil
}

// mismatch compares a replica read from a worker with the controller's
// record. A payload that matches but whose header checksum does not is still
// a damaged file (the worker's own check would keep failing); rewriting it in
// place from a verified copy fixes it.
func mismatch(got workerclient.Chunk, t meta.VerifyTarget) string {
	if int64(len(got.Data)) != t.Size {
		return "size_mismatch"
	}
	sum := sha256.Sum256(got.Data)
	if hex.EncodeToString(sum[:]) != t.ChecksumSHA256 {
		return "checksum_mismatch"
	}
	if got.StoredSHA256 != "" && got.StoredSHA256 != t.ChecksumSHA256 {
		return "header_checksum_mismatch"
	}
	return ""
}
