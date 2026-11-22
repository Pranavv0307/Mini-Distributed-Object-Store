// Package janitor performs deferred physical cleanup (architecture §2.2,
// §4.4). Phase 3 scope (minimal, as planned):
//
//   - drain pending_deletes: send an idempotent DELETE for each entry whose
//     worker is ALIVE, remove the entry when the worker confirms, back off on
//     failure;
//   - finalize DELETING objects whose copies are all confirmed gone.
//
// Phase 9 adds stale-upload aborts, the periodic inventory orphan sweep, and
// startup recovery. The janitor is a single loop, so rounds never overlap;
// every step is idempotent and safe to repeat after a crash.
package janitor

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/wire"
)

// Deleter deletes chunk files on workers (workerclient.Client).
type Deleter interface {
	DeleteChunk(ctx context.Context, baseURL, chunkID string) error
}

// InventoryLister fetches a worker's inventory for the periodic sweep
// (workerclient.Client implements it).
type InventoryLister interface {
	Inventory(ctx context.Context, baseURL, workerID string) (wire.InventoryResponse, error)
}

// Config configures a Janitor.
type Config struct {
	Interval    time.Duration // JANITOR_INTERVAL
	BatchSize   int           // pending deletes handled per round (default 256)
	Concurrency int           // parallel DELETE calls (default 8)
	// MaxBackoff caps the retry delay after failed deletes (default 5m).
	MaxBackoff time.Duration
	// UploadStaleAfter: an UPLOADING object with no activity for this long
	// is aborted (UPLOAD_STALE_AFTER, default 10m).
	UploadStaleAfter time.Duration
	// InventorySweepInterval: period of the live-worker inventory sweep
	// (INVENTORY_SWEEP_INTERVAL, default 5m). The first sweep runs in the
	// first round.
	InventorySweepInterval time.Duration
	Logger                 *slog.Logger
}

// Janitor is the cleanup loop.
type Janitor struct {
	meta      *meta.Store
	reg       *registry.Registry
	deleter   Deleter
	cfg       Config
	kick      chan struct{}
	log       *slog.Logger
	lastSweep time.Time
}

// New creates a Janitor.
func New(m *meta.Store, reg *registry.Registry, d Deleter, cfg Config) *Janitor {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	if cfg.UploadStaleAfter <= 0 {
		cfg.UploadStaleAfter = 10 * time.Minute
	}
	if cfg.InventorySweepInterval <= 0 {
		cfg.InventorySweepInterval = 5 * time.Minute
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Janitor{meta: m, reg: reg, deleter: d, cfg: cfg, kick: make(chan struct{}, 1), log: logger}
}

// Kick asks for a round as soon as possible (e.g. right after an upload
// abort). It never blocks; kicks while a round is pending coalesce.
func (j *Janitor) Kick() {
	select {
	case j.kick <- struct{}{}:
	default:
	}
}

// Run executes rounds every Interval (and on Kick) until ctx is done.
func (j *Janitor) Run(ctx context.Context) {
	t := time.NewTicker(j.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-j.kick:
		}
		if _, err := j.RunOnce(ctx); err != nil && ctx.Err() == nil {
			j.log.Error("janitor_round_failed", "error", err)
		}
	}
}

// Result summarises one round.
type Result struct {
	Deleted   int      // pending deletes confirmed by workers
	Failed    int      // pending deletes that failed and were rescheduled
	Finalized []string // object IDs whose rows were removed
}

// RunOnce performs one round.
func (j *Janitor) RunOnce(ctx context.Context) (Result, error) {
	var res Result
	if n, err := j.meta.AbortStaleUploads(ctx, j.cfg.UploadStaleAfter); err != nil {
		return res, err
	} else if n > 0 {
		j.log.Warn("stale_uploads_aborted", "objects", n)
	}
	if j.cfg.InventorySweepInterval > 0 && (j.lastSweep.IsZero() || time.Since(j.lastSweep) >= j.cfg.InventorySweepInterval) {
		if err := j.sweepInventories(ctx); err != nil {
			return res, err
		}
		j.lastSweep = time.Now()
	}
	alive := j.reg.Alive()
	addr := make(map[string]string, len(alive))
	workerIDs := make([]string, 0, len(alive))
	for _, w := range alive {
		addr[w.ID] = w.Address
		workerIDs = append(workerIDs, w.ID)
	}
	due, err := j.meta.DuePendingDeletes(ctx, workerIDs, j.cfg.BatchSize)
	if err != nil {
		return res, err
	}

	type outcome struct {
		pd  meta.PendingDelete
		err error
	}
	outcomes := make([]outcome, len(due))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(j.cfg.Concurrency)
	for i, pd := range due {
		g.Go(func() error {
			outcomes[i] = outcome{pd: pd, err: j.deleter.DeleteChunk(gctx, addr[pd.WorkerID], pd.ChunkID)}
			return nil // per-entry failures are recorded below, not fatal
		})
	}
	_ = g.Wait()

	// Record outcomes in short, separate transactions (never during the
	// network calls above).
	for _, o := range outcomes {
		if o.err == nil {
			if err := j.meta.CompletePendingDelete(ctx, o.pd.ChunkID, o.pd.WorkerID); err != nil {
				return res, err
			}
			res.Deleted++
			j.log.Debug("chunk_delete_confirmed", "chunk_id", o.pd.ChunkID, "worker_id", o.pd.WorkerID, "reason", o.pd.Reason)
			continue
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		backoff := min(j.cfg.Interval<<min(o.pd.Attempts, 20), j.cfg.MaxBackoff)
		if err := j.meta.FailPendingDelete(ctx, o.pd.ChunkID, o.pd.WorkerID, o.err.Error(), backoff); err != nil {
			return res, err
		}
		res.Failed++
		j.log.Warn("chunk_delete_failed", "chunk_id", o.pd.ChunkID, "worker_id", o.pd.WorkerID,
			"attempt", o.pd.Attempts+1, "retry_in", backoff, "error", o.err)
	}

	res.Finalized, err = j.meta.FinalizeDeletedObjects(ctx)
	if err != nil {
		return res, err
	}
	for _, id := range res.Finalized {
		j.log.Info("object_purged", "object_id", id)
	}
	if res.Deleted > 0 || res.Failed > 0 {
		j.log.Info("janitor_round", "deleted", res.Deleted, "failed", res.Failed, "finalized", len(res.Finalized))
	}
	return res, nil
}

func (j *Janitor) sweepInventories(ctx context.Context) error {
	lister, ok := j.deleter.(InventoryLister)
	if !ok {
		return nil
	}
	for _, w := range j.reg.Alive() {
		// Metadata first, inventory second: the before/after rule that makes
		// the sweep safe while writes land on the worker (DEC-057).
		before, err := j.meta.SnapshotWorker(ctx, w.ID)
		if err != nil {
			return err
		}
		inv, err := lister.Inventory(ctx, w.Address, w.ID)
		if err != nil {
			j.log.Warn("inventory_sweep_failed", "worker_id", w.ID, "error", err)
			continue
		}
		entries := make([]meta.InventoryEntry, len(inv.Chunks))
		for i, c := range inv.Chunks {
			entries[i] = meta.InventoryEntry{ChunkID: c.ChunkID, Size: c.Size}
		}
		result, err := j.meta.SweepInventory(ctx, before, entries)
		if err != nil {
			return err
		}
		if result.OrphansQueued > 0 {
			j.log.Warn("inventory_orphans_queued", "worker_id", w.ID, "count", result.OrphansQueued)
		}
		if result.MissingMarked > 0 || result.SizeMismatch > 0 {
			j.log.Warn("corruption_detected", "worker_id", w.ID, "detected_by", "inventory_sweep",
				"missing", result.MissingMarked, "size_mismatch", result.SizeMismatch)
		}
	}
	return nil
}
