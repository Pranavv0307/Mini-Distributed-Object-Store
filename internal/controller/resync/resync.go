// Package resync reconciles a registering worker's inventory with the
// metadata (architecture §4.5, D20). It implements api.InventoryReconciler
// and runs while the worker is SYNCING (DEC-006), i.e. while no placement,
// read, or repair can use the worker.
package resync

import (
	"context"
	"log/slog"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/wire"
)

// Reconciler applies meta.ReconcileInventory and reports the outcome.
type Reconciler struct {
	meta *meta.Store
	log  *slog.Logger
	// onOrphans (may be nil) is called when orphans were queued, e.g. to kick
	// the janitor so they are deleted promptly.
	onOrphans func()
}

// New creates a Reconciler.
func New(m *meta.Store, logger *slog.Logger, onOrphans func()) *Reconciler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Reconciler{meta: m, log: logger, onOrphans: onOrphans}
}

// ReconcileInventory implements api.InventoryReconciler.
func (r *Reconciler) ReconcileInventory(ctx context.Context, workerID string, epoch uint64, inv []wire.ChunkInfo) error {
	start := time.Now()
	entries := make([]meta.InventoryEntry, len(inv))
	for i, c := range inv {
		entries[i] = meta.InventoryEntry{ChunkID: c.ChunkID, Size: c.Size}
	}
	res, err := r.meta.ReconcileInventory(ctx, workerID, entries)
	if err != nil {
		// Partial progress is safe: every step is conditional and idempotent,
		// and the worker retries registration (staying SYNCING meanwhile).
		return err
	}
	level := slog.LevelInfo
	if res.MissingRemoved > 0 || res.SizeMismatch > 0 {
		level = slog.LevelWarn // data was lost or damaged on this worker
	}
	r.log.Log(ctx, level, "inventory_reconciled", "worker_id", workerID, "epoch", epoch,
		"inventory", len(inv), "matched", res.Matched, "missing_removed", res.MissingRemoved,
		"size_mismatch_corrupt", res.SizeMismatch, "corrupt_kept", res.CorruptKept,
		"pending_kept", res.PendingKept, "orphans_queued", res.OrphansQueued,
		"skipped_concurrent", res.Skipped, "duration", time.Since(start))
	if res.OrphansQueued > 0 && r.onOrphans != nil {
		r.onOrphans()
	}
	return nil
}
