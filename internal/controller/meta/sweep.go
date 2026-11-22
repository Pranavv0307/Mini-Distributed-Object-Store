package meta

import (
	"context"
	"database/sql"

	"objstore/internal/ids"
)

// Periodic inventory checks on a LIVE worker (Phase 9 sweep, audit I4).
//
// Unlike ReconcileInventory, which runs while the worker is SYNCING (no write
// can land on it), these run while uploads, repairs, and deletes target the
// worker. A worker inventory is a snapshot from some moment; the metadata
// keeps changing around it. Comparing the inventory with the metadata as it
// is *after* the snapshot is unsafe: a copy claimed before the snapshot and
// written after it would look "missing" (DEC-057).
//
// The fix is a before/after rule. The caller first takes a WorkerSnapshot of
// the metadata, then fetches the inventory, then calls SweepInventory or
// AuditInventory, which re-check the current metadata:
//
//   - Missing: a replica row that was OK (of a READY object) *before* the
//     inventory was taken had its file written before then, so the file must
//     be in the inventory. If it is not, and the row is still the same OK row
//     (same updated_at), the file was lost. No legitimate operation removes
//     the file of an OK row without first removing the row (I8).
//   - Orphan: a file in the inventory whose (chunk, worker) had neither a
//     replica row nor a pending delete before the inventory was taken, and
//     still has neither afterwards. Every write is claimed before it starts
//     (D10), so such a file belongs to no operation.
//
// PENDING rows are never judged: an operation in progress owns them.

// SnapshotReplica is one replica row of a worker as seen before its
// inventory was fetched.
type SnapshotReplica struct {
	State     string
	UpdatedAt int64
	Size      int64 // the chunk's size from metadata
	Ready     bool  // the chunk's object was READY
}

// WorkerSnapshot is the metadata for one worker, read before its inventory.
type WorkerSnapshot struct {
	WorkerID       string
	Replicas       map[string]SnapshotReplica
	PendingDeletes map[string]bool
}

// SnapshotWorker reads every replica row and pending delete for workerID.
func (s *Store) SnapshotWorker(ctx context.Context, workerID string) (WorkerSnapshot, error) {
	snap := WorkerSnapshot{WorkerID: workerID, Replicas: map[string]SnapshotReplica{}, PendingDeletes: map[string]bool{}}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.chunk_id, r.state, r.updated_at, c.size, o.status = 'READY'
		FROM replicas r
		JOIN chunks c ON c.chunk_id = r.chunk_id
		JOIN objects o ON o.object_id = c.object_id
		WHERE r.worker_id = ?`, workerID)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var id string
		var r SnapshotReplica
		if err := rows.Scan(&id, &r.State, &r.UpdatedAt, &r.Size, &r.Ready); err != nil {
			rows.Close()
			return snap, err
		}
		snap.Replicas[id] = r
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	if err := rows.Err(); err != nil {
		return snap, err
	}
	pd, err := s.db.QueryContext(ctx, `SELECT chunk_id FROM pending_deletes WHERE worker_id = ?`, workerID)
	if err != nil {
		return snap, err
	}
	for pd.Next() {
		var id string
		if err := pd.Scan(&id); err != nil {
			pd.Close()
			return snap, err
		}
		snap.PendingDeletes[id] = true
	}
	if err := pd.Close(); err != nil {
		return snap, err
	}
	return snap, pd.Err()
}

// inventoryFinding is one discrepancy between a snapshot and an inventory,
// still to be re-checked against the current metadata.
type inventoryFinding struct {
	kind      int // findMissing, findSize, findOrphan
	chunkID   string
	updatedAt int64
}

const (
	findMissing = iota
	findSize
	findOrphan
)

func compareInventory(before WorkerSnapshot, inventory []InventoryEntry) []inventoryFinding {
	onDisk := make(map[string]int64, len(inventory))
	for _, e := range inventory {
		onDisk[e.ChunkID] = e.Size
	}
	var out []inventoryFinding
	for id, r := range before.Replicas {
		if r.State != ReplicaOK || !r.Ready {
			continue
		}
		size, present := onDisk[id]
		switch {
		case !present:
			out = append(out, inventoryFinding{kind: findMissing, chunkID: id, updatedAt: r.UpdatedAt})
		case size != r.Size:
			out = append(out, inventoryFinding{kind: findSize, chunkID: id, updatedAt: r.UpdatedAt})
		}
	}
	for _, e := range inventory {
		if _, row := before.Replicas[e.ChunkID]; row || before.PendingDeletes[e.ChunkID] {
			continue
		}
		if !ids.ValidChunkID(e.ChunkID) {
			continue
		}
		out = append(out, inventoryFinding{kind: findOrphan, chunkID: e.ChunkID})
	}
	return out
}

// SweepResult counts what SweepInventory did.
type SweepResult struct {
	MissingMarked int // OK copy whose file is gone: marked CORRUPT (repair rewrites it)
	SizeMismatch  int // OK copy with the wrong file size: marked CORRUPT
	OrphansQueued int // file claimed by nothing: pending delete
}

// SweepInventory applies the before/after rule (see the top of this file) to
// one live worker. Lost or damaged OK copies become CORRUPT, which the repair
// loop rewrites in place without grace; orphans become pending deletes.
// Every write re-checks its condition in the same statement, so a row that
// changed after the snapshot is left alone. Safe to run repeatedly.
func (s *Store) SweepInventory(ctx context.Context, before WorkerSnapshot, inventory []InventoryEntry) (SweepResult, error) {
	var res SweepResult
	findings := compareInventory(before, inventory)
	for start := 0; start < len(findings); start += reconcileBatch {
		batch := findings[start:min(start+reconcileBatch, len(findings))]
		var r SweepResult
		now := s.nowNanos()
		err := s.tx(ctx, func(tx *sql.Tx) error {
			r = SweepResult{}
			for _, f := range batch {
				var q string
				var args []any
				if f.kind == findOrphan {
					q = `INSERT INTO pending_deletes (chunk_id, worker_id, object_id, reason, created_at)
					     SELECT ?, ?, ?, 'orphan', ?
					     WHERE NOT EXISTS (SELECT 1 FROM replicas WHERE chunk_id = ? AND worker_id = ?)
					     ON CONFLICT (chunk_id, worker_id) DO NOTHING`
					args = []any{f.chunkID, before.WorkerID, f.chunkID[:36], now, f.chunkID, before.WorkerID}
				} else {
					q = `UPDATE replicas SET state = 'CORRUPT', updated_at = ?
					     WHERE chunk_id = ? AND worker_id = ? AND state = 'OK' AND updated_at = ?
					       AND EXISTS (SELECT 1 FROM chunks c JOIN objects o ON o.object_id = c.object_id
					                   WHERE c.chunk_id = replicas.chunk_id AND o.status = 'READY')`
					args = []any{now, f.chunkID, before.WorkerID, f.updatedAt}
				}
				out, err := tx.ExecContext(ctx, q, args...)
				if err != nil {
					return err
				}
				if n, _ := out.RowsAffected(); n == 1 {
					switch f.kind {
					case findMissing:
						r.MissingMarked++
					case findSize:
						r.SizeMismatch++
					case findOrphan:
						r.OrphansQueued++
					}
				}
			}
			return nil
		})
		if err != nil {
			return res, err
		}
		res.MissingMarked += r.MissingMarked
		res.SizeMismatch += r.SizeMismatch
		res.OrphansQueued += r.OrphansQueued
	}
	return res, nil
}

// WorkerInventory is one live worker's snapshot and inventory for the audit.
// Err is set (and Inventory ignored) when the inventory could not be fetched.
type WorkerInventory struct {
	Before    WorkerSnapshot
	Inventory []InventoryEntry
	Err       error
}

// AuditInventory checks I4 on live workers with the before/after rule. It
// reports files claimed by nothing (orphans) and OK copies whose file is
// gone or has the wrong size. The design states I4 as holding "after the
// janitor has run" (architecture §3.4), so it is a steady_state check: an
// orphan left by a late PUT exists until the next sweep. Workers whose
// inventory could not be fetched are listed as unchecked, not as violations.
func (s *Store) AuditInventory(ctx context.Context, workers []WorkerInventory) (AuditCheck, error) {
	check := AuditCheck{ID: "I4", Description: "every chunk file on an ALIVE worker is claimed by a replica row or a pending delete, and every OK copy's file is present with the right size",
		Kind: "steady_state", Examples: []string{}}
	for _, w := range workers {
		if w.Err != nil {
			check.UncheckedWorkers = append(check.UncheckedWorkers, w.Before.WorkerID)
			continue
		}
		for _, f := range compareInventory(w.Before, w.Inventory) {
			var still int
			var err error
			if f.kind == findOrphan {
				err = s.db.QueryRowContext(ctx, `
					SELECT (SELECT COUNT(*) FROM replicas WHERE chunk_id = ? AND worker_id = ?)
					     + (SELECT COUNT(*) FROM pending_deletes WHERE chunk_id = ? AND worker_id = ?)`,
					f.chunkID, w.Before.WorkerID, f.chunkID, w.Before.WorkerID).Scan(&still)
				still = boolInt(err == nil && still == 0)
			} else {
				err = s.db.QueryRowContext(ctx, `
					SELECT COUNT(*) FROM replicas WHERE chunk_id = ? AND worker_id = ? AND state = 'OK' AND updated_at = ?`,
					f.chunkID, w.Before.WorkerID, f.updatedAt).Scan(&still)
			}
			if err != nil {
				return check, err
			}
			if still == 0 {
				continue
			}
			switch f.kind {
			case findOrphan:
				checkViolation(&check, f.chunkID+" orphan on "+w.Before.WorkerID)
			case findMissing:
				checkViolation(&check, f.chunkID+" missing on "+w.Before.WorkerID)
			case findSize:
				checkViolation(&check, f.chunkID+" wrong size on "+w.Before.WorkerID)
			}
		}
	}
	return check, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
