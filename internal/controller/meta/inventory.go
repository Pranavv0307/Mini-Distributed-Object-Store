package meta

import (
	"context"
	"database/sql"
)

// InventoryEntry is one chunk file reported by a worker: payload bytes on
// disk (file size minus header), from file metadata only.
type InventoryEntry struct {
	ChunkID string
	Size    int64
}

// ReconcileResult counts what ReconcileInventory found and did.
type ReconcileResult struct {
	Matched        int // OK row, file present with the right size
	MissingRemoved int // OK/CORRUPT row whose file is gone: row removed
	SizeMismatch   int // OK row, file present with the wrong size: marked CORRUPT
	CorruptKept    int // CORRUPT row, file present: left for repair
	PendingKept    int // PENDING row: owned by an operation in progress
	OrphansQueued  int // file with no row for (chunk, worker): pending delete
	Skipped        int // planned action skipped because the row changed meanwhile
}

// reconcileBatch bounds the work per transaction so a large inventory never
// holds the write lock for long (architecture §3.5).
const reconcileBatch = 500

type reconcileAction struct {
	kind     int // actRemove, actCorrupt, actOrphan
	chunkID  string
	objectID string
	state    string // the state observed when planning (for conditional writes)
}

const (
	actRemove = iota
	actCorrupt
	actOrphan
)

// ReconcileInventory compares a registering worker's inventory with the
// metadata (architecture §4.5) and applies the reconciliation table:
//
//	file present, OK row, size matches       → nothing
//	OK or CORRUPT row, file missing           → remove the row (under-replicated)
//	OK row, file size differs                 → mark CORRUPT
//	CORRUPT row, file present                 → nothing (repair rewrites it)
//	PENDING row                               → nothing (an operation in progress owns it)
//	file present, no row for (chunk, worker)  → orphan: pending delete
//
// It runs while the worker is SYNCING, so no new claim can target it. Plans
// are made from one read; every write re-checks the state it planned on, so a
// row changed concurrently is skipped rather than overwritten. The orphan
// insert checks "no replica row" in the same statement, which together with
// the claim-side check keeps invariant I8. Safe to run repeatedly.
func (s *Store) ReconcileInventory(ctx context.Context, workerID string, inventory []InventoryEntry) (ReconcileResult, error) {
	var res ReconcileResult
	type rowInfo struct {
		state, objectID string
		size            int64
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.chunk_id, r.state, c.size, c.object_id
		FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id
		WHERE r.worker_id = ?`, workerID)
	if err != nil {
		return res, err
	}
	known := map[string]rowInfo{}
	for rows.Next() {
		var id string
		var ri rowInfo
		if err := rows.Scan(&id, &ri.state, &ri.size, &ri.objectID); err != nil {
			rows.Close()
			return res, err
		}
		known[id] = ri
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}

	onDisk := make(map[string]int64, len(inventory))
	for _, e := range inventory {
		onDisk[e.ChunkID] = e.Size
	}

	var actions []reconcileAction
	for id, ri := range known {
		size, present := onDisk[id]
		switch {
		case ri.state == ReplicaPending:
			res.PendingKept++
		case !present:
			actions = append(actions, reconcileAction{kind: actRemove, chunkID: id, state: ri.state})
		case ri.state == ReplicaOK && size != ri.size:
			actions = append(actions, reconcileAction{kind: actCorrupt, chunkID: id, state: ri.state})
		case ri.state == ReplicaCorrupt:
			res.CorruptKept++
		default:
			res.Matched++
		}
	}
	for _, e := range inventory {
		if _, ok := known[e.ChunkID]; !ok {
			// The chunk ID's first 36 characters are the object ID (format
			// validated at registration).
			actions = append(actions, reconcileAction{kind: actOrphan, chunkID: e.ChunkID, objectID: e.ChunkID[:36]})
		}
	}

	for start := 0; start < len(actions); start += reconcileBatch {
		batch := actions[start:min(start+reconcileBatch, len(actions))]
		var r ReconcileResult
		now := s.nowNanos()
		err := s.tx(ctx, func(tx *sql.Tx) error {
			r = ReconcileResult{}
			for _, a := range batch {
				var q string
				var args []any
				switch a.kind {
				case actRemove:
					q = `DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ? AND state = ?`
					args = []any{a.chunkID, workerID, a.state}
				case actCorrupt:
					q = `UPDATE replicas SET state = 'CORRUPT', updated_at = ? WHERE chunk_id = ? AND worker_id = ? AND state = 'OK'`
					args = []any{now, a.chunkID, workerID}
				case actOrphan:
					q = `INSERT INTO pending_deletes (chunk_id, worker_id, object_id, reason, created_at)
					     SELECT ?, ?, ?, 'orphan', ?
					     WHERE NOT EXISTS (SELECT 1 FROM replicas WHERE chunk_id = ? AND worker_id = ?)
					     ON CONFLICT (chunk_id, worker_id) DO NOTHING`
					args = []any{a.chunkID, workerID, a.objectID, now, a.chunkID, workerID}
				}
				out, err := tx.ExecContext(ctx, q, args...)
				if err != nil {
					return err
				}
				n, _ := out.RowsAffected()
				switch {
				case n == 0 && a.kind == actOrphan:
					// Already pending (e.g. queued by a failed write or a
					// delete): nothing new to do, not a skip.
				case n == 0:
					r.Skipped++
				case a.kind == actRemove:
					r.MissingRemoved++
				case a.kind == actCorrupt:
					r.SizeMismatch++
				case a.kind == actOrphan:
					r.OrphansQueued++
				}
			}
			return nil
		})
		if err != nil {
			return res, err
		}
		res.MissingRemoved += r.MissingRemoved
		res.SizeMismatch += r.SizeMismatch
		res.OrphansQueued += r.OrphansQueued
		res.Skipped += r.Skipped
	}
	return res, nil
}
