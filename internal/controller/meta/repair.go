package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrReplicaExists: a repair claim was refused because the target already
// has a replica row for the chunk (invariant I3).
var ErrReplicaExists = errors.New("meta: target already holds a replica row for the chunk")

// RepairCandidates returns, for READY objects, every chunk that is not in
// the simple healthy shape "exactly rf OK rows, all on ALIVE workers, and no
// other rows". The repair loop classifies these further with the registry
// (grace periods etc.). At most limit chunks are returned, ordered by object
// and index so repeated rounds make steady progress.
func (s *Store) RepairCandidates(ctx context.Context, alive []string, rf, limit int) ([]ChunkWithReplicas, error) {
	in, args := inList(alive)
	q := `
		SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum_sha256
		FROM chunks c JOIN objects o ON o.object_id = c.object_id
		WHERE o.status = 'READY' AND (
			(SELECT COUNT(*) FROM replicas r WHERE r.chunk_id = c.chunk_id AND r.state = 'OK' AND r.worker_id IN ` + in + `) <> ?
			OR EXISTS (SELECT 1 FROM replicas r WHERE r.chunk_id = c.chunk_id AND (r.state <> 'OK' OR r.worker_id NOT IN ` + in + `)))
		ORDER BY c.object_id, c.idx
		LIMIT ?`
	all := append(append(append(append([]any{}, args...), rf), args...), limit)
	rows, err := s.db.QueryContext(ctx, q, all...)
	if err != nil {
		return nil, err
	}
	var out []ChunkWithReplicas
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.ID, &c.ObjectID, &c.Idx, &c.Size, &c.ChecksumSHA256); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, ChunkWithReplicas{Chunk: c})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		reps, err := s.replicasOf(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Replicas = reps
	}
	return out, nil
}

func (s *Store) replicasOf(ctx context.Context, chunkID string) ([]Replica, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT worker_id, state FROM replicas WHERE chunk_id = ? ORDER BY worker_id`, chunkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Replica
	for rows.Next() {
		r := Replica{ChunkID: chunkID}
		if err := rows.Scan(&r.WorkerID, &r.State); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PendingDeleteWorkers lists the workers that have a pending delete for the
// chunk; repair never targets them (I8).
func (s *Store) PendingDeleteWorkers(ctx context.Context, chunkID string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id FROM pending_deletes WHERE chunk_id = ?`, chunkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var w string
		if err := rows.Scan(&w); err != nil {
			return nil, err
		}
		out[w] = true
	}
	return out, rows.Err()
}

func requireReady(ctx context.Context, tx *sql.Tx, objectID string) error {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM objects WHERE object_id = ?`, objectID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && status != StatusReady) {
		return fmt.Errorf("%w: object %s is not READY", ErrNotFound, objectID)
	}
	return err
}

// ClaimRepair records the intent to write a new copy of chunkID on target
// (claim before write, D10), in one transaction that requires the object to
// be READY and target to have no pending delete for the chunk. With inPlace
// the target's existing CORRUPT row becomes PENDING (DEC-008); otherwise a
// new PENDING row is inserted, which fails with ErrReplicaExists if target
// already has any row for the chunk.
func (s *Store) ClaimRepair(ctx context.Context, objectID, chunkID, target string, inPlace bool) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := requireReady(ctx, tx, objectID); err != nil {
			return err
		}
		if pd, err := hasPendingDelete(ctx, tx, chunkID, target); err != nil {
			return err
		} else if pd {
			return fmt.Errorf("%w: chunk %s on %s", ErrPendingDelete, chunkID, target)
		}
		if inPlace {
			res, err := tx.ExecContext(ctx, `
				UPDATE replicas SET state = 'PENDING', updated_at = ?
				WHERE chunk_id = ? AND worker_id = ? AND state = 'CORRUPT'`, now, chunkID, target)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return fmt.Errorf("%w: no CORRUPT replica of %s on %s", ErrClaimLost, chunkID, target)
			}
			return nil
		}
		var one int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM replicas WHERE chunk_id = ? AND worker_id = ?`, chunkID, target).Scan(&one)
		if err == nil {
			return fmt.Errorf("%w: %s on %s", ErrReplicaExists, chunkID, target)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO replicas (chunk_id, worker_id, state, updated_at) VALUES (?, ?, 'PENDING', ?)`, chunkID, target, now)
		return err
	})
}

// Supersede names a lost replica that a completed repair replaces.
type Supersede struct {
	WorkerID string
	State    string // the state it was observed in (the move is conditional on it)
}

// CompleteRepair marks the repaired copy OK and, in the same transaction,
// moves the superseded lost replica (if any, and only if still in the
// observed state) to pending_deletes, so its file is deleted when its worker
// is reachable. It fails with ErrNotFound if the object is no longer READY
// (deleted during the repair) and ErrClaimLost if the PENDING row is gone.
func (s *Store) CompleteRepair(ctx context.Context, objectID, chunkID, target string, sup *Supersede) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := requireReady(ctx, tx, objectID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE replicas SET state = 'OK', updated_at = ?
			WHERE chunk_id = ? AND worker_id = ? AND state = 'PENDING'`, now, chunkID, target)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: %s@%s", ErrClaimLost, chunkID, target)
		}
		if sup != nil && sup.WorkerID != target {
			if _, err := moveToPendingDelete(ctx, tx, objectID, chunkID, sup.WorkerID, sup.State, "superseded_by_repair", now); err != nil {
				return err
			}
		}
		return nil
	})
}

// AbandonRepair turns a failed repair's PENDING claim into a pending delete:
// the write may or may not have landed (DEC-007).
func (s *Store) AbandonRepair(ctx context.Context, objectID, chunkID, target, reason string) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := moveToPendingDelete(ctx, tx, objectID, chunkID, target, ReplicaPending, reason, now)
		return err
	})
}

// TrimReplica moves one replica row (if still in observedState) to
// pending_deletes, but only if afterwards the chunk still has at least rf OK
// replicas on the given ALIVE workers, so trimming can never reduce a chunk
// below the replication factor. It reports whether the row was moved.
func (s *Store) TrimReplica(ctx context.Context, objectID, chunkID, workerID, observedState, reason string, alive []string, rf int) (bool, error) {
	now := s.nowNanos()
	moved := false
	in, args := inList(alive)
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var remaining int
		q := `SELECT COUNT(*) FROM replicas WHERE chunk_id = ? AND state = 'OK' AND worker_id <> ? AND worker_id IN ` + in
		if err := tx.QueryRowContext(ctx, q, append([]any{chunkID, workerID}, args...)...).Scan(&remaining); err != nil {
			return err
		}
		if remaining < rf {
			return nil
		}
		var err error
		moved, err = moveToPendingDelete(ctx, tx, objectID, chunkID, workerID, observedState, reason, now)
		return err
	})
	return moved, err
}

// moveToPendingDelete deletes the replica row (if in state) and records a
// pending delete, atomically (DEC-007).
func moveToPendingDelete(ctx context.Context, tx *sql.Tx, objectID, chunkID, workerID, state, reason string, now int64) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ? AND state = ?`, chunkID, workerID, state)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, nil
	}
	return true, insertPendingDelete(ctx, tx, chunkID, workerID, objectID, reason, now)
}
