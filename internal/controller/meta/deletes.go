package meta

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// PendingDelete is a durable request to delete one chunk file from one
// worker (DEC-007).
type PendingDelete struct {
	ChunkID   string
	WorkerID  string
	ObjectID  string
	Reason    string
	Attempts  int
	CreatedAt time.Time
}

// DuePendingDeletes returns up to limit entries on the given workers whose
// next attempt is due, oldest first. Filtering by worker (the janitor passes
// the ALIVE ones) keeps entries for down workers from starving the rest.
func (s *Store) DuePendingDeletes(ctx context.Context, workers []string, limit int) ([]PendingDelete, error) {
	if len(workers) == 0 {
		return nil, nil
	}
	args := []any{s.nowNanos()}
	for _, w := range workers {
		args = append(args, w)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT chunk_id, worker_id, object_id, reason, attempts, created_at
		FROM pending_deletes
		WHERE next_attempt_at <= ? AND worker_id IN (?`+strings.Repeat(", ?", len(workers)-1)+`)
		ORDER BY created_at, chunk_id, worker_id LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingDelete
	for rows.Next() {
		var p PendingDelete
		var created int64
		if err := rows.Scan(&p.ChunkID, &p.WorkerID, &p.ObjectID, &p.Reason, &p.Attempts, &created); err != nil {
			return nil, err
		}
		p.CreatedAt = fromNanos(created)
		out = append(out, p)
	}
	return out, rows.Err()
}

// CompletePendingDelete removes an entry after the worker confirmed the
// delete (including "already absent").
func (s *Store) CompletePendingDelete(ctx context.Context, chunkID, workerID string) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM pending_deletes WHERE chunk_id = ? AND worker_id = ?`, chunkID, workerID)
		return err
	})
}

// FailPendingDelete records a failed attempt and schedules the next one.
func (s *Store) FailPendingDelete(ctx context.Context, chunkID, workerID, errMsg string, retryAfter time.Duration) error {
	next := s.now().Add(retryAfter).UnixNano()
	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE pending_deletes SET attempts = attempts + 1, last_error = ?, next_attempt_at = ?
			WHERE chunk_id = ? AND worker_id = ?`, errMsg, next, chunkID, workerID)
		return err
	})
}

// FinalizeDeletedObjects removes DELETING objects (and, by cascade, their
// chunk rows) once no replica row and no pending delete refers to them, i.e.
// every copy has been confirmed gone (§4.4 step 4). It returns the removed
// object IDs.
func (s *Store) FinalizeDeletedObjects(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT o.object_id FROM objects o
			WHERE o.status = 'DELETING'
			  AND NOT EXISTS (SELECT 1 FROM pending_deletes p WHERE p.object_id = o.object_id)
			  AND NOT EXISTS (SELECT 1 FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id
			                  WHERE c.object_id = o.object_id)`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE object_id = ? AND status = 'DELETING'`, id); err != nil {
				return err
			}
		}
		return nil
	})
	return ids, err
}

// PendingDeletesFor lists the pending deletes of one object (tests and
// diagnostics).
func (s *Store) PendingDeletesFor(ctx context.Context, objectID string) ([]PendingDelete, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT chunk_id, worker_id, object_id, reason, attempts, created_at
		FROM pending_deletes WHERE object_id = ? ORDER BY chunk_id, worker_id`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingDelete
	for rows.Next() {
		var p PendingDelete
		var created int64
		if err := rows.Scan(&p.ChunkID, &p.WorkerID, &p.ObjectID, &p.Reason, &p.Attempts, &created); err != nil {
			return nil, err
		}
		p.CreatedAt = fromNanos(created)
		out = append(out, p)
	}
	return out, rows.Err()
}

// Counts summarises table sizes (tests and /cluster).
type Counts struct {
	ObjectsByStatus map[string]int
	Chunks          int
	Replicas        int
	PendingDeletes  int
}

// Counts returns row counts.
func (s *Store) Counts(ctx context.Context) (Counts, error) {
	c := Counts{ObjectsByStatus: map[string]int{}}
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM objects GROUP BY status`)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return c, err
		}
		c.ObjectsByStatus[st] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}
	err = s.db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM chunks), (SELECT COUNT(*) FROM replicas), (SELECT COUNT(*) FROM pending_deletes)`).
		Scan(&c.Chunks, &c.Replicas, &c.PendingDeletes)
	return c, err
}
