package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

// Object statuses (architecture §3.3).
const (
	StatusUploading = "UPLOADING"
	StatusReady     = "READY"
	StatusDeleting  = "DELETING"
)

// Replica states.
const (
	ReplicaPending = "PENDING"
	ReplicaOK      = "OK"
	ReplicaCorrupt = "CORRUPT"
)

// Object is an object row.
type Object struct {
	ID              string
	Name            string
	Size            int64
	ContentType     string
	ChecksumSHA256  string
	NumChunks       int
	Status          string
	CreatedAt       time.Time
	ModifiedAt      time.Time
	LastActivity    time.Time
	MetadataVersion int64
	CustomMetadata  map[string]string
}

// Chunk is a chunk row.
type Chunk struct {
	ID             string
	ObjectID       string
	Idx            int
	Size           int64
	ChecksumSHA256 string
}

// Replica is a replica row.
type Replica struct {
	ChunkID  string
	WorkerID string
	State    string
}

// CreateObject inserts a new object in state UPLOADING (architecture §4.1
// step 2). The object is invisible to clients until CommitObject.
func (s *Store) CreateObject(ctx context.Context, id, name, contentType string) (Object, error) {
	now := s.nowNanos()
	err := s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO objects (object_id, name, content_type, status, created_at, modified_at, last_activity)
			VALUES (?, ?, ?, 'UPLOADING', ?, ?, ?)`, id, name, contentType, now, now, now)
		return err
	})
	if err != nil {
		return Object{}, fmt.Errorf("meta: create object %s: %w", id, err)
	}
	return Object{ID: id, Name: name, ContentType: contentType, Status: StatusUploading,
		CreatedAt: fromNanos(now), ModifiedAt: fromNanos(now), LastActivity: fromNanos(now),
		MetadataVersion: 1, CustomMetadata: map[string]string{}}, nil
}

// touchUploading bumps last_activity and fails with ErrNotUploading unless
// the object is UPLOADING. Every upload-side write calls it first, inside its
// own transaction, which is what makes claims and commits conditional on the
// object's status (DEC-011).
func touchUploading(ctx context.Context, tx *sql.Tx, objectID string, now int64) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE objects SET last_activity = ? WHERE object_id = ? AND status = 'UPLOADING'`, now, objectID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return ErrNotUploading
	}
	return nil
}

func hasPendingDelete(ctx context.Context, tx *sql.Tx, chunkID, workerID string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM pending_deletes WHERE chunk_id = ? AND worker_id = ?`, chunkID, workerID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ClaimChunk records a chunk and one PENDING replica per worker in one
// transaction, before any byte is sent (claim-before-write, D10). It fails
// with ErrNotUploading if the upload was aborted, and with ErrPendingDelete if
// any worker has a pending delete for this chunk (I8).
func (s *Store) ClaimChunk(ctx context.Context, c Chunk, workers []string) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := touchUploading(ctx, tx, c.ObjectID, now); err != nil {
			return err
		}
		for _, w := range workers {
			if pd, err := hasPendingDelete(ctx, tx, c.ID, w); err != nil {
				return err
			} else if pd {
				return fmt.Errorf("%w: chunk %s on %s", ErrPendingDelete, c.ID, w)
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO chunks (chunk_id, object_id, idx, size, checksum_sha256) VALUES (?, ?, ?, ?, ?)`,
			c.ID, c.ObjectID, c.Idx, c.Size, c.ChecksumSHA256); err != nil {
			return fmt.Errorf("meta: insert chunk %s: %w", c.ID, err)
		}
		for _, w := range workers {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO replicas (chunk_id, worker_id, state, updated_at) VALUES (?, ?, 'PENDING', ?)`,
				c.ID, w, now); err != nil {
				return fmt.Errorf("meta: insert replica %s@%s: %w", c.ID, w, err)
			}
		}
		return nil
	})
}

// MarkReplicaOK records that workerID acknowledged chunkID (§4.1 step 7). It
// fails with ErrNotUploading if the upload was aborted and ErrClaimLost if
// the PENDING row no longer exists.
func (s *Store) MarkReplicaOK(ctx context.Context, objectID, chunkID, workerID string) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := touchUploading(ctx, tx, objectID, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE replicas SET state = 'OK', updated_at = ?
			WHERE chunk_id = ? AND worker_id = ? AND state = 'PENDING'`, now, chunkID, workerID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: %s@%s", ErrClaimLost, chunkID, workerID)
		}
		return nil
	})
}

// ReplaceReplica handles a failed (or timed-out) replica write during an
// upload, atomically (DEC-007): the PENDING claim on failedWorker becomes a
// pending delete (its bytes may or may not have landed; a timeout is not
// proof of failure), and a new PENDING claim is recorded on newWorker.
func (s *Store) ReplaceReplica(ctx context.Context, objectID, chunkID, failedWorker, newWorker, reason string) error {
	now := s.nowNanos()
	return s.tx(ctx, func(tx *sql.Tx) error {
		if err := touchUploading(ctx, tx, objectID, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ? AND state = 'PENDING'`, chunkID, failedWorker)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: %s@%s", ErrClaimLost, chunkID, failedWorker)
		}
		if err := insertPendingDelete(ctx, tx, chunkID, failedWorker, objectID, reason, now); err != nil {
			return err
		}
		if pd, err := hasPendingDelete(ctx, tx, chunkID, newWorker); err != nil {
			return err
		} else if pd {
			return fmt.Errorf("%w: chunk %s on %s", ErrPendingDelete, chunkID, newWorker)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO replicas (chunk_id, worker_id, state, updated_at) VALUES (?, ?, 'PENDING', ?)`,
			chunkID, newWorker, now)
		return err
	})
}

func insertPendingDelete(ctx context.Context, tx *sql.Tx, chunkID, workerID, objectID, reason string, now int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO pending_deletes (chunk_id, worker_id, object_id, reason, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (chunk_id, worker_id) DO NOTHING`, chunkID, workerID, objectID, reason, now)
	return err
}

// CommitInput is what CommitObject records.
type CommitInput struct {
	ObjectID          string
	Size              int64
	NumChunks         int
	ChecksumSHA256    string
	CustomMetadata    map[string]string
	ReplicationFactor int
}

// CommitObject makes an upload visible: UPLOADING → READY in one transaction
// (§4.1 step 9). Inside the same transaction it re-verifies, from the rows
// themselves, that chunk indexes are exactly 0..NumChunks-1 (I1), sizes sum
// to Size (I2), and every chunk has at least ReplicationFactor OK replicas
// and no other replicas. A violation returns ErrIncomplete and changes
// nothing; a non-UPLOADING object returns ErrNotUploading.
func (s *Store) CommitObject(ctx context.Context, in CommitInput) (Object, error) {
	md, err := json.Marshal(nonNil(in.CustomMetadata))
	if err != nil {
		return Object{}, fmt.Errorf("meta: encode metadata: %w", err)
	}
	now := s.nowNanos()
	err = s.tx(ctx, func(tx *sql.Tx) error {
		if err := touchUploading(ctx, tx, in.ObjectID, now); err != nil {
			return err
		}
		var count, minIdx, maxIdx, total int64
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*), COALESCE(MIN(idx), 0), COALESCE(MAX(idx), -1), COALESCE(SUM(size), 0)
			FROM chunks WHERE object_id = ?`, in.ObjectID).Scan(&count, &minIdx, &maxIdx, &total); err != nil {
			return err
		}
		if count != int64(in.NumChunks) || (count > 0 && (minIdx != 0 || maxIdx != count-1)) {
			return fmt.Errorf("%w: %d chunk rows with idx %d..%d, want %d", ErrIncomplete, count, minIdx, maxIdx, in.NumChunks)
		}
		if total != in.Size {
			return fmt.Errorf("%w: chunk sizes sum to %d, object size %d", ErrIncomplete, total, in.Size)
		}
		var bad int64
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM chunks c
			WHERE c.object_id = ? AND (
				(SELECT COUNT(*) FROM replicas r WHERE r.chunk_id = c.chunk_id AND r.state = 'OK') < ?
				OR EXISTS (SELECT 1 FROM replicas r WHERE r.chunk_id = c.chunk_id AND r.state <> 'OK'))`,
			in.ObjectID, in.ReplicationFactor).Scan(&bad); err != nil {
			return err
		}
		if bad != 0 {
			return fmt.Errorf("%w: %d chunks not fully replicated", ErrIncomplete, bad)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE objects SET status = 'READY', size = ?, num_chunks = ?, checksum_sha256 = ?,
			       custom_metadata = ?, modified_at = ?
			WHERE object_id = ? AND status = 'UPLOADING'`,
			in.Size, in.NumChunks, in.ChecksumSHA256, string(md), now, in.ObjectID)
		return err
	})
	if err != nil {
		return Object{}, err
	}
	return s.GetObject(ctx, in.ObjectID)
}

// MarkDeleting moves an object from one of fromStatuses to DELETING and turns
// all of its replica rows into pending deletes, in one transaction. This is
// both the upload abort (from UPLOADING) and, from Phase 5, client delete
// (from READY). It reports whether the object was transitioned (false if it
// does not exist or is in another state; repeating it is harmless).
func (s *Store) MarkDeleting(ctx context.Context, objectID, reason string, fromStatuses ...string) (bool, error) {
	if len(fromStatuses) == 0 {
		return false, errors.New("meta: MarkDeleting needs at least one source status")
	}
	now := s.nowNanos()
	moved := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		args := []any{now, objectID}
		for _, st := range fromStatuses {
			args = append(args, st)
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE objects SET status = 'DELETING', last_activity = ?
			WHERE object_id = ? AND status IN (?`+strings.Repeat(", ?", len(fromStatuses)-1)+`)`, args...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil
		}
		moved = true
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO pending_deletes (chunk_id, worker_id, object_id, reason, created_at)
			SELECT r.chunk_id, r.worker_id, c.object_id, ?, ?
			FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id
			WHERE c.object_id = ?
			ON CONFLICT (chunk_id, worker_id) DO NOTHING`, reason, now, objectID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id = ?)`, objectID)
		return err
	})
	return moved, err
}

// GetObject returns an object in any status.
func (s *Store) GetObject(ctx context.Context, id string) (Object, error) {
	var o Object
	var created, modified, activity int64
	var md string
	err := s.db.QueryRowContext(ctx, `
		SELECT object_id, name, size, content_type, checksum_sha256, num_chunks, status,
		       created_at, modified_at, last_activity, metadata_version, custom_metadata
		FROM objects WHERE object_id = ?`, id).Scan(
		&o.ID, &o.Name, &o.Size, &o.ContentType, &o.ChecksumSHA256, &o.NumChunks, &o.Status,
		&created, &modified, &activity, &o.MetadataVersion, &md)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, fmt.Errorf("%w: object %s", ErrNotFound, id)
	}
	if err != nil {
		return Object{}, fmt.Errorf("meta: get object %s: %w", id, err)
	}
	o.CreatedAt, o.ModifiedAt, o.LastActivity = fromNanos(created), fromNanos(modified), fromNanos(activity)
	if err := json.Unmarshal([]byte(md), &o.CustomMetadata); err != nil {
		return Object{}, fmt.Errorf("meta: object %s: decode metadata: %w", id, err)
	}
	return o, nil
}

// ChunkWithReplicas is one entry of an object's chunk map.
type ChunkWithReplicas struct {
	Chunk
	Replicas []Replica
}

// ChunkMap returns an object's chunks ordered by idx, each with its replicas
// ordered by worker ID.
func (s *Store) ChunkMap(ctx context.Context, objectID string) ([]ChunkWithReplicas, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.chunk_id, c.idx, c.size, c.checksum_sha256, r.worker_id, r.state
		FROM chunks c LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
		WHERE c.object_id = ?
		ORDER BY c.idx, r.worker_id`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkWithReplicas
	for rows.Next() {
		var c Chunk
		var worker, state sql.NullString
		if err := rows.Scan(&c.ID, &c.Idx, &c.Size, &c.ChecksumSHA256, &worker, &state); err != nil {
			return nil, err
		}
		c.ObjectID = objectID
		if len(out) == 0 || out[len(out)-1].ID != c.ID {
			out = append(out, ChunkWithReplicas{Chunk: c})
		}
		if worker.Valid {
			last := &out[len(out)-1]
			last.Replicas = append(last.Replicas, Replica{ChunkID: c.ID, WorkerID: worker.String, State: state.String})
		}
	}
	return out, rows.Err()
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// MarkReplicaCorrupt records that a replica failed verification against the
// controller's checksum (architecture §4.9, detection path 1). Only an OK
// replica changes (a PENDING or already CORRUPT one is left alone); it reports
// whether the row changed. The controller's metadata checksum is the
// authority (§3.6), so its own verification needs no further cross-check.
func (s *Store) MarkReplicaCorrupt(ctx context.Context, chunkID, workerID string) (bool, error) {
	now := s.nowNanos()
	changed := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE replicas SET state = 'CORRUPT', updated_at = ?
			WHERE chunk_id = ? AND worker_id = ? AND state = 'OK'`, now, chunkID, workerID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		changed = n == 1
		return err
	})
	return changed, err
}

// ErrVersionConflict: the caller's expected metadata_version does not match
// (If-Match, DEC-014).
var ErrVersionConflict = errors.New("meta: metadata version conflict")

// UpdateMetadata applies fn to a READY object's custom metadata in one
// transaction (read, merge, write), so concurrent updates are serialised and
// none is lost. fn must be pure (no I/O): it receives a copy of the current
// map and returns the new one or an error, which aborts the update.
//
// If ifVersion is non-nil and differs from the current metadata_version, the
// update fails with ErrVersionConflict. If fn's result equals the current
// metadata, nothing is written (the version and modified_at are unchanged),
// so repeating a PATCH is idempotent. Chunks and replicas are never touched.
func (s *Store) UpdateMetadata(ctx context.Context, objectID string, ifVersion *int64,
	fn func(current map[string]string) (map[string]string, error)) (Object, error) {
	now := s.nowNanos()
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var status, raw string
		var version int64
		err := tx.QueryRowContext(ctx,
			`SELECT status, custom_metadata, metadata_version FROM objects WHERE object_id = ?`, objectID).
			Scan(&status, &raw, &version)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && status != StatusReady) {
			return fmt.Errorf("%w: object %s", ErrNotFound, objectID)
		}
		if err != nil {
			return err
		}
		if ifVersion != nil && *ifVersion != version {
			return fmt.Errorf("%w: current version %d, expected %d", ErrVersionConflict, version, *ifVersion)
		}
		var current map[string]string
		if err := json.Unmarshal([]byte(raw), &current); err != nil {
			return fmt.Errorf("meta: object %s: decode metadata: %w", objectID, err)
		}
		next, err := fn(maps.Clone(nonNil(current)))
		if err != nil {
			return err
		}
		if maps.Equal(nonNil(current), nonNil(next)) {
			return nil // no change: idempotent
		}
		enc, err := json.Marshal(nonNil(next))
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE objects SET custom_metadata = ?, metadata_version = metadata_version + 1, modified_at = ?
			WHERE object_id = ? AND status = 'READY'`, string(enc), now, objectID)
		return err
	})
	if err != nil {
		return Object{}, err
	}
	return s.GetObject(ctx, objectID)
}

// ListCursor is the keyset position after which a listing continues.
type ListCursor struct {
	CreatedAt int64 // Unix nanoseconds
	ObjectID  string
}

// ListObjects returns up to limit READY objects ordered by (created_at,
// object_id), starting after the cursor (nil = from the beginning), and the
// cursor for the next page (nil if this is the last page). Keyset pagination
// never returns an object twice and never skips one that existed at both
// page reads.
func (s *Store) ListObjects(ctx context.Context, limit int, after *ListCursor) ([]Object, *ListCursor, error) {
	q := `SELECT object_id, name, size, content_type, checksum_sha256, num_chunks, status,
	             created_at, modified_at, last_activity, metadata_version, custom_metadata
	      FROM objects WHERE status = 'READY'`
	args := []any{}
	if after != nil {
		q += ` AND (created_at > ? OR (created_at = ? AND object_id > ?))`
		args = append(args, after.CreatedAt, after.CreatedAt, after.ObjectID)
	}
	q += ` ORDER BY created_at, object_id LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var o Object
		var created, modified, activity int64
		var md string
		if err := rows.Scan(&o.ID, &o.Name, &o.Size, &o.ContentType, &o.ChecksumSHA256, &o.NumChunks, &o.Status,
			&created, &modified, &activity, &o.MetadataVersion, &md); err != nil {
			return nil, nil, err
		}
		o.CreatedAt, o.ModifiedAt, o.LastActivity = fromNanos(created), fromNanos(modified), fromNanos(activity)
		if err := json.Unmarshal([]byte(md), &o.CustomMetadata); err != nil {
			return nil, nil, err
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	var next *ListCursor
	if len(out) > limit {
		out = out[:limit]
		last := out[limit-1]
		next = &ListCursor{CreatedAt: last.CreatedAt.UnixNano(), ObjectID: last.ID}
	}
	return out, next, nil
}
