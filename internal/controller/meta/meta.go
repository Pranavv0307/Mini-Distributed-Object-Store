// Package meta is the controller's metadata store: the single source of truth
// for objects, chunks, replicas, and pending physical deletions
// (architecture §3, D7, DEC-003).
//
// Concurrency rules (architecture §3.5):
//   - Every write is one short transaction started with BEGIN IMMEDIATE
//     (DSN _txlock=immediate), so writers queue on SQLite's write lock up
//     front instead of failing with SQLITE_BUSY on a read→write upgrade.
//   - No transaction is ever held across a network call. Callers do network
//     work between transactions, and every state change that depends on a
//     prior read re-checks its precondition inside its own transaction
//     (conditional UPDATEs; "rows affected" decides the outcome).
package meta

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

//go:embed schema.sql
var schemaV1 string

// Errors returned (possibly wrapped) by Store methods.
var (
	// ErrNotFound: no such object.
	ErrNotFound = errors.New("meta: not found")
	// ErrNotUploading: the object is no longer UPLOADING (it was aborted or
	// committed), so the caller's upload must stop.
	ErrNotUploading = errors.New("meta: object is not UPLOADING")
	// ErrClaimLost: the PENDING replica row the caller expected is gone
	// (moved to pending_deletes by an abort).
	ErrClaimLost = errors.New("meta: replica claim no longer exists")
	// ErrPendingDelete: a claim was refused because the (chunk, worker) pair
	// has a pending physical deletion (invariant I8, DEC-007).
	ErrPendingDelete = errors.New("meta: chunk has a pending delete on that worker")
	// ErrIncomplete: commit refused because the chunks/replicas do not satisfy
	// invariants I1, I2, and full replication.
	ErrIncomplete = errors.New("meta: object is incomplete")
)

// Store wraps the SQLite database.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Options configures Open.
type Options struct {
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

// Open opens (creating if necessary) the database at path and applies the
// schema. The connection settings are DEC-003: WAL journal, FULL sync
// (a commit is durable when it returns), foreign keys, a busy timeout, and
// immediate-lock write transactions.
func Open(path string, opts Options) (*Store, error) {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(10000)")
	q.Set("_txlock", "immediate")
	dsn := "file:" + path + "?" + q.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("meta: open %s: %w", path, err)
	}
	// Readers may run concurrently with the single writer (WAL). The pool
	// size bounds open file handles; writers queue on the busy timeout.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	s := &Store{db: db, now: now}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks that the database is usable (for /healthz).
func (s *Store) Ping(ctx context.Context) error {
	var one int
	return s.db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
}

const schemaVersion = 1

func (s *Store) migrate(ctx context.Context) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
			return err
		}
		var v int
		err := tx.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&v)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.ExecContext(ctx, schemaV1); err != nil {
				return fmt.Errorf("apply schema v1: %w", err)
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, schemaVersion)
			return err
		case err != nil:
			return err
		case v == schemaVersion:
			return nil
		default:
			return fmt.Errorf("meta: database schema version %d is not supported (want %d)", v, schemaVersion)
		}
	})
}

// tx runs fn in a write transaction (BEGIN IMMEDIATE via the DSN) and
// commits if fn returns nil.
func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("meta: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback() // the original error is what matters
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("meta: commit: %w", err)
	}
	return nil
}

func (s *Store) nowNanos() int64 { return s.now().UnixNano() }

func fromNanos(n int64) time.Time { return time.Unix(0, n).UTC() }
