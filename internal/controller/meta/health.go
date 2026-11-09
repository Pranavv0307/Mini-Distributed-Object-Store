package meta

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// inList returns "(?, ?, ...)" and args for a SQL IN clause. An empty list
// becomes "(”)": no worker has an empty ID, so it matches nothing.
func inList(ids []string) (string, []any) {
	if len(ids) == 0 {
		return "('')", nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return "(?" + strings.Repeat(", ?", len(ids)-1) + ")", args
}

// effectiveSQL selects every chunk of a READY object with its number of
// effective replicas (OK on one of the given ALIVE workers).
func effectiveSQL(alive []string) (string, []any) {
	in, args := inList(alive)
	return `SELECT c.chunk_id, c.object_id, c.idx,
	               (SELECT COUNT(*) FROM replicas r
	                WHERE r.chunk_id = c.chunk_id AND r.state = 'OK' AND r.worker_id IN ` + in + `) AS eff
	        FROM chunks c JOIN objects o ON o.object_id = c.object_id
	        WHERE o.status = 'READY'`, args
}

// ChunkTotals classifies the chunks of READY objects by effective replicas.
type ChunkTotals struct {
	Total           int // chunks of READY objects
	FullyReplicated int // exactly RF effective replicas
	UnderReplicated int // 1 .. RF-1 effective replicas
	ZeroEffective   int // no effective replica: unreadable until a holder returns
	OverReplicated  int // more than RF effective replicas
	ReplicasByState map[string]int
	ObjectsByStatus map[string]int
	PendingDeletes  int
}

// ChunkHealth computes ChunkTotals for the given ALIVE workers.
func (s *Store) ChunkHealth(ctx context.Context, alive []string, rf int) (ChunkTotals, error) {
	t := ChunkTotals{ReplicasByState: map[string]int{}}
	q, args := effectiveSQL(alive)
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(CASE WHEN eff = ? THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN eff > 0 AND eff < ? THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN eff = 0 THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN eff > ? THEN 1 ELSE 0 END), 0)
		FROM (`+q+`)`, append([]any{rf, rf, rf}, args...)...).
		Scan(&t.Total, &t.FullyReplicated, &t.UnderReplicated, &t.ZeroEffective, &t.OverReplicated)
	if err != nil {
		return t, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM replicas GROUP BY state`)
	if err != nil {
		return t, err
	}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return t, err
		}
		t.ReplicasByState[st] = n
	}
	rows.Close()
	c, err := s.Counts(ctx)
	if err != nil {
		return t, err
	}
	t.ObjectsByStatus, t.PendingDeletes = c.ObjectsByStatus, c.PendingDeletes
	return t, nil
}

// AuditCheck is the result of checking one invariant.
type AuditCheck struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	Kind        string   `json:"kind"` // "safety" (must always hold) or "steady_state"
	Violations  int      `json:"violations"`
	Examples    []string `json:"examples"` // at most maxExamples
}

// AuditReport is the result of Audit.
type AuditReport struct {
	CheckedAt string `json:"checked_at"`
	// OK: every safety invariant holds. These must hold at all times.
	OK bool `json:"ok"`
	// SteadyState: I5 holds (every chunk has exactly RF effective replicas).
	// Expected to be false while a failure or repair is in progress.
	SteadyState bool         `json:"steady_state"`
	Checks      []AuditCheck `json:"checks"`
}

const maxExamples = 10

// Audit evaluates the invariants that can be checked from metadata alone
// (architecture §3.4, §5.9): I1, I2, I3, I7, I8 (safety) and I5 (steady
// state). I4 needs worker inventories (Phase 9); I6 is checked by every
// download. The checks are independent reads, not one snapshot, so a check
// racing a concurrent operation can at worst report a transient I5 change;
// the safety checks only examine READY objects, whose chunk rows never change.
func (s *Store) Audit(ctx context.Context, alive []string, rf int) (AuditReport, error) {
	rep := AuditReport{CheckedAt: s.now().UTC().Format(time.RFC3339Nano)}
	type check struct {
		id, desc, kind string
		query          string
		args           []any
	}
	effQ, effArgs := effectiveSQL(alive)
	checks := []check{
		{"I1", "a READY object has chunk rows for every index 0..num_chunks-1, without gaps or duplicates", "safety", `
			SELECT o.object_id || ': ' || COUNT(c.chunk_id) || ' chunk rows, idx ' || COALESCE(MIN(c.idx), '-') || '..' || COALESCE(MAX(c.idx), '-') || ', num_chunks ' || o.num_chunks
			FROM objects o LEFT JOIN chunks c ON c.object_id = o.object_id
			WHERE o.status = 'READY'
			GROUP BY o.object_id
			HAVING COUNT(c.chunk_id) <> o.num_chunks
			    OR (o.num_chunks > 0 AND (MIN(c.idx) <> 0 OR MAX(c.idx) <> o.num_chunks - 1))`, nil},
		{"I2", "the sizes of a READY object's chunks sum to the object size", "safety", `
			SELECT o.object_id || ': chunks sum to ' || COALESCE(SUM(c.size), 0) || ', size ' || o.size
			FROM objects o LEFT JOIN chunks c ON c.object_id = o.object_id
			WHERE o.status = 'READY'
			GROUP BY o.object_id
			HAVING COALESCE(SUM(c.size), 0) <> o.size`, nil},
		{"I3", "no two replica rows of one chunk are on the same worker", "safety", `
			SELECT chunk_id || ' on ' || worker_id || ' x' || COUNT(*)
			FROM replicas GROUP BY chunk_id, worker_id HAVING COUNT(*) > 1`, nil},
		{"I8", "no (chunk, worker) has both a replica row and a pending delete", "safety", `
			SELECT p.chunk_id || ' on ' || p.worker_id || ' (replica ' || r.state || ', pending delete ' || p.reason || ')'
			FROM pending_deletes p JOIN replicas r ON r.chunk_id = p.chunk_id AND r.worker_id = p.worker_id`, nil},
		{"I5", fmt.Sprintf("every chunk of every READY object has exactly %d effective replicas (OK on an ALIVE worker)", rf), "steady_state", `
			SELECT chunk_id || ': ' || eff || ' effective' FROM (` + effQ + `) WHERE eff <> ?`, append(effArgs, rf)},
	}
	for _, c := range checks {
		ac := AuditCheck{ID: c.id, Description: c.desc, Kind: c.kind, Examples: []string{}}
		rows, err := s.db.QueryContext(ctx, c.query, c.args...)
		if err != nil {
			return rep, fmt.Errorf("audit %s: %w", c.id, err)
		}
		for rows.Next() {
			var ex string
			if err := rows.Scan(&ex); err != nil {
				rows.Close()
				return rep, err
			}
			ac.Violations++
			if len(ac.Examples) < maxExamples {
				ac.Examples = append(ac.Examples, ex)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return rep, err
		}
		rep.Checks = append(rep.Checks, ac)
	}

	// I7: the listing (the client's view) contains exactly the READY objects.
	i7 := AuditCheck{ID: "I7", Description: "only READY objects are listed, and every READY object is", Kind: "safety", Examples: []string{}}
	listed := 0
	var cur *ListCursor
	for {
		objs, next, err := s.ListObjects(ctx, 1000, cur)
		if err != nil {
			return rep, fmt.Errorf("audit I7: %w", err)
		}
		for _, o := range objs {
			listed++
			if o.Status != StatusReady {
				i7.Violations++
				if len(i7.Examples) < maxExamples {
					i7.Examples = append(i7.Examples, o.ID+" listed with status "+o.Status)
				}
			}
		}
		if next == nil {
			break
		}
		cur = next
	}
	var ready int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE status = 'READY'`).Scan(&ready); err != nil {
		return rep, err
	}
	if ready != listed {
		i7.Violations++
		i7.Examples = append(i7.Examples, fmt.Sprintf("%d READY objects but %d listed", ready, listed))
	}
	rep.Checks = append(rep.Checks, i7)

	rep.OK, rep.SteadyState = true, true
	for _, c := range rep.Checks {
		if c.Violations == 0 {
			continue
		}
		if c.Kind == "safety" {
			rep.OK = false
		} else {
			rep.SteadyState = false
		}
	}
	return rep, nil
}
