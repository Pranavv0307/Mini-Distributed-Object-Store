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
	// UncheckedWorkers lists workers a check could not examine (I4: the
	// worker's inventory could not be fetched). Not violations.
	UncheckedWorkers []string `json:"unchecked_workers,omitempty"`
}

// AuditReport is the result of Audit.
type AuditReport struct {
	CheckedAt string `json:"checked_at"`
	// OK: every safety invariant holds. These must hold at all times.
	OK bool `json:"ok"`
	// SteadyState: every steady_state check holds: I5 (every chunk has
	// exactly RF effective replicas) and, when inventories are checked, I4.
	// Expected to be false while a failure or repair is in progress.
	SteadyState bool         `json:"steady_state"`
	Checks      []AuditCheck `json:"checks"`
}

// readyIDs returns the IDs of all READY objects.
func (s *Store) readyIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT object_id FROM objects WHERE status = 'READY'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

func checkViolation(c *AuditCheck, example string) {
	c.Violations++
	if len(c.Examples) < maxExamples {
		c.Examples = append(c.Examples, example)
	}
}

const maxExamples = 10

// Audit evaluates the invariants that can be checked from metadata alone
// (architecture §3.4, §5.9): I1, I2, I3, I7, I8 (safety) and I5 (steady
// state). I4 needs worker inventories (AuditInventory, run by the API on
// top of this report); I6 is checked by every download. The checks are
// independent reads, not one snapshot, so a check racing a concurrent
// operation can at worst report a transient I5 change; the safety checks
// only examine READY objects, whose chunk rows never change, and I7 is
// written to tolerate objects committed or deleted during the listing.
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
	// The listing is paged and objects are committed and deleted meanwhile,
	// so comparing it with a count is racy (a false violation under load).
	// Instead: every listed object must be READY in its own row, and every
	// object that was READY both before and after the listing must be in it.
	i7 := AuditCheck{ID: "I7", Description: "only READY objects are listed, and every READY object is", Kind: "safety", Examples: []string{}}
	readyBefore, err := s.readyIDs(ctx)
	if err != nil {
		return rep, fmt.Errorf("audit I7: %w", err)
	}
	listed := map[string]bool{}
	var cur *ListCursor
	for {
		objs, next, err := s.ListObjects(ctx, 1000, cur)
		if err != nil {
			return rep, fmt.Errorf("audit I7: %w", err)
		}
		for _, o := range objs {
			listed[o.ID] = true
			if o.Status != StatusReady {
				checkViolation(&i7, o.ID+" listed with status "+o.Status)
			}
		}
		if next == nil {
			break
		}
		cur = next
	}
	readyAfter, err := s.readyIDs(ctx)
	if err != nil {
		return rep, fmt.Errorf("audit I7: %w", err)
	}
	for id := range readyBefore {
		if readyAfter[id] && !listed[id] {
			checkViolation(&i7, id+" is READY but not listed")
		}
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
