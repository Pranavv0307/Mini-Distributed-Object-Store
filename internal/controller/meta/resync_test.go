package meta

import (
	"fmt"
	"testing"

	"objstore/internal/ids"
)

func replicaStates(t *testing.T, s *Store, obj string) map[string]string {
	t.Helper()
	cm, err := s.ChunkMap(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, c := range cm {
		for _, r := range c.Replicas {
			out[fmt.Sprintf("%d@%s", c.Idx, r.WorkerID)] = r.State
		}
	}
	return out
}

func TestReconcileInventoryTable(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 5, 10) // chunks 0..4 on w1, w2; size 10
	if _, err := commit(s, obj, 5, 10); err != nil {
		t.Fatal(err)
	}
	// Chunk 3 on w1 is CORRUPT (file present), chunk 4 on w1 CORRUPT (file gone).
	s.MarkReplicaCorrupt(ctx, cs[3].ID, "w1")
	s.MarkReplicaCorrupt(ctx, cs[4].ID, "w1")
	// A PENDING claim of an in-progress upload on w1, with no file yet.
	up := ids.NewObjectID()
	s.CreateObject(ctx, up, "u", "x")
	pend := chunk(t, up, 0, 7)
	s.ClaimChunk(ctx, pend, []string{"w1", "w2"})
	orphanOfUnknown, _ := ids.ChunkID(ids.NewObjectID(), 0)

	inv := []InventoryEntry{
		{cs[0].ID, 10}, // match
		{cs[1].ID, 17}, // size differs → CORRUPT
		// cs[2] missing → row removed
		{cs[3].ID, 10}, // CORRUPT, present → kept
		// cs[4] missing, CORRUPT → row removed
		{orphanOfUnknown, 5}, // no row → orphan
	}
	res, err := s.ReconcileInventory(ctx, "w1", inv)
	if err != nil {
		t.Fatal(err)
	}
	want := ReconcileResult{Matched: 1, SizeMismatch: 1, MissingRemoved: 2, CorruptKept: 1, PendingKept: 1, OrphansQueued: 1}
	if res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	st := replicaStates(t, s, obj)
	if st["0@w1"] != ReplicaOK || st["1@w1"] != ReplicaCorrupt || st["2@w1"] != "" ||
		st["3@w1"] != ReplicaCorrupt || st["4@w1"] != "" {
		t.Fatalf("w1 replica states = %v", st)
	}
	for i := range 5 { // w2 untouched
		if st[fmt.Sprintf("%d@w2", i)] != ReplicaOK {
			t.Fatalf("w2 replica %d changed: %v", i, st)
		}
	}
	if ps := replicaStates(t, s, up); ps["0@w1"] != ReplicaPending {
		t.Fatalf("PENDING claim touched: %v", ps)
	}
	pds, _ := s.PendingDeletesFor(ctx, orphanOfUnknown[:36])
	if len(pds) != 1 || pds[0].WorkerID != "w1" || pds[0].Reason != "orphan" {
		t.Fatalf("orphan pending delete = %+v", pds)
	}

	// Repeating with the same inventory changes nothing further.
	res, err = s.ReconcileInventory(ctx, "w1", inv)
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingRemoved != 0 || res.SizeMismatch != 0 || res.OrphansQueued != 0 || res.Skipped != 0 {
		t.Fatalf("second run = %+v, want no changes", res)
	}
}

func TestReconcileQueuesFilesOfDeletedObjectsAndRespectsI8(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 2, 10)
	commit(s, obj, 2, 10)
	s.MarkDeleting(ctx, obj, "client_delete", StatusReady) // rows → pending deletes already
	res, err := s.ReconcileInventory(ctx, "w1", []InventoryEntry{{cs[0].ID, 10}, {cs[1].ID, 10}})
	if err != nil {
		t.Fatal(err)
	}
	// The files are already queued by the delete: no duplicate entries.
	if res.OrphansQueued != 0 {
		t.Fatalf("result = %+v", res)
	}
	if pds, _ := s.PendingDeletesFor(ctx, obj); len(pds) != 4 {
		t.Fatalf("pending deletes = %d, want 4", len(pds))
	}
	audit, err := s.Audit(ctx, []string{"w1", "w2"}, 2)
	if err != nil || !audit.OK {
		t.Fatalf("audit after reconcile: %+v, %v", audit, err)
	}
}

func TestReconcileLargeInventoryInBatches(t *testing.T) {
	s, _ := openTest(t)
	var inv []InventoryEntry
	for range 3*reconcileBatch + 7 {
		id, _ := ids.ChunkID(ids.NewObjectID(), 0)
		inv = append(inv, InventoryEntry{id, 1})
	}
	res, err := s.ReconcileInventory(ctx, "w3", inv)
	if err != nil || res.OrphansQueued != len(inv) {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestChunkHealth(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 4, 10)
	commit(s, obj, 4, 10)
	up := ids.NewObjectID() // chunks of non-READY objects are not counted
	s.CreateObject(ctx, up, "u", "x")
	s.ClaimChunk(ctx, chunk(t, up, 0, 1), []string{"w1", "w2"})

	h, err := s.ChunkHealth(ctx, []string{"w1", "w2"}, 2)
	if err != nil || h.Total != 4 || h.FullyReplicated != 4 || h.UnderReplicated != 0 || h.ZeroEffective != 0 {
		t.Fatalf("all alive: %+v, %v", h, err)
	}
	if h.ReplicasByState[ReplicaOK] != 8 || h.ReplicasByState[ReplicaPending] != 2 || h.ObjectsByStatus[StatusReady] != 1 {
		t.Fatalf("by state: %+v", h)
	}
	// w2 dead: every chunk has one effective replica.
	if h, _ := s.ChunkHealth(ctx, []string{"w1"}, 2); h.UnderReplicated != 4 || h.FullyReplicated != 0 {
		t.Fatalf("w2 dead: %+v", h)
	}
	// One CORRUPT replica and nobody alive.
	s.MarkReplicaCorrupt(ctx, cs[0].ID, "w1")
	if h, _ := s.ChunkHealth(ctx, []string{"w1", "w2"}, 2); h.UnderReplicated != 1 || h.FullyReplicated != 3 {
		t.Fatalf("one corrupt: %+v", h)
	}
	if h, _ := s.ChunkHealth(ctx, nil, 2); h.ZeroEffective != 4 {
		t.Fatalf("nobody alive: %+v", h)
	}
	// Over-replication: a third OK copy.
	if err := s.tx(ctx, func(tx *sqlTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO replicas (chunk_id, worker_id, state, updated_at) VALUES (?, 'w3', 'OK', 0)`, cs[1].ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.ChunkHealth(ctx, []string{"w1", "w2", "w3"}, 2); h.OverReplicated != 1 {
		t.Fatalf("over-replicated: %+v", h)
	}
}

func checkByID(r AuditReport, id string) AuditCheck {
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	return AuditCheck{}
}

func TestAuditDetectsViolations(t *testing.T) {
	s, _ := openTest(t)
	alive := []string{"w1", "w2"}
	good, _ := upload(t, s, 3, 10)
	commit(s, good, 3, 10)
	r, err := s.Audit(ctx, alive, 2)
	if err != nil || !r.OK || !r.SteadyState || len(r.Checks) != 6 {
		t.Fatalf("clean store: %+v, %v", r, err)
	}

	exec := func(q string, args ...any) {
		t.Helper()
		if err := s.tx(ctx, func(tx *sqlTx) error { _, err := tx.ExecContext(ctx, q, args...); return err }); err != nil {
			t.Fatal(err)
		}
	}
	// I1 + I2: remove a chunk row of a READY object.
	bad, bcs := upload(t, s, 3, 10)
	commit(s, bad, 3, 10)
	exec(`DELETE FROM chunks WHERE chunk_id = ?`, bcs[1].ID)
	// I8: a pending delete overlapping a live replica.
	exec(`INSERT INTO pending_deletes (chunk_id, worker_id, object_id, reason, created_at) VALUES (?, 'w1', ?, 'test', 0)`, bcs[0].ID, bad)

	r, _ = s.Audit(ctx, alive, 2)
	if r.OK {
		t.Fatal("audit OK despite violations")
	}
	for id, want := range map[string]int{"I1": 1, "I2": 1, "I3": 0, "I8": 1, "I7": 0} {
		if c := checkByID(r, id); c.Violations != want || (want > 0 && len(c.Examples) == 0) {
			t.Errorf("%s = %+v, want %d violations", id, c, want)
		}
	}

	// I5 is steady-state only: a dead worker breaks it without breaking OK.
	s2, _ := openTest(t)
	o, _ := upload(t, s2, 2, 10)
	commit(s2, o, 2, 10)
	r, _ = s2.Audit(ctx, []string{"w1"}, 2)
	if !r.OK || r.SteadyState || checkByID(r, "I5").Violations != 2 {
		t.Fatalf("w2 dead: %+v", r)
	}
}
