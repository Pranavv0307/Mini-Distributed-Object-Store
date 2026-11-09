package meta

import (
	"errors"
	"testing"
)

func TestRepairCandidates(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 3, 10) // chunks on w1, w2
	commit(s, obj, 3, 10)
	alive := []string{"w1", "w2", "w3"}
	if got, _ := s.RepairCandidates(ctx, alive, 2, 100); len(got) != 0 {
		t.Fatalf("healthy store has candidates: %+v", got)
	}
	// w2 not alive: every chunk is a candidate.
	if got, _ := s.RepairCandidates(ctx, []string{"w1", "w3"}, 2, 100); len(got) != 3 || len(got[0].Replicas) != 2 {
		t.Fatalf("w2 down: %+v", got)
	}
	// One CORRUPT replica: only that chunk.
	s.MarkReplicaCorrupt(ctx, cs[1].ID, "w1")
	got, _ := s.RepairCandidates(ctx, alive, 2, 100)
	if len(got) != 1 || got[0].ID != cs[1].ID || got[0].ObjectID != obj {
		t.Fatalf("one corrupt: %+v", got)
	}
	if got, _ := s.RepairCandidates(ctx, []string{"w1", "w3"}, 2, 2); len(got) != 2 {
		t.Fatalf("limit not applied: %d", len(got))
	}
}

func TestRepairLifecycle(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 1, 10)
	commit(s, obj, 1, 10)
	c := cs[0].ID

	// New copy on w3, superseding w2's (lost) copy.
	if err := s.ClaimRepair(ctx, obj, c, "w3", false); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimRepair(ctx, obj, c, "w3", false); !errors.Is(err, ErrReplicaExists) {
		t.Fatalf("second claim on w3: %v", err)
	}
	if err := s.CompleteRepair(ctx, obj, c, "w3", &Supersede{WorkerID: "w2", State: ReplicaOK}); err != nil {
		t.Fatal(err)
	}
	if st := replicaStates(t, s, obj); st["0@w1"] != ReplicaOK || st["0@w3"] != ReplicaOK || st["0@w2"] != "" {
		t.Fatalf("after repair: %v", st)
	}
	pds, _ := s.PendingDeletesFor(ctx, obj)
	if len(pds) != 1 || pds[0].WorkerID != "w2" || pds[0].Reason != "superseded_by_repair" {
		t.Fatalf("pending deletes: %+v", pds)
	}
	// w2 now has a pending delete: it cannot be a repair target (I8).
	if err := s.ClaimRepair(ctx, obj, c, "w2", false); !errors.Is(err, ErrPendingDelete) {
		t.Fatalf("claim on pending-delete worker: %v", err)
	}
	if pd, _ := s.PendingDeleteWorkers(ctx, c); !pd["w2"] || len(pd) != 1 {
		t.Fatalf("PendingDeleteWorkers = %v", pd)
	}
	if a, _ := s.Audit(ctx, []string{"w1", "w3"}, 2); !a.OK || !a.SteadyState {
		t.Fatalf("audit after repair: %+v", a)
	}
}

func TestInPlaceRepairAndAbandon(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 1, 10)
	commit(s, obj, 1, 10)
	c := cs[0].ID
	if err := s.ClaimRepair(ctx, obj, c, "w1", true); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("in-place claim on an OK replica: %v", err)
	}
	s.MarkReplicaCorrupt(ctx, c, "w1")
	if err := s.ClaimRepair(ctx, obj, c, "w1", true); err != nil {
		t.Fatal(err)
	}
	if st := replicaStates(t, s, obj); st["0@w1"] != ReplicaPending {
		t.Fatalf("in-place claim: %v", st)
	}
	// The write failed: the claim becomes a pending delete.
	if err := s.AbandonRepair(ctx, obj, c, "w1", "repair_write_failed"); err != nil {
		t.Fatal(err)
	}
	if st := replicaStates(t, s, obj); st["0@w1"] != "" || st["0@w2"] != ReplicaOK {
		t.Fatalf("after abandon: %v", st)
	}
	if pds, _ := s.PendingDeletesFor(ctx, obj); len(pds) != 1 || pds[0].Reason != "repair_write_failed" {
		t.Fatalf("pending deletes: %+v", pds)
	}
	// In-place completion: the same row becomes OK; nothing superseded.
	obj2, cs2 := upload(t, s, 1, 10)
	commit(s, obj2, 1, 10)
	s.MarkReplicaCorrupt(ctx, cs2[0].ID, "w2")
	s.ClaimRepair(ctx, obj2, cs2[0].ID, "w2", true)
	if err := s.CompleteRepair(ctx, obj2, cs2[0].ID, "w2", nil); err != nil {
		t.Fatal(err)
	}
	if st := replicaStates(t, s, obj2); st["0@w2"] != ReplicaOK {
		t.Fatalf("in-place completion: %v", st)
	}
}

// TestRepairRacesWithDelete: a delete during a repair wins; the repair
// cannot complete or resurrect anything.
func TestRepairRacesWithDelete(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 1, 10)
	commit(s, obj, 1, 10)
	c := cs[0].ID
	if err := s.ClaimRepair(ctx, obj, c, "w3", false); err != nil {
		t.Fatal(err)
	}
	s.MarkDeleting(ctx, obj, "client_delete", StatusReady) // moves the PENDING claim too
	if err := s.CompleteRepair(ctx, obj, c, "w3", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("complete after delete: %v", err)
	}
	if err := s.ClaimRepair(ctx, obj, c, "w4", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim after delete: %v", err)
	}
	if pds, _ := s.PendingDeletesFor(ctx, obj); len(pds) != 3 { // w1, w2, and the w3 claim
		t.Fatalf("pending deletes after delete: %+v", pds)
	}
}

func TestTrimReplicaGuard(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 1, 10)
	commit(s, obj, 1, 10)
	c := cs[0].ID
	alive := []string{"w1", "w2"}
	// Trimming an OK copy would leave 1 < RF: refused.
	if moved, err := s.TrimReplica(ctx, obj, c, "w1", ReplicaOK, "test", alive, 2); err != nil || moved {
		t.Fatalf("trim below RF: moved=%v err=%v", moved, err)
	}
	// Add a third OK copy; trimming one is now allowed, a second is not.
	s.ClaimRepair(ctx, obj, c, "w3", false)
	s.CompleteRepair(ctx, obj, c, "w3", nil)
	alive = append(alive, "w3")
	if moved, _ := s.TrimReplica(ctx, obj, c, "w3", ReplicaOK, "over_replicated", alive, 2); !moved {
		t.Fatal("trim of an extra copy refused")
	}
	if moved, _ := s.TrimReplica(ctx, obj, c, "w2", ReplicaOK, "over_replicated", alive, 2); moved {
		t.Fatal("second trim went below RF")
	}
	// A state mismatch is not trimmed.
	if moved, _ := s.TrimReplica(ctx, obj, c, "w1", ReplicaCorrupt, "x", alive, 2); moved {
		t.Fatal("trimmed a replica in an unexpected state")
	}
}
