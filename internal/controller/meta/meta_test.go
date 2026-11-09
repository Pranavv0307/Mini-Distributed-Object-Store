package meta

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"objstore/internal/ids"
)

var ctx = context.Background()

func openTest(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func chunk(t *testing.T, obj string, idx int, size int64) Chunk {
	t.Helper()
	id, err := ids.ChunkID(obj, idx)
	if err != nil {
		t.Fatal(err)
	}
	return Chunk{ID: id, ObjectID: obj, Idx: idx, Size: size, ChecksumSHA256: fmt.Sprintf("%064d", idx)}
}

// upload claims and acks n chunks of size each on (w1, w2).
func upload(t *testing.T, s *Store, n int, size int64) (string, []Chunk) {
	t.Helper()
	obj := ids.NewObjectID()
	if _, err := s.CreateObject(ctx, obj, "f.bin", "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	var cs []Chunk
	for i := range n {
		c := chunk(t, obj, i, size)
		if err := s.ClaimChunk(ctx, c, []string{"w1", "w2"}); err != nil {
			t.Fatal(err)
		}
		for _, w := range []string{"w1", "w2"} {
			if err := s.MarkReplicaOK(ctx, obj, c.ID, w); err != nil {
				t.Fatal(err)
			}
		}
		cs = append(cs, c)
	}
	return obj, cs
}

func commit(s *Store, obj string, n int, size int64) (Object, error) {
	return s.CommitObject(ctx, CommitInput{ObjectID: obj, Size: int64(n) * size, NumChunks: n,
		ChecksumSHA256: "abc", CustomMetadata: map[string]string{"k": "v"}, ReplicationFactor: 2})
}

func TestUploadLifecycle(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 3, 100)
	o, err := commit(s, obj, 3, 100)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if o.Status != StatusReady || o.Size != 300 || o.NumChunks != 3 || o.CustomMetadata["k"] != "v" || o.MetadataVersion != 1 {
		t.Fatalf("committed object = %+v", o)
	}
	cm, err := s.ChunkMap(ctx, obj)
	if err != nil || len(cm) != 3 {
		t.Fatalf("ChunkMap = %+v, %v", cm, err)
	}
	for i, c := range cm {
		if c.ID != cs[i].ID || c.Idx != i || len(c.Replicas) != 2 ||
			c.Replicas[0] != (Replica{c.ID, "w1", ReplicaOK}) || c.Replicas[1] != (Replica{c.ID, "w2", ReplicaOK}) {
			t.Fatalf("chunk %d = %+v", i, c)
		}
	}
	// Committing twice fails: the object is no longer UPLOADING.
	if _, err := commit(s, obj, 3, 100); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("second commit err = %v", err)
	}
}

func TestEmptyObjectCommits(t *testing.T) {
	s, _ := openTest(t)
	obj, _ := upload(t, s, 0, 0)
	o, err := s.CommitObject(ctx, CommitInput{ObjectID: obj, ReplicationFactor: 2, ChecksumSHA256: "e3b0"})
	if err != nil || o.Status != StatusReady || o.NumChunks != 0 || o.Size != 0 {
		t.Fatalf("empty commit = %+v, %v", o, err)
	}
}

func TestCommitRejectsIncompleteObjects(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, s *Store) (obj string, in CommitInput)
	}{
		{"missing chunk (gap)", func(t *testing.T, s *Store) (string, CommitInput) {
			obj, _ := upload(t, s, 2, 10)
			return obj, CommitInput{ObjectID: obj, Size: 30, NumChunks: 3, ReplicationFactor: 2}
		}},
		{"size mismatch", func(t *testing.T, s *Store) (string, CommitInput) {
			obj, _ := upload(t, s, 2, 10)
			return obj, CommitInput{ObjectID: obj, Size: 21, NumChunks: 2, ReplicationFactor: 2}
		}},
		{"replica still PENDING", func(t *testing.T, s *Store) (string, CommitInput) {
			obj, _ := upload(t, s, 1, 10)
			c := chunk(t, obj, 1, 10)
			if err := s.ClaimChunk(ctx, c, []string{"w1", "w2"}); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkReplicaOK(ctx, obj, c.ID, "w1"); err != nil {
				t.Fatal(err)
			}
			return obj, CommitInput{ObjectID: obj, Size: 20, NumChunks: 2, ReplicationFactor: 2}
		}},
		{"under-replicated", func(t *testing.T, s *Store) (string, CommitInput) {
			obj, _ := upload(t, s, 1, 10)
			return obj, CommitInput{ObjectID: obj, Size: 10, NumChunks: 1, ReplicationFactor: 3}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTest(t)
			obj, in := tc.setup(t, s)
			if _, err := s.CommitObject(ctx, in); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("err = %v, want ErrIncomplete", err)
			}
			if o, _ := s.GetObject(ctx, obj); o.Status != StatusUploading {
				t.Fatalf("status after refused commit = %s", o.Status)
			}
		})
	}
}

func TestDuplicateReplicaRejected(t *testing.T) {
	s, _ := openTest(t)
	obj := ids.NewObjectID()
	s.CreateObject(ctx, obj, "f", "x")
	if err := s.ClaimChunk(ctx, chunk(t, obj, 0, 1), []string{"w1", "w1"}); err == nil {
		t.Fatal("two replicas of one chunk on one worker accepted (violates I3)")
	}
	// The failed transaction left nothing behind.
	if cm, _ := s.ChunkMap(ctx, obj); len(cm) != 0 {
		t.Fatalf("rows left after failed claim: %+v", cm)
	}
}

func TestReplaceReplicaMovesClaimToPendingDelete(t *testing.T) {
	s, _ := openTest(t)
	obj := ids.NewObjectID()
	s.CreateObject(ctx, obj, "f", "x")
	c := chunk(t, obj, 0, 5)
	if err := s.ClaimChunk(ctx, c, []string{"w1", "w2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceReplica(ctx, obj, c.ID, "w2", "w3", "put_failed"); err != nil {
		t.Fatal(err)
	}
	cm, _ := s.ChunkMap(ctx, obj)
	if len(cm[0].Replicas) != 2 || cm[0].Replicas[0].WorkerID != "w1" || cm[0].Replicas[1] != (Replica{c.ID, "w3", ReplicaPending}) {
		t.Fatalf("replicas after replace = %+v", cm[0].Replicas)
	}
	pds, _ := s.PendingDeletesFor(ctx, obj)
	if len(pds) != 1 || pds[0].WorkerID != "w2" || pds[0].ChunkID != c.ID || pds[0].Reason != "put_failed" {
		t.Fatalf("pending deletes = %+v", pds)
	}
	// I8: the chunk can never be claimed again on w2 while the delete is pending.
	if err := s.ReplaceReplica(ctx, obj, c.ID, "w3", "w2", "x"); !errors.Is(err, ErrPendingDelete) {
		t.Fatalf("claim on pending-delete worker: err = %v, want ErrPendingDelete", err)
	}
	// ...and the failed transaction did not move w3's claim.
	cm, _ = s.ChunkMap(ctx, obj)
	if cm[0].Replicas[1] != (Replica{c.ID, "w3", ReplicaPending}) {
		t.Fatalf("failed replace changed state: %+v", cm[0].Replicas)
	}
	// Replacing a claim that does not exist is ErrClaimLost.
	if err := s.ReplaceReplica(ctx, obj, c.ID, "w9", "w4", "x"); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("err = %v, want ErrClaimLost", err)
	}
}

func TestClaimRefusedForPendingDelete(t *testing.T) {
	s, _ := openTest(t)
	obj := ids.NewObjectID()
	s.CreateObject(ctx, obj, "f", "x")
	c := chunk(t, obj, 0, 5)
	// Simulate a leftover pending delete for this chunk on w2 (e.g. from an
	// orphan sweep), then try to claim it there.
	if err := s.tx(ctx, func(tx *sqlTx) error {
		return insertPendingDelete(ctx, tx, c.ID, "w2", obj, "orphan", s.nowNanos())
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimChunk(ctx, c, []string{"w1", "w2"}); !errors.Is(err, ErrPendingDelete) {
		t.Fatalf("err = %v, want ErrPendingDelete", err)
	}
	if cm, _ := s.ChunkMap(ctx, obj); len(cm) != 0 {
		t.Fatal("refused claim left rows")
	}
}

func TestAbortStopsUploadAndQueuesCleanup(t *testing.T) {
	s, _ := openTest(t)
	obj, cs := upload(t, s, 2, 10)
	c2 := chunk(t, obj, 2, 10)
	if err := s.ClaimChunk(ctx, c2, []string{"w1", "w3"}); err != nil { // a PENDING claim in flight
		t.Fatal(err)
	}
	moved, err := s.MarkDeleting(ctx, obj, "upload_aborted", StatusUploading)
	if err != nil || !moved {
		t.Fatalf("MarkDeleting = %v, %v", moved, err)
	}
	if o, _ := s.GetObject(ctx, obj); o.Status != StatusDeleting {
		t.Fatalf("status = %s", o.Status)
	}
	pds, _ := s.PendingDeletesFor(ctx, obj)
	if len(pds) != 6 { // 2 chunks × 2 OK replicas + 1 chunk × 2 PENDING claims
		t.Fatalf("pending deletes = %d, want 6", len(pds))
	}
	cm, _ := s.ChunkMap(ctx, obj)
	for _, c := range cm {
		if len(c.Replicas) != 0 {
			t.Fatalf("replica rows remain after abort: %+v", c)
		}
	}

	// Every later upload-side write fails: the handler must stop (DEC-011).
	if err := s.ClaimChunk(ctx, chunk(t, obj, 3, 10), []string{"w1", "w2"}); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("claim after abort: %v", err)
	}
	if err := s.MarkReplicaOK(ctx, obj, c2.ID, "w1"); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("mark OK after abort: %v", err)
	}
	if _, err := commit(s, obj, 3, 10); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("commit after abort: %v", err)
	}
	// Aborting again is a harmless no-op.
	if moved, err := s.MarkDeleting(ctx, obj, "again", StatusUploading); err != nil || moved {
		t.Fatalf("second MarkDeleting = %v, %v", moved, err)
	}

	// The object row stays until every pending delete is confirmed.
	if ids, _ := s.FinalizeDeletedObjects(ctx); len(ids) != 0 {
		t.Fatalf("finalized with deletes outstanding: %v", ids)
	}
	for _, p := range pds {
		if err := s.CompletePendingDelete(ctx, p.ChunkID, p.WorkerID); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.FinalizeDeletedObjects(ctx)
	if err != nil || len(got) != 1 || got[0] != obj {
		t.Fatalf("FinalizeDeletedObjects = %v, %v", got, err)
	}
	if _, err := s.GetObject(ctx, obj); !errors.Is(err, ErrNotFound) {
		t.Fatalf("object still present: %v", err)
	}
	if cm, _ := s.ChunkMap(ctx, obj); len(cm) != 0 || cs == nil {
		t.Fatalf("chunk rows remain: %+v", cm)
	}
	c, _ := s.Counts(ctx)
	if c.Chunks != 0 || c.Replicas != 0 || c.PendingDeletes != 0 || len(c.ObjectsByStatus) != 0 {
		t.Fatalf("counts after cleanup = %+v", c)
	}
}

func TestPendingDeleteRetryScheduling(t *testing.T) {
	s, _ := openTest(t)
	obj, _ := upload(t, s, 1, 1)
	s.MarkDeleting(ctx, obj, "test", StatusUploading)
	due, _ := s.DuePendingDeletes(ctx, []string{"w1", "w2"}, 10)
	if len(due) != 2 {
		t.Fatalf("due = %d", len(due))
	}
	if err := s.FailPendingDelete(ctx, due[0].ChunkID, due[0].WorkerID, "worker down", 1<<40); err != nil {
		t.Fatal(err)
	}
	due2, _ := s.DuePendingDeletes(ctx, []string{"w1", "w2"}, 10)
	if len(due2) != 1 || due2[0].WorkerID == due[0].WorkerID {
		t.Fatalf("after failure, due = %+v", due2)
	}
	// Filtering by worker: only entries on the listed workers are returned.
	if only, _ := s.DuePendingDeletes(ctx, []string{due2[0].WorkerID}, 10); len(only) != 1 {
		t.Fatalf("filtered due = %+v", only)
	}
	if none, _ := s.DuePendingDeletes(ctx, []string{"w9"}, 10); len(none) != 0 {
		t.Fatalf("entries returned for an unrelated worker: %+v", none)
	}
	if none, _ := s.DuePendingDeletes(ctx, nil, 10); len(none) != 0 {
		t.Fatalf("entries returned for no workers: %+v", none)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	s, path := openTest(t)
	obj, _ := upload(t, s, 2, 7)
	if _, err := commit(s, obj, 2, 7); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	o, err := s2.GetObject(ctx, obj)
	if err != nil || o.Status != StatusReady || o.Size != 14 {
		t.Fatalf("after reopen: %+v, %v", o, err)
	}
}

// TestConcurrentWriters runs many uploads in parallel. With BEGIN IMMEDIATE
// and a busy timeout, no writer may fail with SQLITE_BUSY.
func TestConcurrentWriters(t *testing.T) {
	s, _ := openTest(t)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			obj := ids.NewObjectID()
			if _, err := s.CreateObject(ctx, obj, "f", "x"); err != nil {
				errs <- err
				return
			}
			for i := range 10 {
				id, _ := ids.ChunkID(obj, i)
				c := Chunk{ID: id, ObjectID: obj, Idx: i, Size: 1, ChecksumSHA256: "x"}
				if err := s.ClaimChunk(ctx, c, []string{"w1", "w2"}); err != nil {
					errs <- err
					return
				}
				for _, w := range []string{"w1", "w2"} {
					if err := s.MarkReplicaOK(ctx, obj, id, w); err != nil {
						errs <- err
						return
					}
				}
			}
			if _, err := s.CommitObject(ctx, CommitInput{ObjectID: obj, Size: 10, NumChunks: 10, ReplicationFactor: 2}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	c, _ := s.Counts(ctx)
	if c.ObjectsByStatus[StatusReady] != 16 || c.Chunks != 160 || c.Replicas != 320 {
		t.Fatalf("counts = %+v", c)
	}
}

func TestSchemaVersionCheck(t *testing.T) {
	s, path := openTest(t)
	if _, err := s.db.Exec(`UPDATE schema_version SET version = 99`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := Open(path, Options{}); err == nil {
		t.Fatal("opened a database with an unknown schema version")
	}
}
