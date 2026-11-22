package scrub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"objstore/internal/worker/store"
)

const (
	chunkA = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000"
	chunkB = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000001"
)

var ctx = context.Background()

func newStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{MaxPayloadSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return st, dir
}

func put(t *testing.T, st *store.Store, id string, data []byte) {
	t.Helper()
	if _, err := st.Put(id, int64(len(data)), sha256.Sum256(data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// flipByte overwrites one payload byte in place (same size), the kind of
// damage only a checksum finds.
func flipByte(t *testing.T, dir, id string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "chunks", id), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, store.HeaderSize+3); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

func TestScrubReportsAndClearsCorruption(t *testing.T) {
	st, dir := newStore(t)
	data := []byte("correct payload")
	put(t, st, chunkA, data)
	s := New(st, Options{})
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("clean suspects = %v", got)
	}
	path := filepath.Join(dir, "chunks", chunkA)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("tampered"))
	_ = f.Close()
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 1 || got[0] != chunkA {
		t.Fatalf("suspects after append = %v", got)
	}
	if err := os.Truncate(path, int64(store.HeaderSize+len(data))); err != nil {
		t.Fatal(err)
	}
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("cleared suspects = %v", got)
	}
}

func TestScrubDetectsSameSizeDamage(t *testing.T) {
	st, dir := newStore(t)
	put(t, st, chunkA, []byte("0123456789"))
	put(t, st, chunkB, []byte("abcdefghij"))
	flipByte(t, dir, chunkB)
	s := New(st, Options{})
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 1 || got[0] != chunkB {
		t.Fatalf("suspects = %v, want [%s]", got, chunkB)
	}
}

// TestSuspectDroppedOnceRewritten: after the controller repairs a copy in
// place, the stale suspect must not be reported again (it would make the
// controller re-mark the fresh copy CORRUPT on every heartbeat until the next
// full pass).
func TestSuspectDroppedOnceRewritten(t *testing.T) {
	st, dir := newStore(t)
	data := []byte("0123456789")
	put(t, st, chunkA, data)
	flipByte(t, dir, chunkA)
	s := New(st, Options{})
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 1 {
		t.Fatalf("suspects = %v", got)
	}
	put(t, st, chunkA, data) // repair rewrites the copy in place
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("suspects after rewrite = %v", got)
	}
	// It stays dropped: no full pass needed.
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("suspects on the next heartbeat = %v", got)
	}
}

func TestDeletedChunkIsNotSuspect(t *testing.T) {
	st, dir := newStore(t)
	put(t, st, chunkA, []byte("0123456789"))
	flipByte(t, dir, chunkA)
	s := New(st, Options{})
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Delete(chunkA); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("suspects after delete = %v", got)
	}
	// A chunk deleted between the inventory and the read is not a suspect.
	fake := &vanishingStore{Store: st, id: chunkB}
	put(t, st, chunkB, []byte("x"))
	s2 := New(fake, Options{})
	if err := s2.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s2.Suspects(); len(got) != 0 {
		t.Fatalf("vanished chunk reported: %v", got)
	}
}

// vanishingStore lists id in the inventory but reports it not found on read.
type vanishingStore struct {
	*store.Store
	id string
}

func (v *vanishingStore) Get(id string) (store.Chunk, error) {
	if id == v.id {
		return store.Chunk{}, store.ErrNotFound
	}
	return v.Store.Get(id)
}

func TestScrubIsThrottled(t *testing.T) {
	st, _ := newStore(t)
	payload := bytes.Repeat([]byte("x"), 10_000)
	for i := range 3 {
		put(t, st, "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_00000"+string(rune('0'+i)), payload)
	}
	// 30,000 bytes at 100,000 B/s: the pass cannot finish in under 300ms.
	s := New(st, Options{BytesPerSec: 100_000})
	start := time.Now()
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 290*time.Millisecond {
		t.Fatalf("throttled pass took %v, want >= 300ms", elapsed)
	}
}

func TestScrubCancelKeepsPreviousSuspects(t *testing.T) {
	st, dir := newStore(t)
	put(t, st, chunkA, []byte("0123456789"))
	flipByte(t, dir, chunkA)
	s := New(st, Options{})
	if err := s.ScrubOnce(ctx); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.ScrubOnce(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pass err = %v", err)
	}
	if got := s.Suspects(); len(got) != 1 {
		t.Fatalf("suspects after cancelled pass = %v", got)
	}
}
