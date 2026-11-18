package scrub

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"objstore/internal/worker/store"
)

const testChunk = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000"

func TestScrubReportsAndClearsCorruption(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{MaxPayloadSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("correct payload")
	sum := sha256.Sum256(data)
	if _, err := st.Put(testChunk, int64(len(data)), sum, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	s := New(st, 0, nil)
	if err := s.ScrubOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("clean suspects = %v", got)
	}
	path := filepath.Join(dir, "chunks", testChunk)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("tampered"))
	_ = f.Close()
	if err := s.ScrubOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 1 || got[0] != testChunk {
		t.Fatalf("suspects = %v", got)
	}
	if err := os.Truncate(path, int64(store.HeaderSize+len(data))); err != nil {
		t.Fatal(err)
	}
	if err := s.ScrubOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.Suspects(); len(got) != 0 {
		t.Fatalf("cleared suspects = %v", got)
	}
}
