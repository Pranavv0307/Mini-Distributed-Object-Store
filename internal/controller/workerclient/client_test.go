package workerclient

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"objstore/internal/wire"
	"objstore/internal/worker/server"
	"objstore/internal/worker/store"
)

const chunkID = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000"

func realWorker(t *testing.T) string {
	t.Helper()
	st, err := store.Open(t.TempDir(), store.Options{MaxPayloadSize: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(st, server.Config{WorkerID: "worker1", MaxChunkSize: 1 << 16, MaxConcurrentReads: 2})
	srv.OpenWrites()
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestPutGetDeleteAgainstRealWorker(t *testing.T) {
	url := realWorker(t)
	c := New(time.Second, 5*time.Second)
	ctx := context.Background()
	data := []byte("chunk bytes")
	sum := sha256.Sum256(data)

	if err := c.PutChunk(ctx, url, chunkID, data, sum); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := c.PutChunk(ctx, url, chunkID, data, sum); err != nil {
		t.Fatalf("idempotent PutChunk: %v", err)
	}
	got, err := c.GetChunk(ctx, url, chunkID, int64(len(data))+1)
	if err != nil || string(got.Data) != string(data) || got.StoredSHA256 == "" {
		t.Fatalf("GetChunk = %+v, %v", got, err)
	}
	err = c.PutChunk(ctx, url, chunkID, data, sha256.Sum256([]byte("other")))
	var we *Error
	if !errors.As(err, &we) || we.Status != http.StatusBadRequest || we.Code != wire.CodeChecksumMismatch {
		t.Fatalf("bad checksum err = %v", err)
	}
	for range 2 {
		if err := c.DeleteChunk(ctx, url, chunkID); err != nil {
			t.Fatalf("DeleteChunk: %v", err)
		}
	}
	_, err = c.GetChunk(ctx, url, chunkID, 100)
	if !errors.As(err, &we) || we.Status != http.StatusNotFound {
		t.Fatalf("GetChunk after delete err = %v", err)
	}
}

func TestRequestTimeout(t *testing.T) {
	block := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer ts.Close()
	defer close(block)
	c := New(time.Second, 50*time.Millisecond)
	start := time.Now()
	err := c.PutChunk(context.Background(), ts.URL, chunkID, []byte("x"), sha256.Sum256([]byte("x")))
	if err == nil {
		t.Fatal("PutChunk to a hung worker succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("timeout took %v", d)
	}
}

func TestConnectionRefused(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close()
	c := New(200*time.Millisecond, time.Second)
	if err := c.DeleteChunk(context.Background(), url, chunkID); err == nil {
		t.Fatal("DeleteChunk to a closed port succeeded")
	}
}
