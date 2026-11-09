package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"objstore/internal/wire"
	"objstore/internal/worker/store"
)

const (
	obj      = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"
	chunkA   = obj + "_000000"
	chunkB   = obj + "_000001"
	maxChunk = 8192
)

type fixture struct {
	srv *Server
	ts  *httptest.Server
	dir string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{MaxPayloadSize: maxChunk})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, Config{WorkerID: "worker1", MaxChunkSize: maxChunk, MaxConcurrentReads: 2})
	srv.SetAcceptingWrites(true)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &fixture{srv: srv, ts: ts, dir: dir}
}

func (f *fixture) do(t *testing.T, method, path string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, f.ts.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func shaHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func (f *fixture) put(t *testing.T, id string, data []byte) *http.Response {
	t.Helper()
	return f.do(t, http.MethodPut, "/internal/chunks/"+id, data, map[string]string{wire.HeaderChunkSHA256: shaHex(data)})
}

func wantError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, status, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("error Content-Type = %q", ct)
	}
	var e wire.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if e.Error.Code != code || e.Error.Message == "" {
		t.Fatalf("error = %+v, want code %s with a message", e.Error, code)
	}
}

func TestPutGetDeleteLifecycle(t *testing.T) {
	f := newFixture(t)
	data := []byte("hello chunk")

	resp := f.put(t, chunkA, data)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first PUT status = %d, want 201", resp.StatusCode)
	}
	var pr wire.PutChunkResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		t.Fatal(err)
	}
	if pr != (wire.PutChunkResponse{ChunkID: chunkA, Size: int64(len(data)), ChecksumSHA256: shaHex(data), Created: true}) {
		t.Fatalf("PUT body = %+v", pr)
	}

	if resp := f.put(t, chunkA, data); resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat PUT status = %d, want 200 (idempotent)", resp.StatusCode)
	}

	resp = f.do(t, http.MethodGet, "/internal/chunks/"+chunkA, nil, nil)
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET = %d %q", resp.StatusCode, got)
	}
	if resp.Header.Get(wire.HeaderChunkSHA256) != shaHex(data) {
		t.Fatalf("GET checksum header = %q", resp.Header.Get(wire.HeaderChunkSHA256))
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("GET Content-Type = %q", resp.Header.Get("Content-Type"))
	}

	for range 2 { // DELETE is idempotent
		if resp := f.do(t, http.MethodDelete, "/internal/chunks/"+chunkA, nil, nil); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
		}
	}
	wantError(t, f.do(t, http.MethodGet, "/internal/chunks/"+chunkA, nil, nil), http.StatusNotFound, wire.CodeChunkNotFound)
}

func TestPutValidation(t *testing.T) {
	f := newFixture(t)
	data := []byte("payload")
	good := map[string]string{wire.HeaderChunkSHA256: shaHex(data)}

	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/not-a-chunk", data, good), http.StatusBadRequest, wire.CodeInvalidChunkID)
	// %2F decodes to "/" inside the path segment; it must still be rejected.
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/..%2F"+chunkA, data, good), http.StatusBadRequest, wire.CodeInvalidChunkID)
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, data, nil), http.StatusBadRequest, wire.CodeInvalidRequest)
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, data, map[string]string{wire.HeaderChunkSHA256: "abc"}), http.StatusBadRequest, wire.CodeInvalidRequest)
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, data, map[string]string{wire.HeaderChunkSHA256: strings.Repeat("zz", 32)}), http.StatusBadRequest, wire.CodeInvalidRequest)
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, data, map[string]string{wire.HeaderChunkSHA256: shaHex([]byte("other"))}), http.StatusBadRequest, wire.CodeChecksumMismatch)

	big := make([]byte, maxChunk+1)
	wantError(t, f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, big, map[string]string{wire.HeaderChunkSHA256: shaHex(big)}), http.StatusBadRequest, wire.CodeChunkTooLarge)

	// Chunked transfer encoding has no Content-Length.
	req, _ := http.NewRequest(http.MethodPut, f.ts.URL+"/internal/chunks/"+chunkA, io.NopCloser(bytes.NewReader(data)))
	req.ContentLength = -1
	req.Header.Set(wire.HeaderChunkSHA256, shaHex(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	wantError(t, resp, http.StatusBadRequest, wire.CodeInvalidRequest)

	// None of the rejected requests may leave anything behind.
	inv := f.inventory(t)
	if len(inv.Chunks) != 0 {
		t.Fatalf("inventory after rejected PUTs = %+v", inv.Chunks)
	}
}

func TestUppercaseChecksumAccepted(t *testing.T) {
	f := newFixture(t)
	data := []byte("x")
	resp := f.do(t, http.MethodPut, "/internal/chunks/"+chunkA, data, map[string]string{wire.HeaderChunkSHA256: strings.ToUpper(shaHex(data))})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestWriteGate(t *testing.T) {
	f := newFixture(t)
	data := []byte("x")
	f.put(t, chunkA, data)
	f.srv.SetAcceptingWrites(false)

	wantError(t, f.put(t, chunkB, data), http.StatusServiceUnavailable, wire.CodeNotRegistered)
	wantError(t, f.do(t, http.MethodDelete, "/internal/chunks/"+chunkA, nil, nil), http.StatusServiceUnavailable, wire.CodeNotRegistered)
	// Reads stay available while writes are gated.
	if resp := f.do(t, http.MethodGet, "/internal/chunks/"+chunkA, nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET while gated = %d", resp.StatusCode)
	}
	if inv := f.inventory(t); len(inv.Chunks) != 1 {
		t.Fatalf("inventory while gated = %+v", inv.Chunks)
	}
	var h wire.WorkerHealth
	resp := f.do(t, http.MethodGet, "/healthz", nil, nil)
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if h != (wire.WorkerHealth{Status: "ok", WorkerID: "worker1", Registered: false, AcceptingWrites: false}) {
		t.Fatalf("healthz = %+v", h)
	}
}

func (f *fixture) inventory(t *testing.T) wire.InventoryResponse {
	t.Helper()
	resp := f.do(t, http.MethodGet, "/internal/workers/worker1/chunks", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("inventory status = %d", resp.StatusCode)
	}
	var inv wire.InventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		t.Fatal(err)
	}
	return inv
}

func TestInventory(t *testing.T) {
	f := newFixture(t)
	if inv := f.inventory(t); inv.WorkerID != "worker1" || inv.Chunks == nil || len(inv.Chunks) != 0 {
		t.Fatalf("empty inventory = %+v (chunks must be [] not null)", inv)
	}
	f.put(t, chunkB, []byte("bbb"))
	f.put(t, chunkA, []byte("aaaaa"))
	inv := f.inventory(t)
	want := []wire.ChunkInfo{{ChunkID: chunkA, Size: 5}, {ChunkID: chunkB, Size: 3}}
	if len(inv.Chunks) != 2 || inv.Chunks[0] != want[0] || inv.Chunks[1] != want[1] {
		t.Fatalf("inventory = %+v, want %+v", inv.Chunks, want)
	}
	wantError(t, f.do(t, http.MethodGet, "/internal/workers/worker2/chunks", nil, nil), http.StatusNotFound, wire.CodeWorkerNotFound)
}

func TestGetReturnsCorruptBytesAsStored(t *testing.T) {
	f := newFixture(t)
	data := []byte("original payload")
	f.put(t, chunkA, data)
	path := filepath.Join(f.dir, "chunks", chunkA)
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Write([]byte("!!")); err != nil {
		t.Fatal(err)
	}
	fh.Close()

	resp := f.do(t, http.MethodGet, "/internal/chunks/"+chunkA, nil, nil)
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(got) != string(data)+"!!" {
		t.Fatalf("GET = %d %q; worker must return bytes as stored", resp.StatusCode, got)
	}
	if resp.Header.Get(wire.HeaderChunkSHA256) != shaHex(data) {
		t.Fatal("checksum header should be the stored (original) checksum")
	}

	// A destroyed header is reported as CHUNK_CORRUPT.
	if err := os.WriteFile(path, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantError(t, f.do(t, http.MethodGet, "/internal/chunks/"+chunkA, nil, nil), http.StatusInternalServerError, wire.CodeChunkCorrupt)
}

func TestMethodNotAllowed(t *testing.T) {
	f := newFixture(t)
	if resp := f.do(t, http.MethodPost, "/internal/chunks/"+chunkA, []byte("x"), nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", resp.StatusCode)
	}
}
