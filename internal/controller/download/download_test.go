package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/workerclient"
	"objstore/internal/ids"
	"objstore/internal/wire"
)

// fakeGetter serves chunk bytes per (worker address, chunk ID), with
// per-replica overrides to simulate failures.
type fakeGetter struct {
	mu        sync.Mutex
	data      map[string][]byte // chunkID → correct bytes
	overrides map[string]func() (workerclient.Chunk, error)
	calls     map[string]int // address/chunkID → count
}

func (f *fakeGetter) GetChunk(_ context.Context, addr, chunkID string, maxBytes int64) (workerclient.Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := addr + "/" + chunkID
	f.calls[key]++
	if o, ok := f.overrides[key]; ok {
		return o()
	}
	d := f.data[chunkID]
	if int64(len(d)) > maxBytes {
		d = d[:maxBytes]
	}
	return workerclient.Chunk{Data: d}, nil
}

func (f *fakeGetter) set(addr, chunkID string, fn func() (workerclient.Chunk, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.overrides[addr+"/"+chunkID] = fn
}

func (f *fakeGetter) callCount(addr, chunkID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[addr+"/"+chunkID]
}

type fixture struct {
	t      *testing.T
	meta   *meta.Store
	reg    *registry.Registry
	getter *fakeGetter
	ts     *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	m, err := meta.Open(filepath.Join(t.TempDir(), "m.db"), meta.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	reg := registry.New(registry.Options{DeadAfter: time.Hour})
	for _, w := range []string{"w1", "w2", "w3"} {
		reg.CompleteRegistration(w, reg.BeginRegistration(w, w, registry.Capacity{}))
	}
	g := &fakeGetter{data: map[string][]byte{}, overrides: map[string]func() (workerclient.Chunk, error){}, calls: map[string]int{}}
	c := New(m, reg, g, nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.ServeObject(w, r, r.URL.Path[1:])
	}))
	t.Cleanup(ts.Close)
	return &fixture{t: t, meta: m, reg: reg, getter: g, ts: ts}
}

func hexsum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// store commits an object of the given chunks, each replicated on workers.
// objectChecksum "" means the correct one.
func (f *fixture) store(name string, chunks [][]byte, workers []string, objectChecksum string) (string, []string) {
	f.t.Helper()
	ctx := context.Background()
	obj := ids.NewObjectID()
	if _, err := f.meta.CreateObject(ctx, obj, name, "text/plain"); err != nil {
		f.t.Fatal(err)
	}
	var all []byte
	var chunkIDs []string
	for i, d := range chunks {
		id, _ := ids.ChunkID(obj, i)
		chunkIDs = append(chunkIDs, id)
		f.getter.data[id] = d
		all = append(all, d...)
		if err := f.meta.ClaimChunk(ctx, meta.Chunk{ID: id, ObjectID: obj, Idx: i, Size: int64(len(d)), ChecksumSHA256: hexsum(d)}, workers); err != nil {
			f.t.Fatal(err)
		}
		for _, w := range workers {
			if err := f.meta.MarkReplicaOK(ctx, obj, id, w); err != nil {
				f.t.Fatal(err)
			}
		}
	}
	if objectChecksum == "" {
		objectChecksum = hexsum(all)
	}
	if _, err := f.meta.CommitObject(ctx, meta.CommitInput{ObjectID: obj, Size: int64(len(all)), NumChunks: len(chunks),
		ChecksumSHA256: objectChecksum, ReplicationFactor: len(workers)}); err != nil {
		f.t.Fatal(err)
	}
	return obj, chunkIDs
}

type response struct {
	status  int
	header  http.Header
	body    []byte
	readErr error
	errBody wire.ErrorResponse
}

func (f *fixture) get(method, id string) response {
	f.t.Helper()
	req, _ := http.NewRequest(method, f.ts.URL+"/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return response{status: -1, readErr: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	r := response{status: resp.StatusCode, header: resp.Header, body: body, readErr: readErr}
	if resp.StatusCode != http.StatusOK {
		_ = jsonUnmarshal(body, &r.errBody)
	}
	return r
}

func (f *fixture) replicaState(obj, chunkID, worker string) string {
	f.t.Helper()
	cm, err := f.meta.ChunkMap(context.Background(), obj)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, c := range cm {
		for _, r := range c.Replicas {
			if c.ID == chunkID && r.WorkerID == worker {
				return r.State
			}
		}
	}
	return ""
}

var three = [][]byte{[]byte("alpha-"), []byte("bravo-"), []byte("ch")}

func TestDownloadHappyPathAndHeaders(t *testing.T) {
	f := newFixture(t)
	obj, _ := f.store(`dir/sub\report "v2".txt`, three, []string{"w1", "w2"}, "")
	r := f.get(http.MethodGet, obj)
	if r.status != 200 || string(r.body) != "alpha-bravo-ch" || r.readErr != nil {
		t.Fatalf("got %d %q %v", r.status, r.body, r.readErr)
	}
	want := hexsum([]byte("alpha-bravo-ch"))
	h := r.header
	if h.Get("Content-Length") != "14" || h.Get("Content-Type") != "text/plain" ||
		h.Get("ETag") != `"`+want+`"` || h.Get(wire.HeaderObjectChecksum) != want {
		t.Fatalf("headers = %v", h)
	}
	if cd := h.Get("Content-Disposition"); cd != `attachment; filename="report v2.txt"` {
		t.Fatalf("Content-Disposition = %q", cd)
	}
}

func TestContentDisposition(t *testing.T) {
	for in, want := range map[string]string{
		"a.txt":         `attachment; filename=a.txt`,
		"":              `attachment`,
		"../../etc/x":   `attachment; filename=x`,
		"..":            `attachment`,
		"bad\r\nname":   `attachment; filename=badname`,
		"résumé.pdf":    `attachment; filename*=utf-8''r%C3%A9sum%C3%A9.pdf`,
		`C:\temp\f.bin`: `attachment; filename=f.bin`,
	} {
		if got := contentDisposition(in); got != want {
			t.Errorf("contentDisposition(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEmptyObject(t *testing.T) {
	f := newFixture(t)
	obj, _ := f.store("e", nil, []string{"w1", "w2"}, "")
	r := f.get(http.MethodGet, obj)
	if r.status != 200 || len(r.body) != 0 || r.header.Get("Content-Length") != "0" {
		t.Fatalf("got %d %q %v", r.status, r.body, r.header)
	}
}

func TestHeadFetchesNothing(t *testing.T) {
	f := newFixture(t)
	obj, ids := f.store("h", three, []string{"w1", "w2"}, "")
	r := f.get(http.MethodHead, obj)
	if r.status != 200 || r.header.Get("Content-Length") != "14" || len(r.body) != 0 {
		t.Fatalf("HEAD = %d %v %q", r.status, r.header, r.body)
	}
	for _, id := range ids {
		if f.getter.callCount("w1", id)+f.getter.callCount("w2", id) != 0 {
			t.Fatal("HEAD fetched chunk data")
		}
	}
}

func TestNotFoundAndInvalid(t *testing.T) {
	f := newFixture(t)
	if r := f.get(http.MethodGet, "not-a-uuid"); r.status != 400 || r.errBody.Error.Code != wire.CodeInvalidObjectID {
		t.Fatalf("malformed id: %d %+v", r.status, r.errBody)
	}
	if r := f.get(http.MethodGet, ids.NewObjectID()); r.status != 404 || r.errBody.Error.Code != wire.CodeObjectNotFound {
		t.Fatalf("unknown id: %d %+v", r.status, r.errBody)
	}
	// UPLOADING and DELETING objects are invisible (I7).
	ctx := context.Background()
	up := ids.NewObjectID()
	f.meta.CreateObject(ctx, up, "u", "x")
	if r := f.get(http.MethodGet, up); r.status != 404 {
		t.Fatalf("UPLOADING object: %d", r.status)
	}
	obj, _ := f.store("d", three, []string{"w1", "w2"}, "")
	f.meta.MarkDeleting(ctx, obj, "test", meta.StatusReady)
	if r := f.get(http.MethodGet, obj); r.status != 404 {
		t.Fatalf("DELETING object: %d", r.status)
	}
}

// TestFallbackAndClassification: every kind of replica failure on one replica
// still yields the correct bytes from the other; "proof of bad data" marks
// the replica CORRUPT, transient errors do not.
func TestFallbackAndClassification(t *testing.T) {
	cases := []struct {
		name        string
		fail        func() (workerclient.Chunk, error)
		wantCorrupt bool
	}{
		{"flipped bytes", func() (workerclient.Chunk, error) { return workerclient.Chunk{Data: []byte("bravX-")}, nil }, true},
		{"extra bytes", func() (workerclient.Chunk, error) { return workerclient.Chunk{Data: []byte("bravo-!")}, nil }, true},
		{"truncated", func() (workerclient.Chunk, error) { return workerclient.Chunk{Data: []byte("bra")}, nil }, true},
		{"missing file", func() (workerclient.Chunk, error) {
			return workerclient.Chunk{}, &workerclient.Error{Status: 404, Code: wire.CodeChunkNotFound}
		}, true},
		{"unreadable header", func() (workerclient.Chunk, error) {
			return workerclient.Chunk{}, &workerclient.Error{Status: 500, Code: wire.CodeChunkCorrupt}
		}, true},
		{"timeout", func() (workerclient.Chunk, error) { return workerclient.Chunk{}, context.DeadlineExceeded }, false},
		{"worker 503", func() (workerclient.Chunk, error) {
			return workerclient.Chunk{}, &workerclient.Error{Status: 503, Code: wire.CodeInternal}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			obj, cids := f.store("f", three, []string{"w1", "w2"}, "")
			f.getter.set("w1", cids[1], tc.fail)
			// Several downloads so the random replica order hits w1 at least once.
			for range 8 {
				r := f.get(http.MethodGet, obj)
				if r.status != 200 || string(r.body) != "alpha-bravo-ch" {
					t.Fatalf("got %d %q", r.status, r.body)
				}
			}
			if f.getter.callCount("w1", cids[1]) == 0 {
				t.Fatal("w1 never tried (random order); test is not meaningful")
			}
			state := f.replicaState(obj, cids[1], "w1")
			if tc.wantCorrupt && state != meta.ReplicaCorrupt || !tc.wantCorrupt && state != meta.ReplicaOK {
				t.Fatalf("w1 replica state = %s, wantCorrupt=%v", state, tc.wantCorrupt)
			}
			if f.replicaState(obj, cids[1], "w2") != meta.ReplicaOK {
				t.Fatal("the good replica was changed")
			}
			if tc.wantCorrupt {
				// Once CORRUPT, the bad replica is no longer read.
				before := f.getter.callCount("w1", cids[1])
				for range 5 {
					f.get(http.MethodGet, obj)
				}
				if f.getter.callCount("w1", cids[1]) != before {
					t.Fatal("CORRUPT replica was read again")
				}
			}
		})
	}
}

func TestDeadWorkerIsNotRead(t *testing.T) {
	f := newFixture(t)
	obj, cids := f.store("f", three, []string{"w1", "w2"}, "")
	// Make w1 DEAD: re-register it, then let the detector see silence.
	clk := time.Now()
	reg := registry.New(registry.Options{DeadAfter: time.Millisecond, Now: func() time.Time { return clk }})
	for _, w := range []string{"w1", "w2"} {
		reg.CompleteRegistration(w, reg.BeginRegistration(w, w, registry.Capacity{}))
	}
	clk = clk.Add(time.Second)
	e2, _ := reg.Get("w2")
	reg.Heartbeat("w2", e2.Epoch, registry.Capacity{})
	reg.CheckLiveness()
	c := New(f.meta, reg, f.getter, nil)
	rec := httptest.NewRecorder()
	c.ServeObject(rec, httptest.NewRequest(http.MethodGet, "/"+obj, nil), obj)
	if rec.Code != 200 || rec.Body.String() != "alpha-bravo-ch" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	for _, id := range cids {
		if f.getter.callCount("w1", id) != 0 {
			t.Fatal("a DEAD worker was read")
		}
	}
}

func TestNoValidReplicaOnChunkZeroIsClean503(t *testing.T) {
	f := newFixture(t)
	obj, cids := f.store("f", three, []string{"w1", "w2"}, "")
	bad := func() (workerclient.Chunk, error) { return workerclient.Chunk{Data: []byte("XXXXXX")}, nil }
	f.getter.set("w1", cids[0], bad)
	f.getter.set("w2", cids[0], bad)
	r := f.get(http.MethodGet, obj)
	if r.status != 503 || r.errBody.Error.Code != wire.CodeStorageUnavailable {
		t.Fatalf("got %d %+v", r.status, r.errBody)
	}
	// Both replicas are now CORRUPT, so the next request fails at pre-flight.
	r = f.get(http.MethodGet, obj)
	if r.status != 503 || r.errBody.Error.Code != wire.CodeStorageUnavailable {
		t.Fatalf("second request: %d %+v", r.status, r.errBody)
	}
}

// TestMidStreamFailureTruncates: when a later chunk has no valid replica, the
// client gets a truncated body (never wrong bytes) and a read error.
func TestMidStreamFailureTruncates(t *testing.T) {
	f := newFixture(t)
	obj, cids := f.store("f", three, []string{"w1", "w2"}, "")
	bad := func() (workerclient.Chunk, error) { return workerclient.Chunk{}, errors.New("connection refused") }
	f.getter.set("w1", cids[2], bad)
	f.getter.set("w2", cids[2], bad)
	r := f.get(http.MethodGet, obj)
	if r.status != 200 || r.readErr == nil {
		t.Fatalf("got %d, readErr %v; want 200 with a truncated body", r.status, r.readErr)
	}
	if !bytes.HasPrefix([]byte("alpha-bravo-ch"), r.body) || len(r.body) >= 14 {
		t.Fatalf("body %q is not a strict prefix of the object", r.body)
	}
}

// TestAssemblyChecksumGuard: every chunk verifies, but the stored object
// checksum disagrees (simulating an assembly bug). The last chunk must not be
// sent.
func TestAssemblyChecksumGuard(t *testing.T) {
	f := newFixture(t)
	obj, _ := f.store("f", three, []string{"w1", "w2"}, hexsum([]byte("something else")))
	r := f.get(http.MethodGet, obj)
	if r.readErr == nil || string(r.body) == "alpha-bravo-ch" {
		t.Fatalf("wrong object fully delivered: %d %q", r.status, r.body)
	}
	if string(r.body) != "alpha-bravo-" {
		t.Fatalf("body = %q, want the verified prefix without the last chunk", r.body)
	}
}

// TestErrorsDoNotRevealWorkers: clients never learn chunk locations (spec
// §5.2), not even in error messages.
func TestErrorsDoNotRevealWorkers(t *testing.T) {
	f := newFixture(t)
	obj, cids := f.store("f", three, []string{"w1", "w2"}, "")
	bad := func() (workerclient.Chunk, error) { return workerclient.Chunk{Data: []byte("XXXXXX")}, nil }
	f.getter.set("w1", cids[0], bad)
	f.getter.set("w2", cids[0], bad)
	for range 2 {
		r := f.get(http.MethodGet, obj)
		if r.status != 503 || bytes.Contains(r.body, []byte("w1")) || bytes.Contains(r.body, []byte("w2")) {
			t.Fatalf("503 body reveals workers or wrong status: %d %s", r.status, r.body)
		}
	}
}
