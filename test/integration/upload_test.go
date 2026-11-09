package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"objstore/internal/config"
	"objstore/internal/controller/api"
	"objstore/internal/controller/download"
	"objstore/internal/controller/janitor"
	"objstore/internal/controller/meta"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/upload"
	"objstore/internal/controller/workerclient"
	"objstore/internal/wire"
)

// uploadCluster is an in-process controller with metadata DB, upload
// coordinator, janitor, and N real workers (each behind a fault injector).
type uploadCluster struct {
	t       testing.TB
	url     string
	meta    *meta.Store
	reg     *registry.Registry
	jan     *janitor.Janitor
	workers []*testWorker
	byID    map[string]*testWorker
}

type clusterOpts struct {
	workers        int
	chunkSize      int64
	window         int
	maxObjectSize  int64
	maxUploads     int
	requestTimeout time.Duration
}

func newUploadCluster(t testing.TB, o clusterOpts) *uploadCluster {
	t.Helper()
	if o.chunkSize == 0 {
		o.chunkSize = config.DefaultChunkSize
	}
	if o.window == 0 {
		o.window = 4
	}
	if o.maxObjectSize == 0 {
		o.maxObjectSize = 1 << 30
	}
	if o.maxUploads == 0 {
		o.maxUploads = 16
	}
	if o.requestTimeout == 0 {
		o.requestTimeout = 5 * time.Second
	}
	m, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"), meta.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New(registry.Options{DeadAfter: deadAfter})
	wc := workerclient.New(time.Second, o.requestTimeout)
	jan := janitor.New(m, reg, wc, janitor.Config{Interval: 20 * time.Millisecond})
	up := upload.New(m, reg, wc, placement.Random{}, upload.Config{
		ChunkSize: o.chunkSize, ReplicationFactor: 2, Window: o.window, MaxObjectSize: o.maxObjectSize,
		ClientReadTimeout: 5 * time.Second,
	}, jan.Kick)
	srv := api.New(reg, api.NoopReconciler{}, api.Config{
		HeartbeatInterval: hbInterval, DeadAfter: deadAfter, RepairGrace: repairGrace,
		Meta: m, Uploader: up, MaxConcurrentUploads: o.maxUploads,
		Downloader: download.New(m, reg, wc, nil), MaxConcurrentDownloads: 32,
	})
	ts := httptest.NewServer(srv.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	bg.Go(func() { registry.RunFailureDetector(ctx, reg, checkEvery) })
	bg.Go(func() { jan.Run(ctx) })
	t.Cleanup(func() { cancel(); bg.Wait(); ts.Close(); m.Close() })

	c := &uploadCluster{t: t, url: ts.URL, meta: m, reg: reg, jan: jan, byID: map[string]*testWorker{}}
	for i := range o.workers {
		w := newTestWorker(t, fmt.Sprintf("worker%d", i+1), ts.URL)
		c.workers = append(c.workers, w)
		c.byID[w.id] = w
	}
	waitFor(t, clusterLimit, "all workers ALIVE", func() bool { return len(reg.Alive()) == o.workers })
	return c
}

type uploadResult struct {
	status int
	info   wire.ObjectInfo
	err    wire.ErrorResponse
}

func (c *uploadCluster) postRaw(contentType string, body io.Reader) uploadResult {
	c.t.Helper()
	resp, err := http.Post(c.url+"/api/v1/objects", contentType, body)
	if err != nil {
		return uploadResult{status: -1}
	}
	defer resp.Body.Close()
	r := uploadResult{status: resp.StatusCode}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusCreated {
		if err := json.Unmarshal(raw, &r.info); err != nil {
			c.t.Fatalf("decode 201 body %q: %v", raw, err)
		}
		if loc := resp.Header.Get("Location"); loc != "/api/v1/objects/"+r.info.ObjectID {
			c.t.Fatalf("Location = %q", loc)
		}
	} else {
		_ = json.Unmarshal(raw, &r.err)
	}
	return r
}

func (c *uploadCluster) upload(parts ...part) uploadResult {
	c.t.Helper()
	ct, body := multipartBody(c.t, parts...)
	return c.postRaw(ct, bytes.NewReader(body))
}

func randomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func sha(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// verifyStored checks the committed object against data: chunk boundaries,
// per-chunk checksums, RF=2 OK replicas on distinct workers, and that every
// replica's bytes on its worker equal the corresponding slice of data.
func (c *uploadCluster) verifyStored(objectID string, data []byte, chunkSize int64) []meta.ChunkWithReplicas {
	c.t.Helper()
	ctx := context.Background()
	o, err := c.meta.GetObject(ctx, objectID)
	if err != nil {
		c.t.Fatal(err)
	}
	wantChunks := int((int64(len(data)) + chunkSize - 1) / chunkSize)
	if o.Status != meta.StatusReady || o.Size != int64(len(data)) || o.NumChunks != wantChunks || o.ChecksumSHA256 != sha(data) {
		c.t.Fatalf("object = %+v, want READY size %d chunks %d", o, len(data), wantChunks)
	}
	cm, err := c.meta.ChunkMap(ctx, objectID)
	if err != nil || len(cm) != wantChunks {
		c.t.Fatalf("chunk map: %d entries, %v", len(cm), err)
	}
	for i, ch := range cm {
		lo := int64(i) * chunkSize
		hi := min(lo+chunkSize, int64(len(data)))
		want := data[lo:hi]
		if ch.Idx != i || ch.Size != hi-lo || ch.ChecksumSHA256 != sha(want) {
			c.t.Fatalf("chunk %d = %+v", i, ch.Chunk)
		}
		if len(ch.Replicas) != 2 || ch.Replicas[0].WorkerID == ch.Replicas[1].WorkerID {
			c.t.Fatalf("chunk %d replicas = %+v", i, ch.Replicas)
		}
		for _, r := range ch.Replicas {
			if r.State != meta.ReplicaOK {
				c.t.Fatalf("chunk %d replica %+v not OK", i, r)
			}
			got, err := c.byID[r.WorkerID].store.Get(ch.ID)
			if err != nil || !bytes.Equal(got.Payload, want) {
				c.t.Fatalf("chunk %d on %s: err=%v equal=%v", i, r.WorkerID, err, bytes.Equal(got.Payload, want))
			}
		}
	}
	return cm
}

// waitClean waits until no UPLOADING/DELETING objects and no pending deletes
// remain, and no worker holds a chunk file that does not belong to a READY
// object (aborted uploads fully cleaned up).
func (c *uploadCluster) waitClean() {
	c.t.Helper()
	waitFor(c.t, clusterLimit, "cleanup of aborted uploads", func() bool {
		cnt, err := c.meta.Counts(context.Background())
		if err != nil || cnt.PendingDeletes != 0 || cnt.ObjectsByStatus[meta.StatusUploading] != 0 ||
			cnt.ObjectsByStatus[meta.StatusDeleting] != 0 {
			return false
		}
		return c.strayFiles() == 0
	})
}

// strayFiles counts chunk files on workers whose object is not READY.
func (c *uploadCluster) strayFiles() int {
	n := 0
	for _, w := range c.workers {
		inv, err := w.store.Inventory()
		if err != nil {
			c.t.Fatal(err)
		}
		for _, e := range inv {
			o, err := c.meta.GetObject(context.Background(), e.ChunkID[:36])
			if err != nil || o.Status != meta.StatusReady {
				n++
			}
		}
	}
	return n
}

// filesOnWorkers counts chunk files (optionally only those of one object).
func (c *uploadCluster) filesOnWorkers(objectIDPrefix string) int {
	n := 0
	for _, w := range c.workers {
		inv, err := w.store.Inventory()
		if err != nil {
			c.t.Fatal(err)
		}
		for _, e := range inv {
			if objectIDPrefix == "" || e.ChunkID[:36] == objectIDPrefix {
				n++
			}
		}
	}
	return n
}

// TestUploadDistributesAndReplicates is the M3 checkpoint: a 5.5 MB object
// becomes 6 chunks (5 × 1,000,000 + 500,000) with 12 OK replica rows on
// distinct workers, and every replica on disk holds exactly its bytes.
func TestUploadDistributesAndReplicates(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4})
	data := randomBytes(t, 5_500_000)
	r := c.upload(metaPart(`{"owner":"alice"}`), part{field: "file", filename: "photo.jpg", contentType: "image/jpeg", data: data})
	if r.status != http.StatusCreated {
		t.Fatalf("status = %d, err = %+v", r.status, r.err)
	}
	i := r.info
	if i.Size != 5_500_000 || i.NumChunks != 6 || i.Name != "photo.jpg" || i.ContentType != "image/jpeg" ||
		i.ChecksumSHA256 != sha(data) || i.CustomMetadata["owner"] != "alice" || i.MetadataVersion != 1 {
		t.Fatalf("response = %+v", i)
	}
	cm := c.verifyStored(i.ObjectID, data, config.DefaultChunkSize)
	if cm[5].Size != 500_000 {
		t.Fatalf("last chunk size = %d", cm[5].Size)
	}
	cnt, _ := c.meta.Counts(context.Background())
	if cnt.Replicas != 12 || cnt.Chunks != 6 || cnt.PendingDeletes != 0 {
		t.Fatalf("counts = %+v", cnt)
	}
	if n := c.filesOnWorkers(i.ObjectID); n != 12 {
		t.Fatalf("%d chunk files on workers, want 12", n)
	}
	for _, ch := range cm { // file names are the chunk IDs
		if ch.ID != fmt.Sprintf("%s_%06d", i.ObjectID, ch.Idx) {
			t.Fatalf("chunk ID %q", ch.ID)
		}
	}
}

func TestUploadEdgeSizes(t *testing.T) {
	const cs = 1000
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: cs})
	for _, n := range []int{0, 1, cs - 1, cs, cs + 1, 3 * cs, 3*cs + 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			data := randomBytes(t, n)
			r := c.upload(filePart("f.bin", data))
			if r.status != http.StatusCreated {
				t.Fatalf("status %d: %+v", r.status, r.err)
			}
			c.verifyStored(r.info.ObjectID, data, cs)
		})
	}
	// The empty object's checksum is SHA-256 of no bytes.
	r := c.upload(filePart("empty", nil))
	if r.info.ChecksumSHA256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" || r.info.NumChunks != 0 {
		t.Fatalf("empty object = %+v", r.info)
	}
}

func TestUploadWindowSizes(t *testing.T) {
	const cs = 1000
	data := randomBytes(t, 25*cs+3)
	for _, window := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: cs, window: window})
			r := c.upload(filePart("f", data))
			if r.status != http.StatusCreated {
				t.Fatalf("status %d: %+v", r.status, r.err)
			}
			c.verifyStored(r.info.ObjectID, data, cs)
		})
	}
}

func TestUploadMetadataPositionAndValidation(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	data := randomBytes(t, 2500)

	// After the file part: applied at commit.
	r := c.upload(filePart("f", data), metaPart(`{"k":"after"}`))
	if r.status != http.StatusCreated || r.info.CustomMetadata["k"] != "after" {
		t.Fatalf("metadata after file: %d %+v", r.status, r.info)
	}

	// Invalid before the file: rejected before anything is stored.
	r = c.upload(metaPart(`{"n":1}`), filePart("f", data))
	if r.status != http.StatusBadRequest || r.err.Error.Code != wire.CodeInvalidMetadata {
		t.Fatalf("invalid metadata before file: %d %+v", r.status, r.err)
	}
	cnt, _ := c.meta.Counts(context.Background())
	if cnt.ObjectsByStatus[meta.StatusDeleting]+cnt.ObjectsByStatus[meta.StatusUploading] != 0 {
		t.Fatalf("object created for a rejected upload: %+v", cnt)
	}

	// Invalid after the file: the stored chunks are aborted and cleaned up.
	r = c.upload(filePart("f", data), metaPart(`{"`+string(bytes.Repeat([]byte("k"), 200))+`":"v"}`))
	if r.status != http.StatusBadRequest || r.err.Error.Code != wire.CodeMetadataLimitExceeded {
		t.Fatalf("invalid metadata after file: %d %+v", r.status, r.err)
	}
	c.waitClean()
	cnt, _ = c.meta.Counts(context.Background())
	if cnt.ObjectsByStatus[meta.StatusReady] != 1 {
		t.Fatalf("counts = %+v", cnt)
	}
}

func TestUploadRequestValidation(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	cases := []struct {
		name   string
		ct     string
		body   []byte
		status int
		code   string
	}{
		{"not multipart", "application/json", []byte(`{}`), 400, wire.CodeInvalidRequest},
	}
	ct, body := multipartBody(t, metaPart(`{}`))
	cases = append(cases, struct {
		name   string
		ct     string
		body   []byte
		status int
		code   string
	}{"no file part", ct, body, 400, wire.CodeMissingFile})
	ct, body = multipartBody(t, filePart("a", []byte("1")), filePart("b", []byte("2")))
	cases = append(cases, struct {
		name   string
		ct     string
		body   []byte
		status int
		code   string
	}{"two file parts", ct, body, 400, wire.CodeMissingFile})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := c.postRaw(tc.ct, bytes.NewReader(tc.body))
			if r.status != tc.status || r.err.Error.Code != tc.code || r.err.Error.Message == "" {
				t.Fatalf("got %d %+v, want %d %s", r.status, r.err, tc.status, tc.code)
			}
		})
	}
	c.waitClean()
	if cnt, _ := c.meta.Counts(context.Background()); cnt.ObjectsByStatus[meta.StatusReady] != 0 {
		t.Fatalf("an invalid request committed an object: %+v", cnt)
	}
}

// TestUploadInsufficientWorkers: with fewer than RF ALIVE workers the upload
// is refused up front with 503 and nothing is created.
func TestUploadInsufficientWorkers(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	c.workers[1].stop()
	waitFor(t, clusterLimit, "worker2 DEAD", func() bool { return len(c.reg.Alive()) == 1 })
	r := c.upload(filePart("f", randomBytes(t, 5000)))
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeInsufficientWorkers {
		t.Fatalf("got %d %+v", r.status, r.err)
	}
	if cnt, _ := c.meta.Counts(context.Background()); len(cnt.ObjectsByStatus) != 0 || cnt.Chunks != 0 {
		t.Fatalf("rows created: %+v", cnt)
	}
	if n := c.filesOnWorkers(""); n != 0 {
		t.Fatalf("%d files stored", n)
	}
}

// TestUploadSurvivesWorkerFailureMidUpload: a worker that starts failing
// partway through does not fail the upload; its failed claims become pending
// deletes and every chunk still ends with 2 OK replicas.
func TestUploadSurvivesWorkerFailureMidUpload(t *testing.T) {
	const cs = 1000
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: cs, window: 1})
	c.byID["worker2"].faults.failAfterPuts.Store(3)
	data := randomBytes(t, 40*cs)
	r := c.upload(filePart("f", data))
	if r.status != http.StatusCreated {
		t.Fatalf("status %d: %+v", r.status, r.err)
	}
	cm := c.verifyStored(r.info.ObjectID, data, cs)
	on2 := 0
	for _, ch := range cm {
		for _, rep := range ch.Replicas {
			if rep.WorkerID == "worker2" {
				on2++
			}
		}
	}
	if on2 > 3 {
		t.Fatalf("%d replicas on worker2, which only accepted 3 PUTs", on2)
	}
	// The failed claim's pending delete is drained by the janitor (worker2's
	// DELETE still works; only PUTs fail).
	waitFor(t, clusterLimit, "pending deletes drained", func() bool {
		cnt, _ := c.meta.Counts(context.Background())
		return cnt.PendingDeletes == 0
	})
}

// TestUploadFailsWhenWorkersRunOut: with 2 workers and one failing every PUT
// there is no distinct second target; the upload returns 503, the object
// never becomes visible, and all stored chunks are cleaned up.
func TestUploadFailsWhenWorkersRunOut(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	c.byID["worker2"].faults.failAfterPuts.Store(2)
	r := c.upload(filePart("f", randomBytes(t, 10_000)))
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeUploadFailed {
		t.Fatalf("got %d %+v", r.status, r.err)
	}
	if strings.Contains(r.err.Error.Message, "worker2") {
		t.Fatalf("error message reveals a worker: %q", r.err.Error.Message)
	}
	c.waitClean()
	if cnt, _ := c.meta.Counts(context.Background()); len(cnt.ObjectsByStatus) != 0 || cnt.Chunks != 0 || cnt.Replicas != 0 {
		t.Fatalf("rows remain: %+v", cnt)
	}
}

// TestUploadTimeoutIsNotTreatedAsFailure: worker3 stores every chunk but
// answers after the request timeout. The controller moves those claims to
// pending deletes (it cannot know the write failed), writes elsewhere, and the
// janitor then deletes the bytes that did land on worker3.
func TestUploadTimeoutIsNotTreatedAsFailure(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000, requestTimeout: 300 * time.Millisecond})
	c.byID["worker3"].faults.slowResponse.Store(int64(2 * time.Second))
	data := randomBytes(t, 8000)
	r := c.upload(filePart("f", data))
	if r.status != http.StatusCreated {
		t.Fatalf("status %d: %+v", r.status, r.err)
	}
	cm := c.verifyStored(r.info.ObjectID, data, 1000)
	for _, ch := range cm {
		for _, rep := range ch.Replicas {
			if rep.WorkerID == "worker3" {
				t.Fatalf("replica recorded on the timed-out worker: %+v", rep)
			}
		}
	}
	// Bytes that landed on worker3 are removed via the pending deletes.
	w3 := c.byID["worker3"]
	waitFor(t, clusterLimit, "worker3 cleaned and no pending deletes", func() bool {
		inv, _ := w3.store.Inventory()
		cnt, _ := c.meta.Counts(context.Background())
		return len(inv) == 0 && cnt.PendingDeletes == 0
	})
}

func TestUploadRetriesAfterTransitCorruption(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	c.byID["worker1"].faults.corruptPuts.Store(true)
	data := randomBytes(t, 6000)
	r := c.upload(filePart("f", data))
	if r.status != http.StatusCreated {
		t.Fatalf("status %d: %+v", r.status, r.err)
	}
	c.verifyStored(r.info.ObjectID, data, 1000)
	if inv, _ := c.byID["worker1"].store.Inventory(); len(inv) != 0 {
		t.Fatalf("worker1 stored corrupted chunks: %+v", inv)
	}
}

// TestTruncatedUploadIsNeverCommitted: a body cut off before its closing
// boundary (client disconnect) must not be mistaken for a short file.
func TestTruncatedUploadIsNeverCommitted(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	ct, body := multipartBody(t, filePart("f", randomBytes(t, 5500)))
	r := c.postRaw(ct, bytes.NewReader(truncate(t, body, 3300)))
	if r.status == http.StatusCreated {
		t.Fatalf("truncated upload committed: %+v", r.info)
	}
	c.waitClean()
	if cnt, _ := c.meta.Counts(context.Background()); cnt.ObjectsByStatus[meta.StatusReady] != 0 {
		t.Fatalf("counts = %+v", cnt)
	}
}

// TestClientDisconnectMidUpload: the client stops sending and closes the
// connection; the upload is aborted and cleaned up.
func TestClientDisconnectMidUpload(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	ct, body := multipartBody(t, filePart("f", randomBytes(t, 20_000)))
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/api/v1/objects", pr)
	req.Header.Set("Content-Type", ct)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	if _, err := pw.Write(body[:6000]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, clusterLimit, "upload in progress", func() bool {
		cnt, _ := c.meta.Counts(context.Background())
		return cnt.Chunks > 0
	})
	cancel() // client goes away
	pw.CloseWithError(errors.New("client gone"))
	<-done
	c.waitClean()
	if cnt, _ := c.meta.Counts(context.Background()); len(cnt.ObjectsByStatus) != 0 {
		t.Fatalf("counts = %+v", cnt)
	}
}

func TestUploadTooLarge(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000, maxObjectSize: 2500})
	if r := c.upload(filePart("ok", randomBytes(t, 2500))); r.status != http.StatusCreated {
		t.Fatalf("object at the limit: %d %+v", r.status, r.err)
	}
	r := c.upload(filePart("big", randomBytes(t, 2501)))
	if r.status != http.StatusBadRequest || r.err.Error.Code != wire.CodeObjectTooLarge {
		t.Fatalf("got %d %+v", r.status, r.err)
	}
	c.waitClean()
}

func TestConcurrentUploads(t *testing.T) {
	const cs = 1000
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: cs})
	type result struct {
		data []byte
		r    uploadResult
	}
	results := make([]result, 12)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			data := randomBytes(t, 1000*i+137)
			results[i] = result{data: data, r: c.upload(filePart(fmt.Sprintf("f%d", i), data))}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for i, res := range results {
		if res.r.status != http.StatusCreated {
			t.Fatalf("upload %d: %d %+v", i, res.r.status, res.r.err)
		}
		if seen[res.r.info.ObjectID] {
			t.Fatalf("duplicate object ID %s", res.r.info.ObjectID)
		}
		seen[res.r.info.ObjectID] = true
		c.verifyStored(res.r.info.ObjectID, res.data, cs)
	}
}

func TestServerBusy(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000, maxUploads: 1})
	ct, body := multipartBody(t, filePart("f", randomBytes(t, 5000)))
	pr, pw := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, c.url+"/api/v1/objects", pr)
	req.Header.Set("Content-Type", ct)
	first := make(chan int, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			first <- -1
			return
		}
		resp.Body.Close()
		first <- resp.StatusCode
	}()
	if _, err := pw.Write(body[:2500]); err != nil { // first upload holds the only slot
		t.Fatal(err)
	}
	waitFor(t, clusterLimit, "first upload in progress", func() bool {
		cnt, _ := c.meta.Counts(context.Background())
		return cnt.ObjectsByStatus[meta.StatusUploading] == 1
	})
	r := c.upload(filePart("g", []byte("x")))
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeServerBusy {
		t.Fatalf("second upload: %d %+v", r.status, r.err)
	}
	if _, err := pw.Write(body[2500:]); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	if st := <-first; st != http.StatusCreated {
		t.Fatalf("first upload status = %d", st)
	}
}

// TestUploadSurvivesWorkerStopMidUpload: a worker process disappears (HTTP
// server closed, heartbeats stop) while an upload is in progress; with ≥ 2
// workers left the upload still succeeds with full replication.
func TestUploadSurvivesWorkerStopMidUpload(t *testing.T) {
	const cs = 1000
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: cs, window: 1})
	for _, w := range c.workers {
		w.faults.putLatency.Store(int64(10 * time.Millisecond)) // make the upload take a while
	}
	data := randomBytes(t, 40*cs)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		deadline := time.Now().Add(clusterLimit)
		for time.Now().Before(deadline) {
			if cnt, _ := c.meta.Counts(context.Background()); cnt.Chunks >= 5 {
				c.byID["worker2"].stop()
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	r := c.upload(filePart("f", data))
	<-stopped
	if r.status != http.StatusCreated {
		t.Fatalf("status %d: %+v", r.status, r.err)
	}
	cm := c.verifyStored(r.info.ObjectID, data, cs)
	late := 0
	for _, ch := range cm[10:] {
		for _, rep := range ch.Replicas {
			if rep.WorkerID == "worker2" {
				late++
			}
		}
	}
	if late != 0 {
		t.Fatalf("%d late chunks recorded on the stopped worker", late)
	}
}
