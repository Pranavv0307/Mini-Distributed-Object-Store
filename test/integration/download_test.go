package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"objstore/internal/config"
	"objstore/internal/controller/meta"
	"objstore/internal/wire"
)

type downloadResult struct {
	status  int
	header  http.Header
	body    []byte
	readErr error
	err     wire.ErrorResponse
}

func (c *uploadCluster) download(objectID string) downloadResult {
	c.t.Helper()
	resp, err := http.Get(c.url + "/api/v1/objects/" + objectID)
	if err != nil {
		return downloadResult{status: -1, readErr: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	r := downloadResult{status: resp.StatusCode, header: resp.Header, body: body, readErr: readErr}
	if resp.StatusCode != http.StatusOK {
		_ = json.Unmarshal(body, &r.err)
	}
	return r
}

// mustRoundTrip uploads data and returns the object ID.
func (c *uploadCluster) mustUpload(data []byte) string {
	c.t.Helper()
	r := c.upload(filePart("obj.bin", data))
	if r.status != http.StatusCreated {
		c.t.Fatalf("upload: %d %+v", r.status, r.err)
	}
	return r.info.ObjectID
}

func (c *uploadCluster) chunkMap(objectID string) []meta.ChunkWithReplicas {
	c.t.Helper()
	cm, err := c.meta.ChunkMap(context.Background(), objectID)
	if err != nil {
		c.t.Fatal(err)
	}
	return cm
}

func (c *uploadCluster) chunkPath(workerID, chunkID string) string {
	return filepath.Join(c.byID[workerID].dataDir, "chunks", chunkID)
}

func (c *uploadCluster) wantDownload(objectID string, data []byte) {
	c.t.Helper()
	r := c.download(objectID)
	if r.status != http.StatusOK || r.readErr != nil || !bytes.Equal(r.body, data) {
		c.t.Fatalf("download: status %d, readErr %v, %d bytes (want %d), equal=%v, err %+v",
			r.status, r.readErr, len(r.body), len(data), bytes.Equal(r.body, data), r.err)
	}
	if got := r.header.Get(wire.HeaderObjectChecksum); got != sha(data) {
		c.t.Fatalf("checksum header %q, want %q", got, sha(data))
	}
}

// TestDownloadRoundTrip is invariant I6: downloaded bytes equal uploaded
// bytes, for edge sizes and the 5.5 MB checkpoint object.
func TestDownloadRoundTrip(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4})
	cs := int(config.DefaultChunkSize)
	for _, n := range []int{0, 1, cs - 1, cs, cs + 1, 5_500_000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			data := randomBytes(t, n)
			id := c.mustUpload(data)
			c.wantDownload(id, data)
			r := c.download(id)
			if r.header.Get("Content-Length") != fmt.Sprint(n) || r.header.Get("Content-Type") != "application/octet-stream" ||
				r.header.Get("ETag") != `"`+sha(data)+`"` || r.header.Get("Content-Disposition") != "attachment; filename=obj.bin" {
				t.Fatalf("headers = %v", r.header)
			}
		})
	}
}

// TestDownloadWithWorkerStopped: a worker holding replicas stops. Before it
// is declared DEAD, reads fail over to the other replica; after, it is no
// longer tried. Either way the bytes are correct.
func TestDownloadWithWorkerStopped(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	data := randomBytes(t, 20_000)
	id := c.mustUpload(data)
	victim := c.chunkMap(id)[0].Replicas[0].WorkerID
	c.byID[victim].stop()

	c.wantDownload(id, data) // victim may still be ALIVE: fallback path
	waitFor(t, clusterLimit, victim+" DEAD", func() bool {
		w, _ := c.reg.Get(victim)
		return w.State == "DEAD"
	})
	c.wantDownload(id, data) // victim excluded: pre-flight uses only effective replicas
	for _, ch := range c.chunkMap(id) {
		for _, r := range ch.Replicas {
			if r.State != meta.ReplicaOK {
				t.Fatalf("an unreachable worker's replica was marked %s: %+v", r.State, r)
			}
		}
	}
}

// TestDownloadSkipsAndMarksCorruptReplica: each kind of on-disk damage to one
// replica is detected; the download is still correct; the replica becomes
// CORRUPT; the other replica and the damaged file are not otherwise touched.
func TestDownloadSkipsAndMarksCorruptReplica(t *testing.T) {
	damage := map[string]func(path string) error{
		"append bytes": func(p string) error {
			f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.WriteString("garbage")
			return err
		},
		"flip a byte": func(p string) error {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			b[len(b)-1] ^= 0xff
			return os.WriteFile(p, b, 0o600)
		},
		"truncate":       func(p string) error { return os.Truncate(p, 100) },
		"delete file":    os.Remove,
		"destroy header": func(p string) error { return os.WriteFile(p, []byte("junk"), 0o600) },
	}
	for name, fn := range damage {
		t.Run(name, func(t *testing.T) {
			c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
			data := randomBytes(t, 5000)
			id := c.mustUpload(data)
			ch := c.chunkMap(id)[2]
			bad, good := ch.Replicas[0].WorkerID, ch.Replicas[1].WorkerID
			if err := fn(c.chunkPath(bad, ch.ID)); err != nil {
				t.Fatal(err)
			}
			// Repeat until the random replica order has hit the bad copy.
			waitFor(t, clusterLimit, "corruption detected", func() bool {
				c.wantDownload(id, data)
				for _, r := range c.chunkMap(id)[2].Replicas {
					if r.WorkerID == bad && r.State == meta.ReplicaCorrupt {
						return true
					}
				}
				return false
			})
			for _, r := range c.chunkMap(id)[2].Replicas {
				if r.WorkerID == good && r.State != meta.ReplicaOK {
					t.Fatalf("good replica changed: %+v", r)
				}
			}
			c.wantDownload(id, data)
		})
	}
}

func corruptFile(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestDownloadBothReplicasCorrupt: with no valid copy left, nothing wrong is
// ever returned: chunk 0 → clean 503; a later chunk → truncated body.
func TestDownloadBothReplicasCorrupt(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	data := randomBytes(t, 5000)

	t.Run("first chunk", func(t *testing.T) {
		id := c.mustUpload(data)
		ch := c.chunkMap(id)[0]
		for _, r := range ch.Replicas {
			corruptFile(t, c.chunkPath(r.WorkerID, ch.ID))
		}
		for range 2 { // second time: refused at pre-flight (both now CORRUPT)
			r := c.download(id)
			if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeStorageUnavailable {
				t.Fatalf("got %d %+v", r.status, r.err)
			}
		}
	})
	t.Run("last chunk", func(t *testing.T) {
		id := c.mustUpload(data)
		ch := c.chunkMap(id)[4]
		for _, r := range ch.Replicas {
			corruptFile(t, c.chunkPath(r.WorkerID, ch.ID))
		}
		r := c.download(id)
		if r.status != http.StatusOK || r.readErr == nil {
			t.Fatalf("got %d, readErr %v; want 200 then a truncated body", r.status, r.readErr)
		}
		if len(r.body) >= len(data) || !bytes.Equal(r.body, data[:len(r.body)]) {
			t.Fatalf("delivered %d bytes that are not a strict prefix of the object", len(r.body))
		}
		// Pre-flight now refuses it outright.
		if r := c.download(id); r.status != http.StatusServiceUnavailable {
			t.Fatalf("second download: %d", r.status)
		}
	})
}

// TestDownloadBothHoldersDown: a chunk whose two holders are DEAD makes the
// object unavailable (503 at pre-flight), not corrupt.
func TestDownloadBothHoldersDown(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	data := randomBytes(t, 3000)
	id := c.mustUpload(data)
	reps := c.chunkMap(id)[0].Replicas
	for _, r := range reps {
		c.byID[r.WorkerID].stop()
	}
	waitFor(t, clusterLimit, "both holders DEAD", func() bool {
		for _, r := range reps {
			if w, _ := c.reg.Get(r.WorkerID); w.State != "DEAD" {
				return false
			}
		}
		return true
	})
	r := c.download(id)
	if r.status != http.StatusServiceUnavailable || r.err.Error.Code != wire.CodeStorageUnavailable {
		t.Fatalf("got %d %+v", r.status, r.err)
	}
	for _, ch := range c.chunkMap(id) {
		for _, rep := range ch.Replicas {
			if rep.State != meta.ReplicaOK {
				t.Fatalf("unavailable replica marked %s", rep.State)
			}
		}
	}
	// One holder returns: the object is readable again.
	c.byID[reps[0].WorkerID].start()
	waitFor(t, clusterLimit, "holder ALIVE", func() bool {
		w, _ := c.reg.Get(reps[0].WorkerID)
		return w.State == "ALIVE"
	})
	c.wantDownload(id, data)
}

func TestDownloadNotFoundCases(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	if r := c.download("nope"); r.status != 400 || r.err.Error.Code != wire.CodeInvalidObjectID {
		t.Fatalf("malformed: %d %+v", r.status, r.err)
	}
	if r := c.download("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"); r.status != 404 || r.err.Error.Code != wire.CodeObjectNotFound {
		t.Fatalf("unknown: %d %+v", r.status, r.err)
	}
	// An upload in progress is invisible.
	payload := randomBytes(t, 5000)
	ct, body := multipartBody(t, filePart("f", payload))
	pr, pw := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, c.url+"/api/v1/objects", pr)
	req.Header.Set("Content-Type", ct)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	pw.Write(body[:2500])
	var uploading string
	waitFor(t, clusterLimit, "upload in progress", func() bool {
		cnt, _ := c.meta.Counts(context.Background())
		return cnt.ObjectsByStatus[meta.StatusUploading] == 1 && cnt.Chunks > 0
	})
	inv, _ := c.workers[0].store.Inventory()
	if len(inv) > 0 {
		uploading = inv[0].ChunkID[:36]
	} else {
		inv, _ = c.workers[1].store.Inventory()
		uploading = inv[0].ChunkID[:36]
	}
	if r := c.download(uploading); r.status != 404 {
		t.Fatalf("UPLOADING object: %d", r.status)
	}
	pw.Write(body[2500:])
	pw.Close()
	<-done
	c.wantDownload(uploading, payload) // committed now: visible and intact
}

func TestConcurrentDownloads(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	objs := map[string][]byte{}
	for i := range 3 {
		d := randomBytes(t, 7000+i*1000)
		objs[c.mustUpload(d)] = d
	}
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for range 8 {
		for id, d := range objs {
			wg.Go(func() {
				r := c.download(id)
				if r.status != 200 || !bytes.Equal(r.body, d) {
					errs <- fmt.Sprintf("%s: %d, equal=%v", id, r.status, bytes.Equal(r.body, d))
				}
			})
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
