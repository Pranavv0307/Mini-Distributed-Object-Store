//go:build e2e

// Package e2e runs end-to-end tests against real Docker containers.
//
//	go test -tags e2e -count=1 -v ./test/e2e/...
//
// Requires a running Docker engine. The image is built from the repository
// root as objstore:e2e unless E2E_IMAGE names an existing image.
package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"objstore/internal/wire"
)

const testObj = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func image(t *testing.T) string {
	t.Helper()
	if img := os.Getenv("E2E_IMAGE"); img != "" {
		return img
	}
	docker(t, "build", "-t", "objstore:e2e", "../..")
	return "objstore:e2e"
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// container is a worker container whose port 9000 is published on a random
// loopback port. The host port changes on every start, so waitHealthy
// re-reads it and returns the current base URL.
type container struct {
	t    *testing.T
	name string
}

func (c *container) waitHealthy() string {
	var base string
	waitFor(c.t, 30*time.Second, c.name+" healthz", func() bool {
		out, err := exec.Command("docker", "port", c.name, "9000/tcp").Output()
		if err != nil {
			return false
		}
		base = "http://" + strings.TrimSpace(strings.Split(string(out), "\n")[0])
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return base
}

func runWorker(t *testing.T, img, name, volume string) *container {
	t.Helper()
	docker(t, "run", "-d", "--name", name,
		"-e", "WORKER_ID=worker1",
		"-v", volume+":/data",
		"-p", "127.0.0.1::9000",
		img)
	return &container{t: t, name: name}
}

func put(t *testing.T, base, id string, data []byte, sumHex string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, base+"/internal/chunks/"+id, bytes.NewReader(data))
	req.Header.Set(wire.HeaderChunkSHA256, sumHex)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func get(t *testing.T, base, id string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(base + "/internal/chunks/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func sumHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func TestWorkerContainer(t *testing.T) {
	img := image(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	name, volume := "objstore-e2e-worker-"+suffix, "objstore-e2e-data-"+suffix
	docker(t, "volume", "create", volume)
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
		_ = exec.Command("docker", "volume", "rm", "-f", volume).Run()
	})

	c := runWorker(t, img, name, volume)
	base := c.waitHealthy()

	committedID, partialID := testObj+"_000000", testObj+"_000001"
	data := bytes.Repeat([]byte("persist me "), 95_000) // ~1 MB
	if st := put(t, base, committedID, data, sumHex(data)); st != http.StatusCreated {
		t.Fatalf("PUT status = %d", st)
	}
	if st, got := get(t, base, committedID); st != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET after PUT: status %d, equal=%v", st, bytes.Equal(got, data))
	}

	t.Run("wrong checksum rejected", func(t *testing.T) {
		if st := put(t, base, testObj+"_000005", data, sumHex([]byte("other"))); st != http.StatusBadRequest {
			t.Fatalf("PUT with wrong checksum: status %d, want 400", st)
		}
	})

	t.Run("survives docker stop and start", func(t *testing.T) {
		docker(t, "stop", name)
		docker(t, "start", name)
		base = c.waitHealthy()
		if st, got := get(t, base, committedID); st != http.StatusOK || !bytes.Equal(got, data) {
			t.Fatalf("GET after restart: status %d, equal=%v", st, bytes.Equal(got, data))
		}
	})

	t.Run("no partial chunk after kill mid-write", func(t *testing.T) {
		full := bytes.Repeat([]byte{0xcd}, 1<<20)
		pr, pw := io.Pipe()
		req, _ := http.NewRequest(http.MethodPut, base+"/internal/chunks/"+partialID, pr)
		req.ContentLength = int64(len(full))
		req.Header.Set(wire.HeaderChunkSHA256, sumHex(full))
		done := make(chan struct{})
		go func() {
			defer close(done)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}()
		if _, err := pw.Write(full[:256<<10]); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 15*time.Second, "partial temp file in container", func() bool {
			out, err := exec.Command("docker", "exec", name, "find", "/data/tmp", "-type", "f", "-size", "+64k").Output()
			return err == nil && strings.TrimSpace(string(out)) != ""
		})
		docker(t, "kill", name)
		pw.CloseWithError(io.ErrClosedPipe)
		<-done

		docker(t, "start", name)
		base = c.waitHealthy()
		if out := docker(t, "exec", name, "ls", "-A", "/data/tmp"); out != "" {
			t.Fatalf("temp files after restart: %q", out)
		}
		if st, _ := get(t, base, partialID); st != http.StatusNotFound {
			t.Fatalf("partial chunk GET status = %d, want 404", st)
		}
		resp, err := http.Get(base + "/internal/workers/worker1/chunks")
		if err != nil {
			t.Fatal(err)
		}
		var inv wire.InventoryResponse
		if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(inv.Chunks) != 1 || inv.Chunks[0].ChunkID != committedID {
			t.Fatalf("inventory = %+v, want only %s", inv.Chunks, committedID)
		}
	})

	t.Run("inspect tool reports VALID", func(t *testing.T) {
		out := docker(t, "exec", name, "worker", "inspect", "/data/chunks/"+committedID)
		if !strings.Contains(out, "status:           VALID") {
			t.Fatalf("inspect output:\n%s", out)
		}
	})

	t.Run("survives container re-creation on the same volume", func(t *testing.T) {
		docker(t, "rm", "-f", name)
		c = runWorker(t, img, name, volume)
		base = c.waitHealthy()
		if st, got := get(t, base, committedID); st != http.StatusOK || !bytes.Equal(got, data) {
			t.Fatalf("GET after re-creation: status %d, equal=%v", st, bytes.Equal(got, data))
		}
	})
}
