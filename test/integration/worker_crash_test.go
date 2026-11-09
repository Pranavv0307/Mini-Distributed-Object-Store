// Package integration holds multi-component tests that run real processes or
// in-process clusters. They run as part of `go test ./...`; use -short to
// skip them.
package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"objstore/internal/config"
	"objstore/internal/wire"
)

const testObj = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"

// buildWorker compiles cmd/worker into a temp dir and returns its path.
func buildWorker(t testing.TB) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "worker")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	out, err := exec.Command("go", "build", "-o", bin, "objstore/cmd/worker").CombinedOutput()
	if err != nil {
		t.Fatalf("go build worker: %v\n%s", err, out)
	}
	return bin
}

func freePort(t testing.TB) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

type workerProc struct {
	cmd  *exec.Cmd
	base string
}

func startWorker(t testing.TB, bin, dataDir string, port int) *workerProc {
	t.Helper()
	logFile, err := os.OpenFile(filepath.Join(t.TempDir(), "worker.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logFile.Close() })
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"WORKER_ID=worker1",
		fmt.Sprintf("LISTEN_ADDR=127.0.0.1:%d", port),
		"DATA_DIR="+dataDir,
		"FSYNC_ON_WRITE=true",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &workerProc{cmd: cmd, base: fmt.Sprintf("http://127.0.0.1:%d", port)}
	t.Cleanup(func() { _ = p.cmd.Process.Kill(); _, _ = p.cmd.Process.Wait() })

	waitFor(t, 15*time.Second, "worker healthz", func() bool {
		resp, err := http.Get(p.base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return p
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func putChunk(t testing.TB, base, id string, data []byte) {
	t.Helper()
	sum := sha256.Sum256(data)
	req, _ := http.NewRequest(http.MethodPut, base+"/internal/chunks/"+id, bytes.NewReader(data))
	req.Header.Set(wire.HeaderChunkSHA256, hex.EncodeToString(sum[:]))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT %s status = %d", id, resp.StatusCode)
	}
}

// TestWorkerKilledMidWriteLeavesNoPartialChunk is the M1 checkpoint "kill the
// process during a write; no half-chunk is visible after restart". It kills
// the real worker process (SIGKILL / TerminateProcess) while a chunk body is
// half transferred and the temp file is on disk.
func TestWorkerKilledMidWriteLeavesNoPartialChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the worker binary")
	}
	bin := buildWorker(t)
	dataDir := t.TempDir()
	port := freePort(t)
	w := startWorker(t, bin, dataDir, port)

	committedID, partialID := testObj+"_000000", testObj+"_000001"
	committed := bytes.Repeat([]byte("committed "), 50_000) // 500 KB
	putChunk(t, w.base, committedID, committed)

	// Start a full-size (CHUNK_SIZE) PUT but send only the first 256 KiB.
	full := bytes.Repeat([]byte{0xab}, int(config.DefaultChunkSize))
	sum := sha256.Sum256(full)
	pr, pw := io.Pipe()
	req, _ := http.NewRequest(http.MethodPut, w.base+"/internal/chunks/"+partialID, pr)
	req.ContentLength = int64(len(full))
	req.Header.Set(wire.HeaderChunkSHA256, hex.EncodeToString(sum[:]))
	clientDone := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		clientDone <- err
	}()
	if _, err := pw.Write(full[:256<<10]); err != nil {
		t.Fatal(err)
	}

	// Wait until the worker has written part of the body to its temp file.
	// os.Stat on the path is used rather than DirEntry.Info: on Windows the
	// directory entry's cached size is not updated while the file is open.
	tmpDir := filepath.Join(dataDir, "tmp")
	waitFor(t, 10*time.Second, "partial temp file", func() bool {
		es, _ := os.ReadDir(tmpDir)
		for _, e := range es {
			if info, err := os.Stat(filepath.Join(tmpDir, e.Name())); err == nil && info.Size() > 64<<10 {
				return true
			}
		}
		return false
	})

	if err := w.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = w.cmd.Process.Wait()
	pw.CloseWithError(io.ErrClosedPipe)
	<-clientDone // the client sees a connection error; the value is irrelevant

	if es, _ := os.ReadDir(tmpDir); len(es) == 0 {
		t.Fatal("expected a leftover temp file after the kill (test did not hit mid-write)")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "chunks", partialID)); !os.IsNotExist(err) {
		t.Fatalf("partial chunk is visible in chunks/ after crash: %v", err)
	}

	// Restart on the same data directory.
	w2 := startWorker(t, bin, dataDir, port)
	if es, _ := os.ReadDir(tmpDir); len(es) != 0 {
		t.Fatalf("temp files survived restart: %d", len(es))
	}
	resp, err := http.Get(w2.base + "/internal/workers/worker1/chunks")
	if err != nil {
		t.Fatal(err)
	}
	var inv wire.InventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(inv.Chunks) != 1 || inv.Chunks[0] != (wire.ChunkInfo{ChunkID: committedID, Size: int64(len(committed))}) {
		t.Fatalf("inventory after restart = %+v; want only the committed chunk", inv.Chunks)
	}

	resp, err = http.Get(w2.base + "/internal/chunks/" + committedID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, committed) {
		t.Fatal("committed chunk changed across the crash")
	}
	resp, err = http.Get(w2.base + "/internal/chunks/" + partialID)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET partial chunk status = %d, want 404", resp.StatusCode)
	}
}
