//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"objstore/internal/wire"
)

func postObject(t *testing.T, port int, name string, data []byte, metadata string) (int, wire.ObjectInfo, wire.ErrorResponse) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if metadata != "" {
		if err := mw.WriteField("metadata", metadata); err != nil {
			t.Fatal(err)
		}
	}
	fw, err := mw.CreateFormFile("file", name)
	if err != nil {
		t.Fatal(err)
	}
	fw.Write(data)
	mw.Close()
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/api/v1/objects", port), mw.FormDataContentType(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var info wire.ObjectInfo
	var e wire.ErrorResponse
	if resp.StatusCode == http.StatusCreated {
		json.NewDecoder(resp.Body).Decode(&info)
	} else {
		json.NewDecoder(resp.Body).Decode(&e)
	}
	return resp.StatusCode, info, e
}

// chunkFiles lists the chunk files of objectID on a worker container.
func (p *composeProject) chunkFiles(worker, objectID string) []string {
	p.t.Helper()
	out, err := exec.Command("docker", "exec", p.container(worker), "sh", "-c",
		"ls /data/chunks | grep '^"+objectID+"_' || true").Output()
	if err != nil {
		p.t.Fatalf("list chunks on %s: %v", worker, err)
	}
	return strings.Fields(string(out))
}

// TestComposeUpload is the M3 checkpoint against the real stack: a 5.5 MB
// upload becomes 6 chunks, each stored as a file on exactly 2 distinct worker
// volumes (12 files), named by chunk ID; with only one worker alive an upload
// is refused with 503 and leaves nothing behind.
func TestComposeUpload(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "JANITOR_INTERVAL=1s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)

	data := make([]byte, 5_500_000)
	rand.Read(data)
	sum := sha256.Sum256(data)
	status, info, e := postObject(t, p.port, "big.bin", data, `{"owner":"e2e"}`)
	if status != http.StatusCreated {
		t.Fatalf("upload status %d: %+v", status, e)
	}
	if info.NumChunks != 6 || info.Size != 5_500_000 || info.ChecksumSHA256 != hex.EncodeToString(sum[:]) ||
		info.CustomMetadata["owner"] != "e2e" {
		t.Fatalf("upload response = %+v", info)
	}

	holders := map[string][]string{} // chunk ID → workers holding a file
	total := 0
	for _, w := range []string{"worker1", "worker2", "worker3", "worker4"} {
		files := p.chunkFiles(w, info.ObjectID)
		total += len(files)
		for _, f := range files {
			holders[f] = append(holders[f], w)
		}
	}
	if total != 12 || len(holders) != 6 {
		t.Fatalf("%d chunk files for %d chunks, want 12 for 6: %v", total, len(holders), holders)
	}
	for idx := range 6 {
		id := fmt.Sprintf("%s_%06d", info.ObjectID, idx)
		if hs := holders[id]; len(hs) != 2 || hs[0] == hs[1] {
			t.Fatalf("chunk %s held by %v, want 2 distinct workers", id, hs)
		}
	}
	t.Logf("chunk placement: %v", holders)

	t.Run("one worker alive: 503 and nothing stored", func(t *testing.T) {
		for _, w := range []string{"worker2", "worker3", "worker4"} {
			docker(t, "stop", p.container(w))
		}
		p.waitStates(30*time.Second, map[string]string{"worker1": "ALIVE", "worker2": "DEAD", "worker3": "DEAD", "worker4": "DEAD"})
		before := len(p.chunkFilesAll("worker1"))
		status, _, e := postObject(t, p.port, "small.bin", []byte("hello"), "")
		if status != http.StatusServiceUnavailable || e.Error.Code != wire.CodeInsufficientWorkers {
			t.Fatalf("got %d %+v", status, e)
		}
		if after := len(p.chunkFilesAll("worker1")); after != before {
			t.Fatalf("worker1 chunk files changed from %d to %d", before, after)
		}
	})
}

func (p *composeProject) chunkFilesAll(worker string) []string {
	p.t.Helper()
	out, err := exec.Command("docker", "exec", p.container(worker), "ls", "/data/chunks").Output()
	if err != nil {
		p.t.Fatalf("ls on %s: %v", worker, err)
	}
	return strings.Fields(string(out))
}
