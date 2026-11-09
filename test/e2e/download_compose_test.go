//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"objstore/internal/wire"
)

func getObject(t *testing.T, port int, id string) (int, []byte, wire.ErrorResponse, error) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/objects/%s", port, id))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	var e wire.ErrorResponse
	if resp.StatusCode != http.StatusOK {
		json.Unmarshal(body, &e)
	}
	return resp.StatusCode, body, e, readErr
}

// holders returns the workers whose volume holds chunkID.
func (p *composeProject) holders(chunkID string, workers ...string) []string {
	var out []string
	for _, w := range workers {
		for _, f := range p.chunkFilesAll(w) {
			if f == chunkID {
				out = append(out, w)
			}
		}
	}
	return out
}

// TestComposeDownload is the M4 checkpoint against the real stack: download
// matches the upload; it still matches with a holder stopped and with one
// replica altered on disk; with both replicas of a chunk altered the download
// is refused (503) and nothing wrong is returned.
func TestComposeDownload(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)
	all := []string{"worker1", "worker2", "worker3", "worker4"}

	data := make([]byte, 3_500_000)
	rand.Read(data)
	status, info, e := postObject(t, p.port, "d.bin", data, "")
	if status != http.StatusCreated {
		t.Fatalf("upload: %d %+v", status, e)
	}
	want := func(t *testing.T) {
		t.Helper()
		st, body, e, readErr := getObject(t, p.port, info.ObjectID)
		if st != http.StatusOK || readErr != nil || !bytes.Equal(body, data) {
			t.Fatalf("download: %d, readErr %v, equal=%v, err %+v", st, readErr, bytes.Equal(body, data), e)
		}
	}
	want(t)

	chunk1 := fmt.Sprintf("%s_%06d", info.ObjectID, 1)
	h := p.holders(chunk1, all...)
	if len(h) != 2 {
		t.Fatalf("chunk 1 holders = %v", h)
	}

	t.Run("holder stopped", func(t *testing.T) {
		docker(t, "stop", p.container(h[0]))
		want(t) // before DEAD: fallback
		p.waitStates(30*time.Second, map[string]string{h[0]: "DEAD"})
		want(t) // after DEAD: excluded
		docker(t, "start", p.container(h[0]))
		p.waitStates(60*time.Second, allAlive)
	})

	t.Run("one replica corrupted on disk", func(t *testing.T) {
		docker(t, "exec", p.container(h[0]), "sh", "-c", "printf garbage >> /data/chunks/"+chunk1)
		for range 6 { // random replica order: several reads hit the bad copy
			want(t)
		}
		out, _ := exec.Command("docker", "logs", p.container("controller")).CombinedOutput()
		if !bytes.Contains(out, []byte(`"msg":"corruption_detected"`)) || !bytes.Contains(out, []byte(chunk1)) {
			t.Fatal("controller did not log corruption_detected for the altered chunk")
		}
	})

	t.Run("both replicas of chunk 0 corrupted", func(t *testing.T) {
		chunk0 := fmt.Sprintf("%s_%06d", info.ObjectID, 0)
		for _, w := range p.holders(chunk0, all...) {
			docker(t, "exec", p.container(w), "sh", "-c", "printf garbage >> /data/chunks/"+chunk0)
		}
		st, body, e, _ := getObject(t, p.port, info.ObjectID)
		if st != http.StatusServiceUnavailable || e.Error.Code != wire.CodeStorageUnavailable {
			t.Fatalf("got %d (%d bytes) %+v", st, len(body), e)
		}
	})
}
