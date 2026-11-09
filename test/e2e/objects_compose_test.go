//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"objstore/internal/wire"
)

// objctl runs the CLI in the Compose "client" container with dir mounted at
// /work and returns combined output.
func (p *composeProject) objctl(dir string, args ...string) (string, error) {
	p.t.Helper()
	base := []string{"compose", "-f", "../../docker-compose.yml", "-p", p.name, "--profile", "tools",
		"run", "--rm", "--no-deps", "-v", dir + ":/work", "client"}
	cmd := exec.Command("docker", append(base, args...)...)
	cmd.Env = p.env
	out, err := cmd.Output()
	if ee, ok := err.(*exec.ExitError); ok {
		return string(out) + string(ee.Stderr), err
	}
	return string(out), err
}

// TestComposeObjectAPI is the M5 checkpoint against the real stack, driven by
// objctl in the client container (spec: "1 or more clients").
func TestComposeObjectAPI(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "JANITOR_INTERVAL=1s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)
	all := []string{"worker1", "worker2", "worker3", "worker4"}

	dir := t.TempDir()
	data := make([]byte, 2_500_000)
	rand.Read(data)
	if err := os.WriteFile(filepath.Join(dir, "in.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := p.objctl(dir, "upload", "/work/in.bin", "-meta", "owner=e2e")
	if err != nil {
		t.Fatalf("upload: %v\n%s", err, out)
	}
	var info wire.ObjectInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil || info.NumChunks != 3 {
		t.Fatalf("upload output: %v\n%s", err, out)
	}

	out, err = p.objctl(dir, "stat", info.ObjectID)
	var md wire.ObjectMetadata
	if err != nil || json.Unmarshal([]byte(out), &md) != nil || len(md.Chunks) != 3 || len(md.Chunks[0].Replicas) != 2 {
		t.Fatalf("stat: %v\n%s", err, out)
	}
	t.Logf("chunk 0 replicas: %+v", md.Chunks[0].Replicas)

	if out, err := p.objctl(dir, "patch", info.ObjectID, "owner=someone", "team=storage"); err != nil ||
		!strings.Contains(out, `"team": "storage"`) || !strings.Contains(out, `"metadata_version": 2`) {
		t.Fatalf("patch: %v\n%s", err, out)
	}
	if out, err := p.objctl(dir, "list"); err != nil || !strings.Contains(out, info.ObjectID) {
		t.Fatalf("list: %v\n%s", err, out)
	}
	if out, err := p.objctl(dir, "download", info.ObjectID, "-o", "/work/out.bin"); err != nil {
		t.Fatalf("download: %v\n%s", err, out)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "out.bin")); !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs")
	}

	t.Run("delete removes the object and its files", func(t *testing.T) {
		if out, err := p.objctl(dir, "delete", info.ObjectID); err != nil {
			t.Fatalf("delete: %v\n%s", err, out)
		}
		if out, err := p.objctl(dir, "stat", info.ObjectID); err == nil || !strings.Contains(out, "OBJECT_NOT_FOUND") {
			t.Fatalf("stat after delete: %v\n%s", err, out)
		}
		waitFor(t, 30*time.Second, "chunk files removed", func() bool {
			for _, w := range all {
				if len(p.chunkFiles(w, info.ObjectID)) != 0 {
					return false
				}
			}
			return true
		})
	})

	t.Run("delete with a holder down completes when it returns", func(t *testing.T) {
		out, err := p.objctl(dir, "upload", "/work/in.bin")
		if err != nil {
			t.Fatalf("upload: %v\n%s", err, out)
		}
		var o wire.ObjectInfo
		json.Unmarshal([]byte(out), &o)
		holder := p.holders(o.ObjectID+"_000000", all...)[0]
		docker(t, "stop", p.container(holder))
		p.waitStates(30*time.Second, map[string]string{holder: "DEAD"})
		if out, err := p.objctl(dir, "delete", o.ObjectID); err != nil {
			t.Fatalf("delete: %v\n%s", err, out)
		}
		waitFor(t, 30*time.Second, "live holders cleaned", func() bool {
			for _, w := range all {
				if w != holder && len(p.chunkFiles(w, o.ObjectID)) != 0 {
					return false
				}
			}
			return true
		})
		docker(t, "start", p.container(holder))
		waitFor(t, 60*time.Second, "returned holder cleaned", func() bool {
			return len(p.chunkFiles(holder, o.ObjectID)) == 0
		})
	})
}
