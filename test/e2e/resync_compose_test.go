//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"objstore/internal/wire"
)

type auditReport struct {
	OK          bool `json:"ok"`
	SteadyState bool `json:"steady_state"`
	Checks      []struct {
		ID         string `json:"id"`
		Violations int    `json:"violations"`
	} `json:"checks"`
}

func (p *composeProject) getJSON(t *testing.T, path string, v any) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", p.port, path))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func (a auditReport) violations(id string) int {
	for _, c := range a.Checks {
		if c.ID == id {
			return c.Violations
		}
	}
	return -1
}

// TestComposeResync is the M6 checkpoint against the real stack: a stopped
// worker leaves chunks under-replicated but service continues; a return
// within the grace period restores everything with no data movement; a
// return with a wiped volume has its missing replicas detected and dropped;
// the audit's safety invariants hold throughout.
func TestComposeResync(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "REPAIR_GRACE=60s", "JANITOR_INTERVAL=1s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)

	data := make([]byte, 4_000_000)
	rand.Read(data)
	status, info, e := postObject(t, p.port, "r.bin", data, "")
	if status != http.StatusCreated {
		t.Fatalf("upload: %d %+v", status, e)
	}
	var md wire.ObjectMetadata
	p.getJSON(t, "/api/v1/objects/"+info.ObjectID+"/metadata", &md)
	count := map[string]int{}
	for _, ch := range md.Chunks {
		for _, r := range ch.Replicas {
			count[r.WorkerID]++
		}
	}
	x, n := "", 0
	for w, c := range count {
		if c > n {
			x, n = w, c
		}
	}
	t.Logf("%s holds %d of the 8 replicas", x, n)

	cluster := func() wire.ClusterView {
		var v wire.ClusterView
		p.getJSON(t, "/cluster", &v)
		return v
	}
	audit := func() auditReport {
		var a auditReport
		p.getJSON(t, "/admin/audit", &a)
		return a
	}
	if s := cluster().Storage.Chunks; s.Total != 4 || s.FullyReplicated != 4 {
		t.Fatalf("healthy totals = %+v", s)
	}

	filesBefore := p.chunkFiles(x, info.ObjectID) // taken while the worker runs

	t.Run("stopped worker: degraded but serving", func(t *testing.T) {
		docker(t, "stop", p.container(x))
		p.waitStates(30*time.Second, map[string]string{x: "DEAD"})
		if s := cluster().Storage.Chunks; s.UnderReplicated != n {
			t.Fatalf("totals = %+v, want %d under-replicated", s, n)
		}
		st, body, e, _ := getObject(t, p.port, info.ObjectID)
		if st != 200 || !bytes.Equal(body, data) {
			t.Fatalf("download while degraded: %d %+v", st, e)
		}
		if st, _, e := postObject(t, p.port, "new.bin", []byte("written while degraded"), ""); st != 201 {
			t.Fatalf("upload while degraded: %d %+v", st, e)
		}
		if a := audit(); !a.OK || a.SteadyState || a.violations("I5") != n {
			t.Fatalf("audit while degraded: %+v", a)
		}
	})

	t.Run("return within grace: no data movement", func(t *testing.T) {
		docker(t, "start", p.container(x))
		p.waitStates(60*time.Second, allAlive)
		v := cluster()
		if s := v.Storage; s.Chunks.UnderReplicated != 0 || s.PendingDeletes != 0 {
			t.Fatalf("after return: %+v", s)
		}
		if after := p.chunkFiles(x, info.ObjectID); len(after) != len(filesBefore) || len(after) != n {
			t.Fatalf("files on %s changed: %v → %v", x, filesBefore, after)
		}
		if a := audit(); !a.OK || !a.SteadyState {
			t.Fatalf("audit after return: %+v", a)
		}
	})

	t.Run("return with a wiped volume: missing replicas dropped", func(t *testing.T) {
		var v0 wire.ClusterView = cluster()
		var oldEpoch uint64
		for _, w := range v0.Workers {
			if w.WorkerID == x {
				oldEpoch = w.Epoch
			}
		}
		docker(t, "stop", p.container(x))
		docker(t, "run", "--rm", "-v", p.name+"_"+x+"-data:/data", "alpine:3.24", "sh", "-c", "rm -f /data/chunks/*")
		docker(t, "start", p.container(x))
		waitFor(t, 60*time.Second, x+" re-registered", func() bool {
			for _, w := range cluster().Workers {
				if w.WorkerID == x && w.State == "ALIVE" && w.Epoch > oldEpoch {
					return true
				}
			}
			return false
		})
		var md wire.ObjectMetadata
		p.getJSON(t, "/api/v1/objects/"+info.ObjectID+"/metadata", &md)
		for _, ch := range md.Chunks {
			for _, r := range ch.Replicas {
				if r.WorkerID == x {
					t.Fatalf("replica row on wiped %s remains: %+v", x, r)
				}
			}
		}
		if s := cluster().Storage.Chunks; s.UnderReplicated != n {
			t.Fatalf("totals = %+v, want %d under-replicated", s, n)
		}
		if a := audit(); !a.OK || a.violations("I5") != n {
			t.Fatalf("audit after wipe: %+v", a)
		}
		st, body, e, _ := getObject(t, p.port, info.ObjectID)
		if st != 200 || !bytes.Equal(body, data) {
			t.Fatalf("download after wipe: %d %+v", st, e)
		}
	})
}
