//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"objstore/internal/wire"
)

// TestComposeSelfHealing is the M7 main demo against the real stack:
// stop the worker holding most replicas; downloads keep working; after
// REPAIR_GRACE the under-replicated chunks are re-replicated on the other
// three workers until the audit is steady; when the worker returns, its
// superseded copies are deleted and the cluster stays steady with no
// over-replication. Detection time and time to full RF are logged.
func TestComposeSelfHealing(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "REPAIR_GRACE=6s",
		"RECONCILER_INTERVAL=1s", "JANITOR_INTERVAL=1s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)

	objs := map[string][]byte{}
	for range 3 {
		d := make([]byte, 3_000_000)
		rand.Read(d)
		st, info, e := postObject(t, p.port, "o.bin", d, "")
		if st != http.StatusCreated {
			t.Fatalf("upload: %d %+v", st, e)
		}
		objs[info.ObjectID] = d
	}
	holds := map[string]int{}
	for id := range objs {
		var md wire.ObjectMetadata
		p.getJSON(t, "/api/v1/objects/"+id+"/metadata", &md)
		for _, ch := range md.Chunks {
			for _, r := range ch.Replicas {
				holds[r.WorkerID]++
			}
		}
	}
	x, lost := "", 0
	for w, n := range holds {
		if n > lost {
			x, lost = w, n
		}
	}
	audit := func() auditReport {
		var a auditReport
		p.getJSON(t, "/admin/audit", &a)
		return a
	}
	cluster := func() wire.ClusterView {
		var v wire.ClusterView
		p.getJSON(t, "/cluster", &v)
		return v
	}
	if a := audit(); !a.OK || !a.SteadyState {
		t.Fatalf("not steady before the failure: %+v", a)
	}

	stopped := time.Now()
	docker(t, "stop", p.container(x))
	p.waitStates(30*time.Second, map[string]string{x: "DEAD"})
	detected := time.Since(stopped)
	if v := cluster(); v.Storage.Chunks.UnderReplicated != lost || v.Repair.Completed != 0 {
		t.Fatalf("right after DEAD: storage %+v repair %+v", v.Storage.Chunks, v.Repair)
	}
	for id, d := range objs {
		if st, body, e, _ := getObject(t, p.port, id); st != 200 || !bytes.Equal(body, d) {
			t.Fatalf("degraded download: %d %+v", st, e)
		}
	}
	waitFor(t, 90*time.Second, "full RF restored", func() bool {
		a := audit()
		if !a.OK {
			t.Fatalf("safety invariant violated: %+v", a)
		}
		return a.SteadyState
	})
	healed := time.Since(stopped)
	v := cluster()
	t.Logf("%s held %d replicas; DEAD after %v; full RF after %v (DEAD_AFTER 4s + grace 6s + copying); repair %+v",
		x, lost, detected.Round(100*time.Millisecond), healed.Round(100*time.Millisecond), *v.Repair)
	if v.Repair.Completed != int64(lost) || v.Repair.Failed != 0 {
		t.Fatalf("repair counters %+v, want %d completed", *v.Repair, lost)
	}
	for id, d := range objs {
		if st, body, e, _ := getObject(t, p.port, id); st != 200 || !bytes.Equal(body, d) {
			t.Fatalf("download after repair: %d %+v", st, e)
		}
	}

	docker(t, "start", p.container(x))
	p.waitStates(60*time.Second, allAlive)
	waitFor(t, 60*time.Second, "returned worker's superseded copies deleted", func() bool {
		for id := range objs {
			if len(p.chunkFiles(x, id)) != 0 {
				return false
			}
		}
		return cluster().Storage.PendingDeletes == 0
	})
	if v := cluster(); v.Storage.Chunks.OverReplicated != 0 || v.Storage.Chunks.FullyReplicated != v.Storage.Chunks.Total {
		t.Fatalf("after return: %+v", v.Storage.Chunks)
	}
	if a := audit(); !a.OK || !a.SteadyState {
		t.Fatalf("audit after return: %+v", a)
	}
}
