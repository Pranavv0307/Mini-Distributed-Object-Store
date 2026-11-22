//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"objstore/internal/wire"
)

func (p *composeProject) logs(service string) string {
	out, _ := exec.Command("docker", "logs", p.container(service)).CombinedOutput()
	return string(out)
}

// TestComposeIntegrity is the M8 checkpoint against the real stack, with no
// client read of the damaged copies: the worker scrubber finds a same-size
// corruption, the controller confirms it and repair rewrites the copy in
// place, exactly once; a chunk file deleted from a volume is found by the
// inventory sweep and rewritten.
func TestComposeIntegrity(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "REPAIR_GRACE=60s",
		"RECONCILER_INTERVAL=1s", "JANITOR_INTERVAL=1s", "SCRUB_INTERVAL=2s", "INVENTORY_SWEEP_INTERVAL=2s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)
	all := []string{"worker1", "worker2", "worker3", "worker4"}

	data := make([]byte, 3_500_000)
	rand.Read(data)
	st, info, e := postObject(t, p.port, "i.bin", data, "")
	if st != http.StatusCreated {
		t.Fatalf("upload: %d %+v", st, e)
	}
	audit := func() auditReport {
		var a auditReport
		p.getJSON(t, "/admin/audit", &a)
		if !a.OK {
			t.Fatalf("safety invariant violated: %+v", a)
		}
		return a
	}
	repaired := func() int64 {
		var v wire.ClusterView
		p.getJSON(t, "/cluster", &v)
		return v.Repair.Completed
	}

	t.Run("scrubber finds same-size corruption", func(t *testing.T) {
		chunk := fmt.Sprintf("%s_%06d", info.ObjectID, 1)
		h := p.holders(chunk, all...)[0]
		// Overwrite 4 payload bytes in place: the size stays right, so only
		// a checksum can tell.
		docker(t, "exec", p.container(h), "sh", "-c",
			"printf ZZZZ | dd of=/data/chunks/"+chunk+" bs=1 seek=1000 conv=notrunc 2>/dev/null")
		start := time.Now()
		waitFor(t, 60*time.Second, "scrubber corruption repaired", func() bool { return repaired() >= 1 && audit().SteadyState })
		t.Logf("scrubber detection + in-place repair took %v (SCRUB_INTERVAL 2s)", time.Since(start).Round(100*time.Millisecond))
		logs := p.logs("controller")
		if !strings.Contains(logs, `"detected_by":"scrubber"`) || !strings.Contains(logs, chunk) {
			t.Fatal("controller did not log a scrubber detection for the altered chunk")
		}
		// No churn: two more full scrub passes on that worker, and the
		// chunk is still repaired exactly once.
		passes := strings.Count(p.logs(h), `"msg":"scrub_completed"`)
		waitFor(t, 30*time.Second, "two more scrub passes", func() bool {
			return strings.Count(p.logs(h), `"msg":"scrub_completed"`) >= passes+2
		})
		if n := strings.Count(p.logs("controller"), `"msg":"repair_completed"`); n != 1 {
			t.Fatalf("%d repairs for one corruption, want 1", n)
		}
		if got := strings.Count(p.logs("controller"), `"msg":"corruption_detected"`); got != 1 {
			t.Fatalf("corruption_detected logged %d times, want 1", got)
		}
	})

	t.Run("deleted chunk file found by inventory sweep", func(t *testing.T) {
		chunk := fmt.Sprintf("%s_%06d", info.ObjectID, 2)
		h := p.holders(chunk, all...)[0]
		before := repaired()
		docker(t, "exec", p.container(h), "rm", "/data/chunks/"+chunk)
		waitFor(t, 60*time.Second, "deleted file rewritten", func() bool {
			return repaired() > before && audit().SteadyState && len(p.holders(chunk, h)) == 1
		})
		if !strings.Contains(p.logs("controller"), `"detected_by":"inventory_sweep"`) {
			t.Fatal("controller did not log an inventory_sweep detection")
		}
	})

	st2, body, e2, _ := getObject(t, p.port, info.ObjectID)
	if st2 != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("download after repairs: %d %+v", st2, e2)
	}
}

// TestComposeControllerKilledMidUpload is the M9 checkpoint: the controller
// is killed (SIGKILL) while an upload is streaming, after some chunks are
// stored. On restart, recovery turns the unfinished upload into deletion
// work; the janitor removes every file; the object never becomes visible;
// the audit is steady.
func TestComposeControllerKilledMidUpload(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "JANITOR_INTERVAL=1s")
	p.compose("up", "-d", "--no-build")
	p.waitStates(60*time.Second, allAlive)

	// Stream a multipart upload: 2.5 MB now, then hold the request open.
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		fw, err := mw.CreateFormFile("file", "killed.bin")
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		chunk := make([]byte, 2_500_000)
		rand.Read(chunk)
		if _, err := fw.Write(chunk); err != nil {
			pw.CloseWithError(err)
		}
		// Block here until the test ends the request.
	}()
	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/api/v1/objects", p.port), mw.FormDataContentType(), pr)
		if err == nil {
			resp.Body.Close()
		}
	}()
	defer func() { pw.CloseWithError(errors.New("test over")); <-reqDone }()

	storage := func() *wire.StorageView {
		v, ok := p.cluster()
		if !ok || v.Storage == nil {
			return nil
		}
		return v.Storage
	}
	waitFor(t, 30*time.Second, "chunks of the in-flight upload stored", func() bool {
		s := storage()
		return s != nil && s.ObjectsByStatus["UPLOADING"] == 1 && s.ReplicasByState["OK"] >= 4
	})

	docker(t, "kill", p.container("controller"))
	pw.CloseWithError(errors.New("controller killed"))
	<-reqDone
	docker(t, "start", p.container("controller"))
	p.waitStates(60*time.Second, allAlive)

	if !strings.Contains(p.logs("controller"), `"msg":"startup_recovery"`) {
		t.Fatal("controller did not log startup_recovery")
	}
	waitFor(t, 60*time.Second, "unfinished upload cleaned up", func() bool {
		s := storage()
		if s == nil || len(s.ObjectsByStatus) != 0 || s.PendingDeletes != 0 {
			return false
		}
		for _, w := range []string{"worker1", "worker2", "worker3", "worker4"} {
			if len(p.chunkFilesAll(w)) != 0 {
				return false
			}
		}
		return true
	})
	var list wire.ObjectList
	p.getJSON(t, "/api/v1/objects", &list)
	if len(list.Objects) != 0 {
		t.Fatalf("listing after recovery = %+v, want empty", list.Objects)
	}
	var a auditReport
	p.getJSON(t, "/admin/audit", &a)
	if !a.OK || !a.SteadyState {
		t.Fatalf("audit after recovery: %+v", a)
	}
}
