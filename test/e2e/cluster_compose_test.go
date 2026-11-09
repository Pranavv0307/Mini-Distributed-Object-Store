//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"objstore/internal/wire"
)

// composeProject runs docker compose for an isolated project: a unique
// project name (so volumes and the network are its own), a unique container
// prefix (so it cannot collide with other containers named controller or
// worker1..4), and a free host port.
type composeProject struct {
	t      *testing.T
	name   string
	prefix string
	port   int
	env    []string
}

func newComposeProject(t *testing.T, img string, extraEnv ...string) *composeProject {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n := time.Now().UnixNano()
	p := &composeProject{
		t:      t,
		name:   fmt.Sprintf("objstore-e2e-%d", n),
		prefix: fmt.Sprintf("objstore-e2e-%d-", n),
		port:   port,
	}
	p.env = append(os.Environ(),
		"OBJSTORE_IMAGE="+img,
		"OBJSTORE_CONTAINER_PREFIX="+p.prefix,
		fmt.Sprintf("CONTROLLER_PORT=%d", port),
	)
	p.env = append(p.env, extraEnv...)
	t.Cleanup(func() {
		// Removes only this project's containers, network, and volumes.
		cmd := exec.Command("docker", "compose", "-f", "../../docker-compose.yml", "-p", p.name, "down", "-v", "--remove-orphans")
		cmd.Env = p.env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("compose down: %v\n%s", err, out)
		}
	})
	return p
}

func (p *composeProject) compose(args ...string) string {
	p.t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", "../../docker-compose.yml", "-p", p.name}, args...)...)
	cmd.Env = p.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		p.t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func (p *composeProject) container(service string) string { return p.prefix + service }

func (p *composeProject) cluster() (wire.ClusterView, bool) {
	var v wire.ClusterView
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/cluster", p.port))
	if err != nil {
		return v, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&v) != nil {
		return v, false
	}
	return v, true
}

func (p *composeProject) states() map[string]wire.WorkerView {
	v, _ := p.cluster()
	m := map[string]wire.WorkerView{}
	for _, w := range v.Workers {
		m[w.WorkerID] = w
	}
	return m
}

func (p *composeProject) waitStates(timeout time.Duration, want map[string]string) map[string]wire.WorkerView {
	p.t.Helper()
	var got map[string]wire.WorkerView
	waitFor(p.t, timeout, fmt.Sprintf("worker states %v", want), func() bool {
		got = p.states()
		for id, st := range want {
			if got[id].State != st {
				return false
			}
		}
		return true
	})
	return got
}

var allAlive = map[string]string{"worker1": "ALIVE", "worker2": "ALIVE", "worker3": "ALIVE", "worker4": "ALIVE"}

// TestComposeClusterMembership is the M2 checkpoint against the real Compose
// stack: four workers ALIVE; `docker stop worker2` → DEAD; `docker start
// worker2` → ALIVE; `docker restart controller` → all workers re-register
// without being touched. Timings are shortened (heartbeat 1s, DEAD_AFTER 4s)
// to keep the test fast; the defaults are 3s and 10s.
func TestComposeClusterMembership(t *testing.T) {
	img := image(t)
	p := newComposeProject(t, img, "HEARTBEAT_INTERVAL=1s", "DEAD_AFTER=4s", "REPAIR_GRACE=6s")
	p.compose("up", "-d", "--no-build")

	before := p.waitStates(60*time.Second, allAlive)
	if v, _ := p.cluster(); v.Summary != (wire.WorkerSummary{Total: 4, Alive: 4}) {
		t.Fatalf("summary = %+v", v.Summary)
	}
	for id, w := range before {
		if w.Address != "http://"+id+":9000" {
			t.Fatalf("%s address = %q", id, w.Address)
		}
	}

	t.Run("docker stop worker2 marks it DEAD", func(t *testing.T) {
		stopped := time.Now()
		docker(t, "stop", p.container("worker2"))
		p.waitStates(30*time.Second, map[string]string{"worker2": "DEAD", "worker1": "ALIVE", "worker3": "ALIVE", "worker4": "ALIVE"})
		t.Logf("worker2 DEAD %v after docker stop returned (DEAD_AFTER 4s)", time.Since(stopped))
		waitFor(t, 30*time.Second, "worker2 repair-eligible", func() bool { return p.states()["worker2"].RepairEligible })
	})

	t.Run("docker start worker2 returns it to ALIVE", func(t *testing.T) {
		docker(t, "start", p.container("worker2"))
		after := p.waitStates(60*time.Second, allAlive)
		if after["worker2"].Epoch <= before["worker2"].Epoch {
			t.Fatalf("worker2 epoch %d not newer than %d", after["worker2"].Epoch, before["worker2"].Epoch)
		}
	})

	t.Run("controller restart: workers re-register untouched", func(t *testing.T) {
		pre := p.states()
		docker(t, "restart", p.container("controller"))
		after := p.waitStates(60*time.Second, allAlive)
		for id := range allAlive {
			if after[id].Epoch <= pre[id].Epoch {
				t.Fatalf("%s epoch %d not newer than %d after controller restart", id, after[id].Epoch, pre[id].Epoch)
			}
		}
	})
}
