package integration

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/repair"
	"objstore/internal/controller/workerclient"
	"objstore/internal/wire"
)

func (c *uploadCluster) steady() bool {
	a := c.audit()
	if !a.OK {
		c.t.Fatalf("safety invariant violated: %+v", a)
	}
	return a.SteadyState
}

func (c *uploadCluster) repairStats() wire.RepairView {
	c.t.Helper()
	v := c.clusterView()
	if v.Repair == nil {
		c.t.Fatal("/cluster has no repair section")
	}
	return *v.Repair
}

// TestSelfHealingAfterWorkerLoss is the M7 checkpoint ("the main demo"):
// a worker stops; nothing is copied during the grace period; after it, the
// chunks it held are re-replicated on the remaining workers until the audit
// is steady again; when the worker returns, its superseded copies are
// deleted and the system is steady with no over-replication.
func TestSelfHealingAfterWorkerLoss(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000, repair: true})
	datas := map[string][]byte{}
	for i := range 3 {
		d := randomBytes(t, 7000+i*1500)
		datas[c.mustUpload(d)] = d
	}
	var ids []string
	for id := range datas {
		ids = append(ids, id)
	}
	if !c.steady() {
		t.Fatal("not steady before the failure")
	}
	x := c.holderWithMostReplicas(ids...)
	lost := len(c.replicasOn(x, ids...))
	stopped := time.Now()
	c.byID[x].stop()
	c.waitWorkerState(x, "DEAD")
	detected := time.Since(stopped)

	// Within the grace period: degraded, waiting, nothing copied.
	if st := c.repairStats(); st.Completed != 0 {
		t.Fatalf("repairs started before the grace period ended: %+v", st)
	}
	waitFor(t, clusterLimit, "chunks reported as waiting for grace", func() bool {
		return c.repairStats().WaitingForGrace == int64(lost)
	})
	for id, d := range datas {
		c.wantDownload(id, d)
	}

	// After the grace period: full RF restored on the remaining workers.
	waitFor(t, 3*clusterLimit, "steady state restored", c.steady)
	healed := time.Since(stopped)
	st := c.repairStats()
	if st.Completed != int64(lost) || st.Failed != 0 || st.Unrecoverable != 0 || st.BytesCopied == 0 {
		t.Fatalf("repair stats = %+v, want %d completed", st, lost)
	}
	if n := len(c.replicasOn(x, ids...)); n != 0 {
		t.Fatalf("%d replica rows remain on the lost worker", n)
	}
	for _, id := range ids {
		for _, ch := range c.chunkMap(id) {
			if len(ch.Replicas) != 2 || ch.Replicas[0].WorkerID == ch.Replicas[1].WorkerID {
				t.Fatalf("chunk %s replicas = %+v", ch.ID, ch.Replicas)
			}
			for _, r := range ch.Replicas {
				got, err := c.byID[r.WorkerID].store.Get(ch.ID)
				if err != nil || sha(got.Payload) != ch.ChecksumSHA256 {
					t.Fatalf("replica %s on %s is not valid: %v", ch.ID, r.WorkerID, err)
				}
			}
		}
	}
	for id, d := range datas {
		c.wantDownload(id, d)
	}
	t.Logf("detection %v; full RF restored %v after the stop (DEAD_AFTER %v, grace %v); %d chunks, %d bytes copied",
		detected.Round(time.Millisecond), healed.Round(time.Millisecond), deadAfter, repairGrace, st.Completed, st.BytesCopied)

	// The worker returns: its superseded copies are deleted; no over-replication.
	c.byID[x].start()
	c.waitReregistered(x, 0)
	waitFor(t, clusterLimit, "returned worker's stale copies deleted", func() bool {
		return c.filesOnWorkers("") == c.countReplicaRows(ids...) && c.clusterView().Storage.PendingDeletes == 0
	})
	if v := c.clusterView().Storage.Chunks; v.OverReplicated != 0 || v.FullyReplicated != v.Total {
		t.Fatalf("after return: %+v", v)
	}
	if !c.steady() {
		t.Fatal("not steady after the worker returned")
	}
}

func (c *uploadCluster) countReplicaRows(ids ...string) int {
	n := 0
	for _, id := range ids {
		for _, ch := range c.chunkMap(id) {
			n += len(ch.Replicas)
		}
	}
	return n
}

// TestNoRepairWithinGrace: a worker that is DEAD for less than REPAIR_GRACE
// causes no copying (D12), and its return needs none.
func TestNoRepairWithinGrace(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	id := c.mustUpload(randomBytes(t, 6000))
	x := c.holderWithMostReplicas(id)
	c.byID[x].stop()
	c.waitWorkerState(x, "DEAD")
	waitFor(t, clusterLimit, "chunks waiting for grace", func() bool { return c.repairStats().WaitingForGrace > 0 })
	after := c.repairStats().Rounds
	waitFor(t, clusterLimit, "five more rounds", func() bool { return c.repairStats().Rounds > after+5 })
	if st := c.repairStats(); st.Completed != 0 || st.InFlight != 0 || st.WaitingForGrace == 0 {
		t.Fatalf("stats = %+v; want no repairs and chunks waiting", st)
	}
	c.byID[x].start()
	c.waitReregistered(x, 0)
	waitFor(t, clusterLimit, "steady", c.steady)
	if st := c.repairStats(); st.Completed != 0 {
		t.Fatalf("copies made although the worker returned within grace: %+v", st)
	}
}

// TestCorruptReplicaRepairedInPlace (D16, DEC-008): a replica found corrupt
// by a download is rewritten on the same worker, with no grace period.
func TestCorruptReplicaRepairedInPlace(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	data := randomBytes(t, 5000)
	id := c.mustUpload(data)
	ch := c.chunkMap(id)[2]
	bad := ch.Replicas[0].WorkerID
	corruptFile(t, c.chunkPath(bad, ch.ID))
	waitFor(t, clusterLimit, "corruption detected by reads", func() bool {
		c.wantDownload(id, data)
		return c.replicasOn(bad, id)[ch.ID] == meta.ReplicaCorrupt || c.repairStats().Completed > 0
	})
	waitFor(t, clusterLimit, "steady after in-place repair", c.steady)
	if st := c.replicasOn(bad, id)[ch.ID]; st != meta.ReplicaOK {
		t.Fatalf("replica on %s = %q, want OK (rewritten in place)", bad, st)
	}
	got, err := c.byID[bad].store.Get(ch.ID)
	if err != nil || sha(got.Payload) != ch.ChecksumSHA256 {
		t.Fatalf("file on %s not repaired: %v", bad, err)
	}
	if n := c.clusterView().Storage.PendingDeletes; n != 0 {
		t.Fatalf("%d pending deletes after an in-place repair", n)
	}
}

// TestWipedWorkerRepairedImmediately: rows dropped by reconciliation are lost
// copies, not "may come back", so they are repaired without a grace period.
func TestWipedWorkerRepairedImmediately(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true, repairGrace: time.Hour})
	data := randomBytes(t, 8000)
	id := c.mustUpload(data)
	x := c.holderWithMostReplicas(id)
	c.restart(x, (*testWorker).wipe)
	waitFor(t, clusterLimit, "steady after repair", c.steady)
	c.wantDownload(id, data)
}

// TestReturningHolderRescuesUnrecoverableChunk: the only effective copy turns
// out to be corrupt while the other holder is gone beyond grace. Nothing can
// be repaired (reported unrecoverable, reads 503); when the other holder
// returns, its intact copy is used to repair and the object is whole again.
func TestReturningHolderRescuesUnrecoverableChunk(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000, repair: true})
	data := randomBytes(t, 3000)
	id := c.mustUpload(data)
	a, b := c.workers[0], c.workers[1]
	b.stop()
	c.waitWorkerState(b.id, "DEAD")
	ch := c.chunkMap(id)[1]
	corruptFile(t, c.chunkPath(a.id, ch.ID))
	// The download marks a's copy CORRUPT; with b lost, chunk 1 has no valid copy.
	waitFor(t, 3*clusterLimit, "chunk reported unrecoverable", func() bool {
		c.download(id)
		return c.repairStats().Unrecoverable == 1
	})
	if r := c.download(id); r.status != 503 {
		t.Fatalf("download of an unrecoverable object: %d", r.status)
	}
	b.start()
	c.waitReregistered(b.id, 0)
	waitFor(t, 3*clusterLimit, "steady after the holder returned", c.steady)
	c.wantDownload(id, data)
	if st := c.repairStats(); st.Unrecoverable != 0 {
		t.Fatalf("still unrecoverable: %+v", st)
	}
}

// TestOverReplicationTrimmed: a chunk with a third valid copy is trimmed back
// to RF, and the extra file is deleted.
func TestOverReplicationTrimmed(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000, repair: true})
	data := randomBytes(t, 2000)
	id := c.mustUpload(data)
	ch := c.chunkMap(id)[0]
	holders := map[string]bool{ch.Replicas[0].WorkerID: true, ch.Replicas[1].WorkerID: true}
	var third string
	for _, w := range c.workers {
		if !holders[w.id] {
			third = w.id
		}
	}
	// Create a valid third copy the way a repair would.
	ctx := context.Background()
	if err := c.meta.ClaimRepair(ctx, id, ch.ID, third, false); err != nil {
		t.Fatal(err)
	}
	if err := putDirect(t, c.byID[third], ch.ID, data[:1000]); err != nil {
		t.Fatal(err)
	}
	if err := c.meta.CompleteRepair(ctx, id, ch.ID, third, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, clusterLimit, "trimmed and steady", func() bool {
		v := c.clusterView()
		return v.Storage.Chunks.OverReplicated == 0 && v.Storage.PendingDeletes == 0 && v.Repair.Trimmed == 1
	})
	if n := len(c.chunkMap(id)[0].Replicas); n != 2 {
		t.Fatalf("%d replicas after trimming", n)
	}
	if got := c.filesOnWorkers(id); got != 4 {
		t.Fatalf("%d files after trimming, want 4", got)
	}
	c.wantDownload(id, data)
}

// countingClient tracks the maximum number of concurrent repair writes.
type countingClient struct {
	repair.Client
	cur, peak atomic.Int64
}

func (c *countingClient) PutChunk(ctx context.Context, url, id string, data []byte, sum [sha256.Size]byte) error {
	n := c.cur.Add(1)
	for {
		p := c.peak.Load()
		if n <= p || c.peak.CompareAndSwap(p, n) {
			break
		}
	}
	defer c.cur.Add(-1)
	time.Sleep(30 * time.Millisecond) // keep writes overlapping
	return c.Client.PutChunk(ctx, url, id, data, sum)
}

// TestRepairConcurrencyIsBounded: at most MAX_CONCURRENT_REPAIRS copies are
// written at once (D14 throttle).
func TestRepairConcurrencyIsBounded(t *testing.T) {
	var cc *countingClient
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000, repair: true, maxRepairs: 2,
		repairClient: func(inner repair.Client) repair.Client { cc = &countingClient{Client: inner}; return cc }})
	id := c.mustUpload(randomBytes(t, 20_000))
	x := c.holderWithMostReplicas(id)
	c.restart(x, (*testWorker).wipe) // lost copies, repaired without grace
	waitFor(t, 3*clusterLimit, "steady", c.steady)
	if p := cc.peak.Load(); p < 1 || p > 2 {
		t.Fatalf("peak concurrent repair writes = %d, want 1..2", p)
	}
	t.Logf("peak concurrent repair writes: %d (limit 2)", cc.peak.Load())
}

// TestNoMassRepairAfterControllerRestart: right after a controller restart
// the registry is empty. Workers it has not heard from yet are treated as
// DEAD since the restart, so nothing is repaired until DEAD_AFTER +
// REPAIR_GRACE have passed (architecture §4.10).
func TestNoMassRepairAfterControllerRestart(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	c.mustUpload(randomBytes(t, 5000))

	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	fresh := registry.New(registry.Options{DeadAfter: deadAfter, Now: clock}) // "restarted" controller
	rp := repair.New(c.meta, fresh, workerclient.New(time.Second, time.Second), placement.Random{}, repair.Config{
		MaxConcurrent: 4, RepairGrace: repairGrace, DeadAfter: deadAfter, ReplicationFactor: 2})
	res, err := rp.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Repaired != 0 || res.Failed != 0 || res.Unrecoverable != 0 || res.WaitingForGrace != 5 {
		t.Fatalf("right after restart: %+v; want all 5 chunks waiting, nothing repaired", res)
	}
	mu.Lock()
	now = now.Add(deadAfter + repairGrace + time.Second)
	mu.Unlock()
	res, _ = rp.RunOnce(context.Background())
	if res.Unrecoverable != 5 || res.Repaired != 0 {
		t.Fatalf("after grace with no worker back: %+v; want 5 unrecoverable", res)
	}
}
