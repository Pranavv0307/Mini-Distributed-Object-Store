package server

import (
	"context"
	"sync"
)

// writeGate admits or refuses mutating requests (chunk PUT and DELETE) and
// can be closed in a way that waits for admitted requests to finish.
//
// Why draining matters (DEC-006): before (re-)registering, the worker closes
// the gate and then snapshots its inventory. If a PUT admitted just before
// the close were still running, it could rename its chunk into place after
// the snapshot was taken, so the inventory would miss a chunk that is on
// disk. Close therefore returns only when no admitted request is still in
// flight; every write is either in the snapshot or was refused.
type writeGate struct {
	mu       sync.Mutex
	open     bool
	inflight int
	drained  chan struct{} // closed when inflight reaches 0 while draining; nil otherwise
}

// enter admits a mutating request if the gate is open. Every successful
// enter must be paired with exactly one exit.
func (g *writeGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return false
	}
	g.inflight++
	return true
}

func (g *writeGate) exit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
	if g.inflight == 0 && g.drained != nil {
		close(g.drained)
		g.drained = nil
	}
}

// setOpen opens the gate. (Closing goes through closeAndDrain.)
func (g *writeGate) setOpen() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = true
}

// closeAndDrain closes the gate immediately (new requests are refused) and
// waits until all admitted requests have finished, or ctx ends. The gate
// stays closed even if ctx ends first.
func (g *writeGate) closeAndDrain(ctx context.Context) error {
	g.mu.Lock()
	g.open = false
	if g.inflight == 0 {
		g.mu.Unlock()
		return nil
	}
	if g.drained == nil {
		g.drained = make(chan struct{})
	}
	ch := g.drained
	g.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *writeGate) isOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}
