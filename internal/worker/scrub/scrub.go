// Package scrub is the worker's background integrity check (architecture
// §3.6 layer 2): it slowly re-reads every committed chunk and checks the
// payload against the length and SHA-256 recorded in the chunk's own header.
// Mismatches are "suspects", reported to the controller on the next
// heartbeat; the controller cross-checks them against its own checksum before
// marking anything CORRUPT (layer 3 is the authority).
//
// A suspect stays reported only while it is still bad: Suspects re-verifies
// each one before returning it, so a chunk the controller has since rewritten
// in place (repair) or deleted stops being reported at the next heartbeat
// instead of at the next full pass (DEC-058).
package scrub

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"objstore/internal/worker/store"
)

// Store is the part of the chunk store the scrubber reads.
type Store interface {
	Inventory() ([]store.Entry, error)
	Get(string) (store.Chunk, error)
}

// Options configures a Scrubber.
type Options struct {
	// Interval between the starts of full passes (SCRUB_INTERVAL). Default 10m.
	Interval time.Duration
	// BytesPerSec throttles a pass (SCRUB_BYTES_PER_SEC); 0 = unthrottled.
	BytesPerSec int64
	Logger      *slog.Logger
}

// Scrubber runs passes and holds the current suspects.
type Scrubber struct {
	store   Store
	opts    Options
	log     *slog.Logger
	mu      sync.Mutex
	suspect map[string]struct{}
}

// New creates a Scrubber.
func New(st Store, opts Options) *Scrubber {
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Minute
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Scrubber{store: st, opts: opts, log: logger, suspect: map[string]struct{}{}}
}

// Run scrubs immediately and then every Interval until ctx ends.
func (s *Scrubber) Run(ctx context.Context) {
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		_ = s.ScrubOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Suspects returns the chunk IDs that failed verification and still fail it
// now. Each latched suspect is re-read here (suspects are rare, so this is
// cheap); one that verifies or no longer exists is dropped.
func (s *Scrubber) Suspects() []string {
	s.mu.Lock()
	ids := make([]string, 0, len(s.suspect))
	for id := range s.suspect {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if bad, _ := s.check(id); bad {
			out = append(out, id)
			continue
		}
		s.mu.Lock()
		delete(s.suspect, id)
		s.mu.Unlock()
	}
	sort.Strings(out)
	return out
}

// check verifies one chunk. A chunk that no longer exists is not bad: it was
// deleted between the inventory and the read.
func (s *Scrubber) check(id string) (bad bool, size int) {
	chunk, err := s.store.Get(id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return false, 0
	case err != nil:
		// Unreadable or unparseable header: the file is damaged.
		s.log.Warn("chunk_scrub_suspect", "chunk_id", id, "reason", "unreadable", "error", err)
		return true, 0
	}
	size = len(chunk.Payload)
	if int64(size) != chunk.Header.PayloadSize {
		s.log.Warn("chunk_scrub_suspect", "chunk_id", id, "reason", "size_mismatch",
			"header_size", chunk.Header.PayloadSize, "payload_size", size)
		return true, size
	}
	if sha256.Sum256(chunk.Payload) != chunk.Header.SHA256 {
		s.log.Warn("chunk_scrub_suspect", "chunk_id", id, "reason", "checksum_mismatch")
		return true, size
	}
	return false, size
}

// ScrubOnce runs one full pass, throttled to BytesPerSec, and replaces the
// suspect set with what it found. It returns ctx.Err() if cancelled, in which
// case the previous suspects are kept.
func (s *Scrubber) ScrubOnce(ctx context.Context) error {
	inv, err := s.store.Inventory()
	if err != nil {
		s.log.Warn("scrub_inventory_failed", "error", err)
		return err
	}
	start := time.Now()
	var bytesRead int64
	current := make(map[string]struct{})
	for _, entry := range inv {
		if err := ctx.Err(); err != nil {
			return err
		}
		bad, n := s.check(entry.ChunkID)
		if bad {
			current[entry.ChunkID] = struct{}{}
		}
		bytesRead += int64(n)
		if err := s.throttle(ctx, start, bytesRead); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.suspect = current
	s.mu.Unlock()
	s.log.Info("scrub_completed", "chunks", len(inv), "suspects", len(current),
		"bytes", bytesRead, "duration", time.Since(start))
	return nil
}

// throttle waits until reading bytesRead since start is within BytesPerSec.
func (s *Scrubber) throttle(ctx context.Context, start time.Time, bytesRead int64) error {
	if s.opts.BytesPerSec <= 0 {
		return nil
	}
	due := start.Add(time.Duration(float64(bytesRead) / float64(s.opts.BytesPerSec) * float64(time.Second)))
	wait := time.Until(due)
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
