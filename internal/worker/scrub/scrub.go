// Package scrub periodically verifies committed chunk files without changing
// them. Suspects are reported by the membership agent on its next heartbeat;
// the controller remains the authority for replica state.
package scrub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sort"
	"sync"
	"time"

	"objstore/internal/worker/store"
)

type Store interface {
	Inventory() ([]store.Entry, error)
	Get(string) (store.Chunk, error)
}

type Scrubber struct {
	store    Store
	interval time.Duration
	log      *slog.Logger
	mu       sync.Mutex
	suspect  map[string]struct{}
}

func New(st Store, interval time.Duration, logger *slog.Logger) *Scrubber {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Scrubber{store: st, interval: interval, log: logger, suspect: map[string]struct{}{}}
}

func (s *Scrubber) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	s.scrubOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.scrubOnce()
		}
	}
}

func (s *Scrubber) Suspects() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.suspect))
	for id := range s.suspect {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (s *Scrubber) ScrubOnce() error { return s.scrubOnce() }

func (s *Scrubber) scrubOnce() error {
	inv, err := s.store.Inventory()
	if err != nil {
		s.log.Warn("scrub_inventory_failed", "error", err)
		return err
	}
	current := make(map[string]struct{})
	for _, entry := range inv {
		chunk, getErr := s.store.Get(entry.ChunkID)
		bad := getErr != nil
		if getErr == nil {
			sum := sha256.Sum256(chunk.Payload)
			bad = int64(len(chunk.Payload)) != chunk.Header.PayloadSize ||
				hex.EncodeToString(sum[:]) != hex.EncodeToString(chunk.Header.SHA256[:])
		}
		if bad {
			current[entry.ChunkID] = struct{}{}
			s.log.Warn("chunk_scrub_suspect", "chunk_id", entry.ChunkID, "error", getErr)
		}
	}
	s.mu.Lock()
	s.suspect = current
	s.mu.Unlock()
	s.log.Info("scrub_completed", "chunks", len(inv), "suspects", len(current))
	return nil
}
