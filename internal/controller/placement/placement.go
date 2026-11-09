// Package placement chooses which workers receive a chunk (architecture
// §5.3, D17). Policies are pluggable; Phase 3 ships random-distinct.
//
// Hard rules, applied by Eligible before any policy runs:
//  1. only ALIVE workers (SYNCING and DEAD are never candidates, DEC-006);
//  2. distinct workers per chunk (policies return distinct IDs; the database
//     enforces it again with UNIQUE(chunk_id, worker_id));
//  3. skip workers that reported less free space than MinFreeBytes
//     (workers reporting -1, "unknown", are not skipped).
package placement

import (
	"math/rand/v2"
	"sort"

	"objstore/internal/controller/registry"
)

// Policy picks n distinct workers for a chunk from candidates (already
// filtered by Eligible). It returns fewer than n only if there are fewer
// candidates.
type Policy interface {
	Choose(chunkID string, n int, candidates []string) []string
}

// Random chooses uniformly at random among the candidates (Phase 3 policy).
type Random struct{}

// Choose implements Policy.
func (Random) Choose(_ string, n int, candidates []string) []string {
	c := append([]string(nil), candidates...)
	rand.Shuffle(len(c), func(i, j int) { c[i], c[j] = c[j], c[i] })
	if n > len(c) {
		n = len(c)
	}
	return c[:n]
}

// Eligible returns the IDs of ALIVE workers that are not in exclude and have
// enough free space, sorted by ID (so policies see a deterministic order).
func Eligible(alive []registry.Worker, exclude map[string]bool, minFreeBytes int64) []string {
	out := make([]string, 0, len(alive))
	for _, w := range alive {
		if w.State != registry.Alive || exclude[w.ID] {
			continue
		}
		if fb := w.Capacity.FreeBytes; fb >= 0 && fb < minFreeBytes {
			continue
		}
		out = append(out, w.ID)
	}
	sort.Strings(out)
	return out
}
