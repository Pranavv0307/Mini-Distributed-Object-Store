package placement

import (
	"testing"

	"objstore/internal/controller/registry"
)

func workers(states map[string]registry.State, free map[string]int64) []registry.Worker {
	var out []registry.Worker
	for id, st := range states {
		out = append(out, registry.Worker{ID: id, State: st, Capacity: registry.Capacity{FreeBytes: free[id]}})
	}
	return out
}

func TestEligible(t *testing.T) {
	ws := workers(
		map[string]registry.State{"w1": registry.Alive, "w2": registry.Alive, "w3": registry.Syncing, "w4": registry.Dead, "w5": registry.Alive, "w6": registry.Alive},
		map[string]int64{"w1": 1000, "w2": -1, "w5": 10, "w6": 1000},
	)
	got := Eligible(ws, map[string]bool{"w6": true}, 100)
	want := []string{"w1", "w2"} // w3 SYNCING, w4 DEAD, w5 too full, w6 excluded; w2 unknown space is fine
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Eligible = %v, want %v", got, want)
	}
}

func TestRandomChoosesDistinctAndSpreads(t *testing.T) {
	cands := []string{"w1", "w2", "w3", "w4"}
	counts := map[string]int{}
	for range 4000 {
		got := Random{}.Choose("c", 2, cands)
		if len(got) != 2 || got[0] == got[1] {
			t.Fatalf("Choose = %v", got)
		}
		for _, w := range got {
			counts[w]++
		}
	}
	for _, w := range cands {
		// Expected 2000 each; allow wide slack so the test is not flaky.
		if counts[w] < 1700 || counts[w] > 2300 {
			t.Fatalf("uneven spread: %v", counts)
		}
	}
	if got := (Random{}).Choose("c", 3, []string{"w1"}); len(got) != 1 {
		t.Fatalf("Choose with too few candidates = %v", got)
	}
	// Choose must not reorder the caller's slice.
	if cands[0] != "w1" || cands[3] != "w4" {
		t.Fatalf("candidates mutated: %v", cands)
	}
}
