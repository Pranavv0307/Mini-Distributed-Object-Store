package registry

import (
	"context"
	"time"
)

// RunFailureDetector calls r.CheckLiveness every interval until ctx is done.
// A worker is therefore declared DEAD between DeadAfter and
// DeadAfter+interval after its last heartbeat.
func RunFailureDetector(ctx context.Context, r *Registry, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.CheckLiveness()
		}
	}
}
