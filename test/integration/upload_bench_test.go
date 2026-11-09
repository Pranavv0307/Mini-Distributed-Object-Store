package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// BenchmarkUploadWindow compares sequential (window 1, M3a) with windowed
// (M3b) uploads of an 8 MB object (8 chunks) through the real controller and
// four in-process workers, with 5 ms of simulated latency per chunk PUT.
//
//	go test -run '^$' -bench UploadWindow -benchtime 10x ./test/integration/
func BenchmarkUploadWindow(b *testing.B) {
	for _, window := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("window=%d", window), func(b *testing.B) {
			c := newUploadCluster(b, clusterOpts{workers: 4, window: window})
			for _, w := range c.workers {
				w.faults.putLatency.Store(int64(5 * time.Millisecond))
			}
			data := randomBytes(b, 8_000_000)
			ct, body := multipartBody(b, filePart("bench.bin", data))
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				r := c.postRaw(ct, bytes.NewReader(body))
				if r.status != http.StatusCreated {
					b.Fatalf("status %d: %+v", r.status, r.err)
				}
			}
		})
	}
}
