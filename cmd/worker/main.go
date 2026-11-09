// Command worker is a storage node: it stores, serves, and deletes chunk files
// on its persistent volume, and registers/heartbeats with the controller. It
// makes no decisions about anything outside its own disk.
//
// Phase 0 skeleton: no functionality yet (implemented in Phase 1).
package main

import (
	"fmt"
	"os"

	"objstore/internal/version"
)

func main() {
	fmt.Fprintln(os.Stderr, version.String("worker")+": not implemented yet (see docs/IMPLEMENTATION_PLAN.md)")
	os.Exit(2)
}
