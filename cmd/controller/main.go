// Command controller is the single coordinator of the object store: it serves
// the client REST API, owns the metadata database, tracks worker liveness, and
// runs the background repair and cleanup loops.
//
// Phase 0 skeleton: no functionality yet (implemented from Phase 2 onward).
package main

import (
	"fmt"
	"os"

	"objstore/internal/version"
)

func main() {
	fmt.Fprintln(os.Stderr, version.String("controller")+": not implemented yet (see docs/IMPLEMENTATION_PLAN.md)")
	os.Exit(2)
}
