// Command objctl is a small client for the object store's REST API, used for
// demos, scripted verification, and benchmarks. It only knows the
// controller's address, never worker addresses.
//
// Phase 0 skeleton: no functionality yet (implemented alongside the client API).
package main

import (
	"fmt"
	"os"

	"objstore/internal/version"
)

func main() {
	fmt.Fprintln(os.Stderr, version.String("objctl")+": not implemented yet (see docs/IMPLEMENTATION_PLAN.md)")
	os.Exit(2)
}
