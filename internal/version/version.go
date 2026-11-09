// Package version exposes build identification for all binaries.
//
// Version is overridden at link time, for example:
//
//	go build -ldflags "-X objstore/internal/version.Version=v0.1.0" ./cmd/...
package version

// Version is the build version. It is "dev" unless set via -ldflags.
var Version = "dev"

// String returns a human-readable identification for the named binary.
func String(binary string) string {
	return binary + " " + Version
}
