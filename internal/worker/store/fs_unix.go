//go:build !windows

package store

import (
	"errors"
	"os"
	"syscall"
)

// syncDir fsyncs a directory so that a rename or unlink inside it is durable
// across power loss, not just process crashes.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func isDiskFull(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}
