//go:build windows

package store

import (
	"errors"
	"syscall"
)

// syncDir is a no-op on Windows, which cannot open a directory for fsync.
// Workers run in Linux containers; Windows is only used for development and
// tests, where power-loss durability is irrelevant.
func syncDir(string) error { return nil }

const (
	errorHandleDiskFull syscall.Errno = 39  // ERROR_HANDLE_DISK_FULL
	errorDiskFull       syscall.Errno = 112 // ERROR_DISK_FULL
)

func isDiskFull(err error) bool {
	return errors.Is(err, errorDiskFull) || errors.Is(err, errorHandleDiskFull)
}
