//go:build darwin && !cgo

package collectors

import "syscall"

func currentRSSBytes() (uint64, bool) {
	return 0, false
}

func peakRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	if usage.Maxrss <= 0 {
		return 0, false
	}
	return uint64(usage.Maxrss), true
}
