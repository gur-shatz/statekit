//go:build linux

package collectors

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func currentRSSBytes() (uint64, bool) {
	text, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(text))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	pageSize := uint64(os.Getpagesize())
	return pages * pageSize, true
}

func peakRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	if usage.Maxrss <= 0 {
		return 0, false
	}
	return uint64(usage.Maxrss) * 1024, true
}
