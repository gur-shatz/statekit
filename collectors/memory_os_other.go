//go:build !linux && !darwin

package collectors

func currentRSSBytes() (uint64, bool) {
	return 0, false
}

func peakRSSBytes() (uint64, bool) {
	return 0, false
}
