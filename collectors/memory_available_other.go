//go:build !linux && !darwin

package collectors

func AvailableMemoryBytes() (uint64, bool) {
	return 0, false
}
