//go:build linux

package collectors

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

func AvailableMemoryBytes() (uint64, bool) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kib * 1024, true
	}
	return 0, false
}
