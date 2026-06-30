//go:build darwin

package collectors

import (
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var vmStatPageSizePattern = regexp.MustCompile(`page size of ([0-9]+) bytes`)

func AvailableMemoryBytes() (uint64, bool) {
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		return 0, false
	}

	pageSize, pages := uint64(0), uint64(0)
	for _, line := range strings.Split(string(out), "\n") {
		if matches := vmStatPageSizePattern.FindStringSubmatch(line); len(matches) == 2 {
			parsed, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return 0, false
			}
			pageSize = parsed
			continue
		}

		if !strings.HasPrefix(line, "Pages free:") &&
			!strings.HasPrefix(line, "Pages inactive:") &&
			!strings.HasPrefix(line, "Pages speculative:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return 0, false
		}
		value := strings.TrimSuffix(fields[2], ".")
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, false
		}
		pages += parsed
	}
	if pageSize == 0 || pages == 0 {
		return 0, false
	}
	return pageSize * pages, true
}
