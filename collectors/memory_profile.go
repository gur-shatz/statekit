package collectors

import (
	"os"
	"path/filepath"
	"runtime/pprof"
)

func writeHeapProfile(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, "heap-*.pprof")
	if err != nil {
		return "", err
	}
	path := f.Name()
	if err := pprof.WriteHeapProfile(f); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path, nil
	}
	return abs, nil
}
