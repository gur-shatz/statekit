package collectors

import (
	"errors"
	"testing"

	"github.com/gur-shatz/statekit"
)

var errMemoryProfileTest = errors.New("profile failed")

func TestMemoryMetricsCollectPrometheus(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{
			GoAllocBytes:        10,
			GoTotalAllocBytes:   20,
			GoSysBytes:          30,
			GoHeapAllocBytes:    40,
			GoHeapSysBytes:      50,
			GoHeapIdleBytes:     60,
			GoHeapReleasedBytes: 70,
			GoStackInuseBytes:   80,
			GoObjects:           90,
			OSRSSBytes:          100,
			OSRSSAvailable:      true,
			OSPeakRSSBytes:      110,
			OSPeakRSSAvailable:  true,
		}
	}))

	samples := metrics.CollectPrometheus()
	got := map[string]float64{}
	for _, sample := range samples {
		got[sample.Name] = sample.Value
	}

	for name, want := range map[string]float64{
		"process_memory_go_alloc_bytes":         10,
		"process_memory_go_total_alloc_bytes":   20,
		"process_memory_go_sys_bytes":           30,
		"process_memory_go_heap_alloc_bytes":    40,
		"process_memory_go_heap_sys_bytes":      50,
		"process_memory_go_heap_idle_bytes":     60,
		"process_memory_go_heap_released_bytes": 70,
		"process_memory_go_stack_inuse_bytes":   80,
		"process_memory_go_objects":             90,
		"process_memory_os_rss_bytes":           100,
		"process_memory_os_peak_rss_bytes":      110,
	} {
		if got[name] != want {
			t.Fatalf("%s = %f, want %f; got %#v", name, got[name], want, got)
		}
	}
}

func TestMemoryMetricsOmitsUnavailableOSPrometheusSamples(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 30}
	}))

	for _, sample := range metrics.CollectPrometheus() {
		if sample.Name == "process_memory_os_rss_bytes" || sample.Name == "process_memory_os_peak_rss_bytes" {
			t.Fatalf("unexpected unavailable OS sample: %#v", sample)
		}
	}
}

func TestMemoryUsageCheckUsesOSRSSWhenAvailable(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{
			GoSysBytes:     50,
			OSRSSBytes:     120,
			OSRSSAvailable: true,
		}
	}))
	check := NewMemoryUsageCheck(metrics, "memory", 100, 200)

	current := check.Current()
	if current.Status != statekit.Warn {
		t.Fatalf("status = %s, want warn", current.Status)
	}
	if current.Data["usage_bytes"] != uint64(120) {
		t.Fatalf("usage_bytes = %#v, want 120", current.Data["usage_bytes"])
	}
	if current.Data["usage_source"] != "os_rss_bytes" {
		t.Fatalf("usage_source = %#v, want os_rss_bytes", current.Data["usage_source"])
	}
}

func TestMemoryUsageCheckFallsBackToGoSys(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 220}
	}))
	check := NewMemoryUsageCheck(metrics, "memory", 100, 200)

	current := check.Current()
	if current.Status != statekit.Fail {
		t.Fatalf("status = %s, want fail", current.Status)
	}
	if current.Data["usage_source"] != "go_sys_bytes" {
		t.Fatalf("usage_source = %#v, want go_sys_bytes", current.Data["usage_source"])
	}
}

func TestNewMemoryTracker(t *testing.T) {
	tracker := NewMemoryTracker("memory", 100, 200, withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoHeapAllocBytes: 10}
	}))
	if tracker.Metrics == nil {
		t.Fatal("tracker metrics is nil")
	}
	if tracker.State == nil {
		t.Fatal("tracker state is nil")
	}
	if tracker.State.Name() != "memory" {
		t.Fatalf("tracker state name = %q, want memory", tracker.State.Name())
	}
}

func TestMemoryUsageCheckWritesHeapProfileOnThresholdCrossing(t *testing.T) {
	values := []uint64{50, 120, 130, 220, 210, 80, 120}
	var profiles []string
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		v := values[0]
		values = values[1:]
		return MemorySnapshot{GoSysBytes: v}
	}))
	check := NewMemoryUsageCheck(
		metrics,
		"memory",
		100,
		200,
		WithMemoryHeapProfileDir(t.TempDir()),
		withMemoryHeapProfileWriter(func(dir string) (string, error) {
			path := dir + "/heap.pprof"
			profiles = append(profiles, path)
			return path, nil
		}),
	)

	for range 7 {
		check.Current()
	}

	if len(profiles) != 3 {
		t.Fatalf("profiles written = %d, want 3: %#v", len(profiles), profiles)
	}
}

func TestMemoryUsageCheckReportsHeapProfileError(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 120}
	}))
	check := NewMemoryUsageCheck(
		metrics,
		"memory",
		100,
		200,
		WithMemoryHeapProfileDir(t.TempDir()),
		withMemoryHeapProfileWriter(func(string) (string, error) {
			return "", errMemoryProfileTest
		}),
	)

	current := check.Current()
	if current.Data["heap_profile_error"] != errMemoryProfileTest.Error() {
		t.Fatalf("heap_profile_error = %#v, want %q", current.Data["heap_profile_error"], errMemoryProfileTest.Error())
	}
}
