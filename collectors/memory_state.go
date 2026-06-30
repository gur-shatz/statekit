package collectors

import (
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/gur-shatz/statekit"
)

type MemoryCheckState struct {
	Status statekit.Status
	Reason string
	Data   map[string]any
}

type MemoryCurrentFunc func() MemoryCheckState

type MemoryCheck struct {
	inner   *statekit.ManualState
	current MemoryCurrentFunc
}

type MemoryTracker struct {
	Metrics *MemoryMetrics
	State   *MemoryCheck
}

func NewMemoryTrackerFromOS(name string, opts ...MemoryOption) *MemoryTracker {
	warnAtBytes, failAtBytes, _, _ := MemoryThresholdsFromOS(0.80, 0.95)
	return NewMemoryTracker(name, warnAtBytes, failAtBytes, opts...)
}

func MemoryThresholdsFromOS(warnRatio, failRatio float64) (warnAtBytes, failAtBytes, availableBytes uint64, ok bool) {
	const fallbackAvailableBytes = uint64(1 << 30)

	if warnRatio <= 0 {
		warnRatio = 0.80
	}
	if failRatio <= 0 {
		failRatio = 0.95
	}
	availableBytes, ok = AvailableMemoryBytes()
	if !ok || availableBytes == 0 {
		availableBytes = fallbackAvailableBytes
	}
	return uint64(float64(availableBytes) * warnRatio), uint64(float64(availableBytes) * failRatio), availableBytes, ok
}

func NewMemoryTracker(name string, warnAtBytes, failAtBytes uint64, opts ...MemoryOption) *MemoryTracker {
	cfg := defaultMemoryOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	metrics := &MemoryMetrics{read: cfg.read}
	return &MemoryTracker{
		Metrics: metrics,
		State:   newMemoryUsageCheck(metrics, name, warnAtBytes, failAtBytes, cfg),
	}
}

func NewMemoryUsageCheck(metrics *MemoryMetrics, name string, warnAtBytes, failAtBytes uint64, opts ...MemoryOption) *MemoryCheck {
	cfg := defaultMemoryOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newMemoryUsageCheck(metrics, name, warnAtBytes, failAtBytes, cfg)
}

func newMemoryUsageCheck(metrics *MemoryMetrics, name string, warnAtBytes, failAtBytes uint64, cfg memoryOptions) *MemoryCheck {
	var mu sync.Mutex
	lastStatus := statekit.Pass

	return newMemoryCheck(name, func() MemoryCheckState {
		if metrics == nil {
			return MemoryCheckState{Status: statekit.Down, Reason: "missing memory metrics", Data: map[string]any{}}
		}
		snap := metrics.Snapshot()
		usage, source, ok := snap.UsageBytes()
		if !ok {
			return MemoryCheckState{Status: statekit.Down, Reason: "memory usage is unavailable", Data: memoryStateData(snap, 0, "")}
		}
		data := memoryStateData(snap, usage, source)
		data["warn_at_bytes"] = warnAtBytes
		data["fail_at_bytes"] = failAtBytes
		status, threshold := uintThresholdStatus(usage, warnAtBytes, failAtBytes, 0)
		maybeWriteHeapProfile(data, status, &lastStatus, &mu, cfg)
		if status == statekit.Pass {
			return MemoryCheckState{Status: statekit.Pass, Data: data}
		}
		return MemoryCheckState{
			Status: status,
			Reason: fmt.Sprintf("memory usage %d bytes above %s threshold %d bytes", usage, status, threshold),
			Data:   data,
		}
	})
}

func newMemoryCheck(name string, current MemoryCurrentFunc) *MemoryCheck {
	if name == "" {
		name = "memory usage"
	}
	if current == nil {
		current = func() MemoryCheckState {
			return MemoryCheckState{Status: statekit.Pass, Data: map[string]any{}}
		}
	}
	return &MemoryCheck{
		inner:   statekit.NewManualState(name),
		current: current,
	}
}

func (c *MemoryCheck) Name() string {
	return c.inner.Name()
}

func (c *MemoryCheck) Current() MemoryCheckState {
	current := c.current()
	return MemoryCheckState{
		Status: current.Status,
		Reason: current.Reason,
		Data:   maps.Clone(current.Data),
	}
}

func (c *MemoryCheck) Snapshot() statekit.Snapshot {
	current := c.Current()
	c.inner.Set(current.Status, current.Reason, current.Data)
	return c.inner.Snapshot()
}

func memoryStateData(snap MemorySnapshot, usage uint64, source string) map[string]any {
	data := map[string]any{
		"usage":                  FormatBytes(usage),
		"usage_bytes":            usage,
		"usage_source":           source,
		"go_alloc":               FormatBytes(snap.GoAllocBytes),
		"go_alloc_bytes":         snap.GoAllocBytes,
		"go_total_alloc":         FormatBytes(snap.GoTotalAllocBytes),
		"go_total_alloc_bytes":   snap.GoTotalAllocBytes,
		"go_sys":                 FormatBytes(snap.GoSysBytes),
		"go_sys_bytes":           snap.GoSysBytes,
		"go_heap_alloc":          FormatBytes(snap.GoHeapAllocBytes),
		"go_heap_alloc_bytes":    snap.GoHeapAllocBytes,
		"go_heap_sys":            FormatBytes(snap.GoHeapSysBytes),
		"go_heap_sys_bytes":      snap.GoHeapSysBytes,
		"go_heap_idle":           FormatBytes(snap.GoHeapIdleBytes),
		"go_heap_idle_bytes":     snap.GoHeapIdleBytes,
		"go_heap_released":       FormatBytes(snap.GoHeapReleasedBytes),
		"go_heap_released_bytes": snap.GoHeapReleasedBytes,
		"go_stack_inuse":         FormatBytes(snap.GoStackInuseBytes),
		"go_stack_inuse_bytes":   snap.GoStackInuseBytes,
		"go_objects":             snap.GoObjects,
		"os_rss_available":       snap.OSRSSAvailable,
		"os_peak_rss_available":  snap.OSPeakRSSAvailable,
	}
	if snap.OSRSSAvailable {
		data["os_rss"] = FormatBytes(snap.OSRSSBytes)
		data["os_rss_bytes"] = snap.OSRSSBytes
	}
	if snap.OSPeakRSSAvailable {
		data["os_peak_rss"] = FormatBytes(snap.OSPeakRSSBytes)
		data["os_peak_rss_bytes"] = snap.OSPeakRSSBytes
	}
	return data
}

func maybeWriteHeapProfile(data map[string]any, status statekit.Status, lastStatus *statekit.Status, mu *sync.Mutex, cfg memoryOptions) {
	if strings.TrimSpace(cfg.heapProfileDir) == "" || cfg.writeHeap == nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if status <= statekit.Pass || status <= *lastStatus {
		*lastStatus = status
		return
	}
	*lastStatus = status

	path, err := cfg.writeHeap(cfg.heapProfileDir)
	if err != nil {
		data["heap_profile_error"] = err.Error()
		return
	}
	data["heap_profile_path"] = path
}
