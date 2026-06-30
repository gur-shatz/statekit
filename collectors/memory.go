package collectors

import (
	"runtime"

	"github.com/gur-shatz/statekit"
)

type MemoryOption func(*memoryOptions)

type MemoryMetrics struct {
	read memoryReadFunc
}

type memoryReadFunc func() MemorySnapshot
type memoryHeapProfileFunc func(string) (string, error)

type memoryOptions struct {
	read           memoryReadFunc
	heapProfileDir string
	writeHeap      memoryHeapProfileFunc
}

type MemorySnapshot struct {
	GoAllocBytes        uint64 `json:"go_alloc_bytes" yaml:"go_alloc_bytes"`
	GoTotalAllocBytes   uint64 `json:"go_total_alloc_bytes" yaml:"go_total_alloc_bytes"`
	GoSysBytes          uint64 `json:"go_sys_bytes" yaml:"go_sys_bytes"`
	GoHeapAllocBytes    uint64 `json:"go_heap_alloc_bytes" yaml:"go_heap_alloc_bytes"`
	GoHeapSysBytes      uint64 `json:"go_heap_sys_bytes" yaml:"go_heap_sys_bytes"`
	GoHeapIdleBytes     uint64 `json:"go_heap_idle_bytes" yaml:"go_heap_idle_bytes"`
	GoHeapReleasedBytes uint64 `json:"go_heap_released_bytes" yaml:"go_heap_released_bytes"`
	GoStackInuseBytes   uint64 `json:"go_stack_inuse_bytes" yaml:"go_stack_inuse_bytes"`
	GoObjects           uint64 `json:"go_objects" yaml:"go_objects"`
	OSRSSBytes          uint64 `json:"os_rss_bytes" yaml:"os_rss_bytes"`
	OSRSSAvailable      bool   `json:"os_rss_available" yaml:"os_rss_available"`
	OSPeakRSSBytes      uint64 `json:"os_peak_rss_bytes" yaml:"os_peak_rss_bytes"`
	OSPeakRSSAvailable  bool   `json:"os_peak_rss_available" yaml:"os_peak_rss_available"`
}

func NewMemoryMetrics(opts ...MemoryOption) *MemoryMetrics {
	cfg := defaultMemoryOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &MemoryMetrics{read: cfg.read}
}

func withMemoryReader(read memoryReadFunc) MemoryOption {
	return func(cfg *memoryOptions) {
		if read != nil {
			cfg.read = read
		}
	}
}

func WithMemoryHeapProfileDir(dir string) MemoryOption {
	return func(cfg *memoryOptions) {
		cfg.heapProfileDir = dir
	}
}

func withMemoryHeapProfileWriter(write memoryHeapProfileFunc) MemoryOption {
	return func(cfg *memoryOptions) {
		if write != nil {
			cfg.writeHeap = write
		}
	}
}

func defaultMemoryOptions() memoryOptions {
	return memoryOptions{
		read:      readMemorySnapshot,
		writeHeap: writeHeapProfile,
	}
}

func (m *MemoryMetrics) Snapshot() MemorySnapshot {
	if m == nil || m.read == nil {
		return MemorySnapshot{}
	}
	return m.read()
}

func (m *MemoryMetrics) UsageBytes() (uint64, string, bool) {
	snap := m.Snapshot()
	return snap.UsageBytes()
}

func (s MemorySnapshot) UsageBytes() (uint64, string, bool) {
	if s.OSRSSAvailable {
		return s.OSRSSBytes, "os_rss_bytes", true
	}
	if s.GoSysBytes > 0 {
		return s.GoSysBytes, "go_sys_bytes", true
	}
	if s.GoHeapAllocBytes > 0 {
		return s.GoHeapAllocBytes, "go_heap_alloc_bytes", true
	}
	return 0, "", false
}

func (m *MemoryMetrics) DescribePrometheus() []statekit.PrometheusDesc {
	return []statekit.PrometheusDesc{
		{Name: "process_memory_os_rss_bytes", Help: "Current process resident set size reported by the OS, when available.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_os_peak_rss_bytes", Help: "Peak process resident set size reported by the OS, when available.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_alloc_bytes", Help: "Bytes of allocated heap objects reported by Go runtime MemStats Alloc.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_total_alloc_bytes", Help: "Cumulative bytes allocated for heap objects reported by Go runtime MemStats TotalAlloc.", Type: statekit.PrometheusCounter},
		{Name: "process_memory_go_sys_bytes", Help: "Total bytes of memory obtained from the OS by the Go runtime.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_heap_alloc_bytes", Help: "Bytes of allocated heap objects reported by Go runtime MemStats HeapAlloc.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_heap_sys_bytes", Help: "Bytes of heap memory obtained from the OS by the Go runtime.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_heap_idle_bytes", Help: "Bytes in idle unused spans reported by Go runtime MemStats HeapIdle.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_heap_released_bytes", Help: "Bytes of physical memory returned to the OS by the Go runtime.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_stack_inuse_bytes", Help: "Bytes in stack spans reported by Go runtime MemStats StackInuse.", Type: statekit.PrometheusGauge},
		{Name: "process_memory_go_objects", Help: "Number of allocated heap objects reported by Go runtime MemStats HeapObjects.", Type: statekit.PrometheusGauge},
	}
}

func (m *MemoryMetrics) CollectPrometheus() []statekit.PrometheusSample {
	snap := m.Snapshot()
	out := []statekit.PrometheusSample{
		{Name: "process_memory_go_alloc_bytes", Value: float64(snap.GoAllocBytes)},
		{Name: "process_memory_go_total_alloc_bytes", Value: float64(snap.GoTotalAllocBytes)},
		{Name: "process_memory_go_sys_bytes", Value: float64(snap.GoSysBytes)},
		{Name: "process_memory_go_heap_alloc_bytes", Value: float64(snap.GoHeapAllocBytes)},
		{Name: "process_memory_go_heap_sys_bytes", Value: float64(snap.GoHeapSysBytes)},
		{Name: "process_memory_go_heap_idle_bytes", Value: float64(snap.GoHeapIdleBytes)},
		{Name: "process_memory_go_heap_released_bytes", Value: float64(snap.GoHeapReleasedBytes)},
		{Name: "process_memory_go_stack_inuse_bytes", Value: float64(snap.GoStackInuseBytes)},
		{Name: "process_memory_go_objects", Value: float64(snap.GoObjects)},
	}
	if snap.OSRSSAvailable {
		out = append(out, statekit.PrometheusSample{Name: "process_memory_os_rss_bytes", Value: float64(snap.OSRSSBytes)})
	}
	if snap.OSPeakRSSAvailable {
		out = append(out, statekit.PrometheusSample{Name: "process_memory_os_peak_rss_bytes", Value: float64(snap.OSPeakRSSBytes)})
	}
	return out
}

func readMemorySnapshot() MemorySnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss, rssOK := currentRSSBytes()
	peak, peakOK := peakRSSBytes()
	return MemorySnapshot{
		GoAllocBytes:        stats.Alloc,
		GoTotalAllocBytes:   stats.TotalAlloc,
		GoSysBytes:          stats.Sys,
		GoHeapAllocBytes:    stats.HeapAlloc,
		GoHeapSysBytes:      stats.HeapSys,
		GoHeapIdleBytes:     stats.HeapIdle,
		GoHeapReleasedBytes: stats.HeapReleased,
		GoStackInuseBytes:   stats.StackInuse,
		GoObjects:           stats.HeapObjects,
		OSRSSBytes:          rss,
		OSRSSAvailable:      rssOK,
		OSPeakRSSBytes:      peak,
		OSPeakRSSAvailable:  peakOK,
	}
}
