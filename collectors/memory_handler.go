package collectors

import (
	"encoding/json"
	"fmt"
	"net/http"

	"gopkg.in/yaml.v3"
)

type MemoryReport struct {
	Usage          string         `json:"usage" yaml:"usage"`
	UsageBytes     uint64         `json:"usage_bytes" yaml:"usage_bytes"`
	UsageSource    string         `json:"usage_source,omitempty" yaml:"usage_source,omitempty"`
	UsageAvailable bool           `json:"usage_available" yaml:"usage_available"`
	Snapshot       MemorySnapshot `json:"snapshot" yaml:"snapshot"`
}

type memoryDisplayReport struct {
	Usage          string                `json:"usage" yaml:"usage"`
	UsageBytes     uint64                `json:"usage_bytes" yaml:"usage_bytes"`
	UsageSource    string                `json:"usage_source,omitempty" yaml:"usage_source,omitempty"`
	UsageAvailable bool                  `json:"usage_available" yaml:"usage_available"`
	Snapshot       memoryDisplaySnapshot `json:"snapshot" yaml:"snapshot"`
}

type memoryDisplaySnapshot struct {
	GoAlloc             string `json:"go_alloc" yaml:"go_alloc"`
	GoAllocBytes        uint64 `json:"go_alloc_bytes" yaml:"go_alloc_bytes"`
	GoTotalAlloc        string `json:"go_total_alloc" yaml:"go_total_alloc"`
	GoTotalAllocBytes   uint64 `json:"go_total_alloc_bytes" yaml:"go_total_alloc_bytes"`
	GoSys               string `json:"go_sys" yaml:"go_sys"`
	GoSysBytes          uint64 `json:"go_sys_bytes" yaml:"go_sys_bytes"`
	GoHeapAlloc         string `json:"go_heap_alloc" yaml:"go_heap_alloc"`
	GoHeapAllocBytes    uint64 `json:"go_heap_alloc_bytes" yaml:"go_heap_alloc_bytes"`
	GoHeapSys           string `json:"go_heap_sys" yaml:"go_heap_sys"`
	GoHeapSysBytes      uint64 `json:"go_heap_sys_bytes" yaml:"go_heap_sys_bytes"`
	GoHeapIdle          string `json:"go_heap_idle" yaml:"go_heap_idle"`
	GoHeapIdleBytes     uint64 `json:"go_heap_idle_bytes" yaml:"go_heap_idle_bytes"`
	GoHeapReleased      string `json:"go_heap_released" yaml:"go_heap_released"`
	GoHeapReleasedBytes uint64 `json:"go_heap_released_bytes" yaml:"go_heap_released_bytes"`
	GoStackInuse        string `json:"go_stack_inuse" yaml:"go_stack_inuse"`
	GoStackInuseBytes   uint64 `json:"go_stack_inuse_bytes" yaml:"go_stack_inuse_bytes"`
	GoObjects           uint64 `json:"go_objects" yaml:"go_objects"`
	OSRSS               string `json:"os_rss,omitempty" yaml:"os_rss,omitempty"`
	OSRSSBytes          uint64 `json:"os_rss_bytes,omitempty" yaml:"os_rss_bytes,omitempty"`
	OSRSSAvailable      bool   `json:"os_rss_available" yaml:"os_rss_available"`
	OSPeakRSS           string `json:"os_peak_rss,omitempty" yaml:"os_peak_rss,omitempty"`
	OSPeakRSSBytes      uint64 `json:"os_peak_rss_bytes,omitempty" yaml:"os_peak_rss_bytes,omitempty"`
	OSPeakRSSAvailable  bool   `json:"os_peak_rss_available" yaml:"os_peak_rss_available"`
}

func (m *MemoryMetrics) Report() MemoryReport {
	snap := m.Snapshot()
	usage, source, ok := snap.UsageBytes()
	return MemoryReport{
		Usage:          FormatBytes(usage),
		UsageBytes:     usage,
		UsageSource:    source,
		UsageAvailable: ok,
		Snapshot:       snap,
	}
}

func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	value := float64(bytes)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.2f EiB", value/unit)
}

func (m *MemoryMetrics) Handler(format string) http.Handler {
	return MemoryHandlerFunc(m, format)
}

func MemoryHandlerFunc(metrics *MemoryMetrics, format string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, contentType, err := formatMemoryReport(metrics, format)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(data)
	}
}

func formatMemoryReport(metrics *MemoryMetrics, format string) ([]byte, string, error) {
	if metrics == nil {
		return nil, "", fmt.Errorf("statekit: nil memory metrics")
	}
	switch format {
	case "json":
		data, err := json.MarshalIndent(newMemoryDisplayReport(metrics.Report()), "", "  ")
		return data, "application/json; charset=utf-8", err
	case "", "yaml":
		data, err := yaml.Marshal(newMemoryDisplayReport(metrics.Report()))
		return data, "application/yaml; charset=utf-8", err
	default:
		return nil, "", fmt.Errorf("statekit: unsupported memory format %q", format)
	}
}

func newMemoryDisplayReport(report MemoryReport) memoryDisplayReport {
	snap := report.Snapshot
	out := memoryDisplayReport{
		Usage:          report.Usage,
		UsageBytes:     report.UsageBytes,
		UsageSource:    report.UsageSource,
		UsageAvailable: report.UsageAvailable,
		Snapshot: memoryDisplaySnapshot{
			GoAlloc:             FormatBytes(snap.GoAllocBytes),
			GoAllocBytes:        snap.GoAllocBytes,
			GoTotalAlloc:        FormatBytes(snap.GoTotalAllocBytes),
			GoTotalAllocBytes:   snap.GoTotalAllocBytes,
			GoSys:               FormatBytes(snap.GoSysBytes),
			GoSysBytes:          snap.GoSysBytes,
			GoHeapAlloc:         FormatBytes(snap.GoHeapAllocBytes),
			GoHeapAllocBytes:    snap.GoHeapAllocBytes,
			GoHeapSys:           FormatBytes(snap.GoHeapSysBytes),
			GoHeapSysBytes:      snap.GoHeapSysBytes,
			GoHeapIdle:          FormatBytes(snap.GoHeapIdleBytes),
			GoHeapIdleBytes:     snap.GoHeapIdleBytes,
			GoHeapReleased:      FormatBytes(snap.GoHeapReleasedBytes),
			GoHeapReleasedBytes: snap.GoHeapReleasedBytes,
			GoStackInuse:        FormatBytes(snap.GoStackInuseBytes),
			GoStackInuseBytes:   snap.GoStackInuseBytes,
			GoObjects:           snap.GoObjects,
			OSRSSAvailable:      snap.OSRSSAvailable,
			OSPeakRSSAvailable:  snap.OSPeakRSSAvailable,
		},
	}
	if snap.OSRSSAvailable {
		out.Snapshot.OSRSS = FormatBytes(snap.OSRSSBytes)
		out.Snapshot.OSRSSBytes = snap.OSRSSBytes
	}
	if snap.OSPeakRSSAvailable {
		out.Snapshot.OSPeakRSS = FormatBytes(snap.OSPeakRSSBytes)
		out.Snapshot.OSPeakRSSBytes = snap.OSPeakRSSBytes
	}
	return out
}
