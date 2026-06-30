package collectors

import (
	"encoding/json"
	"fmt"
	"net/http"

	"gopkg.in/yaml.v3"
)

type MemoryReport struct {
	UsageBytes     uint64         `json:"usage_bytes" yaml:"usage_bytes"`
	UsageSource    string         `json:"usage_source,omitempty" yaml:"usage_source,omitempty"`
	UsageAvailable bool           `json:"usage_available" yaml:"usage_available"`
	Snapshot       MemorySnapshot `json:"snapshot" yaml:"snapshot"`
}

func (m *MemoryMetrics) Report() MemoryReport {
	snap := m.Snapshot()
	usage, source, ok := snap.UsageBytes()
	return MemoryReport{
		UsageBytes:     usage,
		UsageSource:    source,
		UsageAvailable: ok,
		Snapshot:       snap,
	}
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
		data, err := json.MarshalIndent(metrics.Report(), "", "  ")
		return data, "application/json; charset=utf-8", err
	case "", "yaml":
		data, err := yaml.Marshal(metrics.Report())
		return data, "application/yaml; charset=utf-8", err
	default:
		return nil, "", fmt.Errorf("statekit: unsupported memory format %q", format)
	}
}
