package statekit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type collectorSnapshot struct {
	Metrics metricList `json:"metrics" yaml:"metrics"`
}

type metricSnapshot struct {
	Name    string                   `json:"name" yaml:"name"`
	Help    string                   `json:"help,omitempty" yaml:"help,omitempty"`
	Type    PrometheusType           `json:"type,omitempty" yaml:"type,omitempty"`
	Labels  []string                 `json:"labels,omitempty" yaml:"labels,omitempty"`
	Samples []metricSample `json:"samples" yaml:"samples"`
}

type metricSample struct {
	Labels map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Value  float64           `json:"value" yaml:"value"`
}

type Format string

const (
	FormatJSON Format = "json"
	FormatYAML Format = "yaml"
)

func PrometheusCollectorJSON(collector PrometheusCollector) ([]byte, error) {
	if collector == nil {
		return nil, fmt.Errorf("statekit: nil prometheus collector")
	}
	return json.MarshalIndent(prometheusCollectorSnapshot(collector), "", "  ")
}

func PrometheusCollectorYAML(collector PrometheusCollector) ([]byte, error) {
	if collector == nil {
		return nil, fmt.Errorf("statekit: nil prometheus collector")
	}
	return yaml.Marshal(prometheusCollectorSnapshot(collector))
}

func PrometheusCollectorYaml(collector PrometheusCollector) ([]byte, error) {
	return PrometheusCollectorYAML(collector)
}

func SnapshotJSON(state State) ([]byte, error) {
	return StateJSON(state)
}

func StateJSON(state State) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("statekit: nil state")
	}
	return json.MarshalIndent(state.Snapshot(), "", "  ")
}

func SnapshotYAML(state State) ([]byte, error) {
	return StateYAML(state)
}

func StateYAML(state State) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("statekit: nil state")
	}
	return yaml.Marshal(state.Snapshot())
}

func SnapshotYaml(state State) ([]byte, error) {
	return SnapshotYAML(state)
}

func StateYaml(state State) ([]byte, error) {
	return StateYAML(state)
}

func StateHandlerFunc(state State, format string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, contentType, err := formatState(state, format)
		writeFormattedValue(w, data, contentType, err)
	}
}

func SnapshotHandlerFunc(state State, format string) http.HandlerFunc {
	return StateHandlerFunc(state, format)
}

func PrometheusCollectorHandlerFunc(collector PrometheusCollector, format string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, contentType, err := formatPrometheusCollector(collector, format)
		writeFormattedValue(w, data, contentType, err)
	}
}

func prometheusCollectorSnapshot(collector PrometheusCollector) collectorSnapshot {
	descs := collector.DescribePrometheus()
	samples := collector.CollectPrometheus()

	metrics := make([]metricSnapshot, 0, len(descs))
	metricIndexes := make(map[string]int, len(descs))
	for _, desc := range descs {
		if _, exists := metricIndexes[desc.Name]; exists {
			continue
		}
		metricIndexes[desc.Name] = len(metrics)
		metrics = append(metrics, metricSnapshot{
			Name:   desc.Name,
			Help:   desc.Help,
			Type:   desc.Type,
			Labels: append([]string(nil), desc.Labels...),
		})
	}

	for _, sample := range samples {
		name := localPrometheusDescName(sample.Name, descs)
		idx, ok := metricIndexes[name]
		if !ok {
			idx = len(metrics)
			metricIndexes[name] = idx
			metrics = append(metrics, metricSnapshot{Name: name})
		}
		metrics[idx].Samples = append(metrics[idx].Samples, metricSample{
			Labels: cloneStringMap(sample.Labels),
			Value:  sample.Value,
		})
	}

	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].Name < metrics[j].Name
	})
	for i := range metrics {
		sort.Slice(metrics[i].Samples, func(a, b int) bool {
			return formatLabels(metrics[i].Samples[a].Labels) < formatLabels(metrics[i].Samples[b].Labels)
		})
	}
	return collectorSnapshot{Metrics: metrics}
}

func formatState(state State, format string) ([]byte, string, error) {
	switch normalizeFormat(format) {
	case FormatJSON:
		data, err := StateJSON(state)
		return data, "application/json; charset=utf-8", err
	case FormatYAML:
		data, err := StateYAML(state)
		return data, "text/yaml; charset=utf-8", err
	default:
		return nil, "", fmt.Errorf("statekit: unsupported format %q", format)
	}
}

func formatPrometheusCollector(collector PrometheusCollector, format string) ([]byte, string, error) {
	switch normalizeFormat(format) {
	case FormatJSON:
		data, err := PrometheusCollectorJSON(collector)
		return data, "application/json; charset=utf-8", err
	case FormatYAML:
		data, err := PrometheusCollectorYAML(collector)
		return data, "text/yaml; charset=utf-8", err
	default:
		return nil, "", fmt.Errorf("statekit: unsupported format %q", format)
	}
}

func normalizeFormat(format string) Format {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json":
		return FormatJSON
	case "yaml", "yml":
		return FormatYAML
	default:
		return Format(format)
	}
}

func writeFormattedValue(w http.ResponseWriter, data []byte, contentType string, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(data)
}

func localPrometheusDescName(sampleName string, descs []PrometheusDesc) string {
	for _, desc := range descs {
		if desc.Type != PrometheusHistogram {
			continue
		}
		if sampleName == desc.Name ||
			sampleName == desc.Name+"_bucket" ||
			sampleName == desc.Name+"_sum" ||
			sampleName == desc.Name+"_count" {
			return desc.Name
		}
	}
	return sampleName
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
