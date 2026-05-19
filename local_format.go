package statekit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"
)

type PrometheusCollectorSnapshot struct {
	Descriptions []PrometheusDesc   `json:"descriptions" yaml:"descriptions"`
	Samples      []PrometheusSample `json:"samples" yaml:"samples"`
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

func prometheusCollectorSnapshot(collector PrometheusCollector) PrometheusCollectorSnapshot {
	return PrometheusCollectorSnapshot{
		Descriptions: collector.DescribePrometheus(),
		Samples:      collector.CollectPrometheus(),
	}
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
