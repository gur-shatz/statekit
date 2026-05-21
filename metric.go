package statekit

type PrometheusType string

const (
	PrometheusGauge     PrometheusType = "gauge"
	PrometheusCounter   PrometheusType = "counter"
	PrometheusHistogram PrometheusType = "histogram"
	PrometheusSummary   PrometheusType = "summary"
)

type PrometheusDesc struct {
	Name   string         `json:"name" yaml:"name"`
	Help   string         `json:"help,omitempty" yaml:"help,omitempty"`
	Type   PrometheusType `json:"type" yaml:"type"`
	Labels []string       `json:"labels,omitempty" yaml:"labels,omitempty"`
}

type PrometheusSample struct {
	Name   string            `json:"name" yaml:"name"`
	Labels map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Value  float64           `json:"value" yaml:"value"`
}

// PrometheusCollector is an optional interface for objects that expose factual
// values in addition to their state. State gauges are emitted automatically for
// all registered states.
type PrometheusCollector interface {
	DescribePrometheus() []PrometheusDesc
	CollectPrometheus() []PrometheusSample
}
