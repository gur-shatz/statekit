package statekit

type PrometheusType string

const (
	PrometheusGauge     PrometheusType = "gauge"
	PrometheusCounter   PrometheusType = "counter"
	PrometheusHistogram PrometheusType = "histogram"
)

type PrometheusDesc struct {
	Name   string
	Help   string
	Type   PrometheusType
	Labels []string
}

type PrometheusSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// PrometheusCollector is an optional interface for objects that expose factual
// values in addition to their state. State gauges are emitted automatically for
// all registered states.
type PrometheusCollector interface {
	DescribePrometheus() []PrometheusDesc
	CollectPrometheus() []PrometheusSample
}
