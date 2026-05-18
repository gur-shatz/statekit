package collectors

import (
	"math"
	"runtime/metrics"
	"strings"

	"github.com/gur-shatz/statekit"
)

type RuntimeMetricFilter func(metrics.Description) bool

type RuntimeOption func(*RuntimeMetrics)

type RuntimeMetrics struct {
	descs     []metrics.Description
	names     []string
	prefix    string
	filter    RuntimeMetricFilter
	whitelist map[string]struct{}
}

// RecommendedRuntimeMetrics is a small Prometheus-name whitelist for the runtime
// signals that tend to be useful in application health decisions.
var RecommendedRuntimeMetrics = []string{
	"go_runtime_sched_goroutines_goroutines",
	"go_runtime_gc_pauses_seconds",
	"go_runtime_cpu_classes_gc_pause_cpu_seconds",
	"go_runtime_memory_classes_total_bytes",
	"go_runtime_memory_classes_heap_released_bytes",
	"go_runtime_sched_latencies_seconds",
}

func NewRuntimeMetrics(opts ...RuntimeOption) *RuntimeMetrics {
	r := &RuntimeMetrics{
		prefix: "go_runtime_",
		filter: func(metrics.Description) bool {
			return true
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	all := metrics.All()
	seen := map[string]struct{}{}
	for _, desc := range all {
		if desc.Kind == metrics.KindBad || !r.runtimeMetricAllowed(desc.Name) || !r.filter(desc) {
			continue
		}
		promName := prometheusMetricName(r.prefix, desc.Name)
		if _, ok := seen[promName]; ok {
			continue
		}
		seen[promName] = struct{}{}
		r.descs = append(r.descs, desc)
		r.names = append(r.names, desc.Name)
	}
	return r
}

func WithRuntimeMetricsPrefix(prefix string) RuntimeOption {
	return func(r *RuntimeMetrics) {
		r.prefix = prefix
	}
}

func WithRuntimeMetricsFilter(filter RuntimeMetricFilter) RuntimeOption {
	return func(r *RuntimeMetrics) {
		if filter != nil {
			r.filter = filter
		}
	}
}

func WithRuntimeMetricsWhitelist(names ...string) RuntimeOption {
	return func(r *RuntimeMetrics) {
		r.whitelist = runtimeMetricNameSet(names)
	}
}

func WithRecommendedRuntimeMetrics() RuntimeOption {
	return WithRuntimeMetricsWhitelist(RecommendedRuntimeMetrics...)
}

func (r *RuntimeMetrics) Value(name string) (float64, bool) {
	runtimeName, ok := r.runtimeMetricName(name)
	if !ok {
		return 0, false
	}
	samples := []metrics.Sample{{Name: runtimeName}}
	metrics.Read(samples)
	switch samples[0].Value.Kind() {
	case metrics.KindUint64:
		return float64(samples[0].Value.Uint64()), true
	case metrics.KindFloat64:
		return samples[0].Value.Float64(), true
	default:
		return 0, false
	}
}

func (r *RuntimeMetrics) runtimeMetricAllowed(name string) bool {
	if len(r.whitelist) == 0 {
		return true
	}
	if _, ok := r.whitelist[name]; ok {
		return true
	}
	_, ok := r.whitelist[prometheusMetricName(r.prefix, name)]
	return ok
}

func (r *RuntimeMetrics) runtimeMetricName(name string) (string, bool) {
	for _, desc := range metrics.All() {
		runtimeName := desc.Name
		if name == runtimeName || name == prometheusMetricName(r.prefix, runtimeName) {
			return runtimeName, true
		}
	}
	return "", false
}

func (r *RuntimeMetrics) DescribePrometheus() []statekit.PrometheusDesc {
	out := make([]statekit.PrometheusDesc, 0, len(r.descs))
	for _, desc := range r.descs {
		typ := statekit.PrometheusGauge
		if desc.Kind == metrics.KindFloat64Histogram {
			typ = statekit.PrometheusHistogram
		} else if desc.Cumulative {
			typ = statekit.PrometheusCounter
		}
		out = append(out, statekit.PrometheusDesc{
			Name: prometheusMetricName(r.prefix, desc.Name),
			Help: runtimeMetricHelp(desc),
			Type: typ,
		})
	}
	return out
}

func (r *RuntimeMetrics) CollectPrometheus() []statekit.PrometheusSample {
	samples := make([]metrics.Sample, len(r.names))
	for i, name := range r.names {
		samples[i].Name = name
	}
	metrics.Read(samples)

	out := make([]statekit.PrometheusSample, 0, len(samples))
	for _, sample := range samples {
		name := prometheusMetricName(r.prefix, sample.Name)
		switch sample.Value.Kind() {
		case metrics.KindUint64:
			out = append(out, statekit.PrometheusSample{Name: name, Value: float64(sample.Value.Uint64())})
		case metrics.KindFloat64:
			out = append(out, statekit.PrometheusSample{Name: name, Value: sample.Value.Float64()})
		case metrics.KindFloat64Histogram:
			out = append(out, runtimeHistogramSamples(name, sample.Value.Float64Histogram())...)
		}
	}
	return out
}

func runtimeMetricNameSet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func runtimeMetricHelp(desc metrics.Description) string {
	help := strings.TrimSpace(desc.Description)
	if help == "" {
		help = desc.Name
	}
	return help
}

func runtimeHistogramSamples(name string, h *metrics.Float64Histogram) []statekit.PrometheusSample {
	if h == nil {
		return nil
	}
	total := uint64(0)
	out := make([]statekit.PrometheusSample, 0, len(h.Counts)+1)
	for i, count := range h.Counts {
		total += count
		upper := math.Inf(1)
		if i+1 < len(h.Buckets) {
			upper = h.Buckets[i+1]
		}
		out = append(out, statekit.PrometheusSample{
			Name:   name + "_bucket",
			Labels: map[string]string{"le": prometheusFloat(upper)},
			Value:  float64(total),
		})
	}
	out = append(out, statekit.PrometheusSample{
		Name:  name + "_count",
		Value: float64(total),
	})
	return out
}
