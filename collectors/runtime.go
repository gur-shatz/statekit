package collectors

import (
	"math"
	"runtime/metrics"
	"strings"
	"sync"
	"time"

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

	histMu sync.Mutex
	hist   map[string]*histWindow
}

// histWindow holds two timestamped snapshots of a runtime histogram's bucket
// counts. The older one is used as the baseline for delta-based quantiles; the
// newer one rotates into the older slot once it's at least 1 minute old. This
// gives a sliding window between 1m and 2m wide without a background goroutine.
type histWindow struct {
	olderCounts []uint64
	olderAt     time.Time
	newerCounts []uint64
	newerAt     time.Time
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
		hist: map[string]*histWindow{},
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
		help := runtimeMetricHelp(desc)
		var labels []string
		if desc.Kind == metrics.KindFloat64Histogram {
			typ = statekit.PrometheusSummary
			help = help + " Reported as p50/p90/p95/p99 quantiles over a sliding ~1m window (typical / warning / concerning / worst-case)."
			labels = []string{"quantile", "duration"}
		} else if desc.Cumulative {
			typ = statekit.PrometheusCounter
		}
		out = append(out, statekit.PrometheusDesc{
			Name:   prometheusMetricName(r.prefix, desc.Name),
			Help:   help,
			Type:   typ,
			Labels: labels,
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

	now := time.Now()
	out := make([]statekit.PrometheusSample, 0, len(samples))
	for _, sample := range samples {
		name := prometheusMetricName(r.prefix, sample.Name)
		switch sample.Value.Kind() {
		case metrics.KindUint64:
			out = append(out, statekit.PrometheusSample{Name: name, Value: float64(sample.Value.Uint64())})
		case metrics.KindFloat64:
			out = append(out, statekit.PrometheusSample{Name: name, Value: sample.Value.Float64()})
		case metrics.KindFloat64Histogram:
			delta, duration := r.histogramDelta(sample.Name, sample.Value.Float64Histogram(), now)
			out = append(out, runtimeSummarySamples(name, delta, duration)...)
		}
	}
	return out
}

// histogramDelta returns a windowed delta of h's bucket counts relative to a
// snapshot taken at least 1 minute ago, plus the label that describes the
// window. The first call (and any call before a baseline has aged past 1m)
// returns h itself with duration="lifetime"; thereafter the older snapshot
// rotates as the newer one ages out, keeping the effective window between 1m
// and 2m wide.
func (r *RuntimeMetrics) histogramDelta(name string, h *metrics.Float64Histogram, now time.Time) (*metrics.Float64Histogram, string) {
	if h == nil {
		return nil, "lifetime"
	}
	r.histMu.Lock()
	defer r.histMu.Unlock()

	w := r.hist[name]
	if w == nil {
		w = &histWindow{}
		r.hist[name] = w
	}

	if w.newerCounts == nil {
		w.newerCounts = append([]uint64(nil), h.Counts...)
		w.newerAt = now
		return h, "lifetime"
	}

	if now.Sub(w.newerAt) >= time.Minute {
		w.olderCounts = w.newerCounts
		w.olderAt = w.newerAt
		w.newerCounts = append([]uint64(nil), h.Counts...)
		w.newerAt = now
	}

	if w.olderCounts == nil {
		return h, "lifetime"
	}

	delta := &metrics.Float64Histogram{
		Buckets: h.Buckets,
		Counts:  make([]uint64, len(h.Counts)),
	}
	for i, c := range h.Counts {
		if i < len(w.olderCounts) && c >= w.olderCounts[i] {
			delta.Counts[i] = c - w.olderCounts[i]
		} else {
			delta.Counts[i] = c
		}
	}
	return delta, "1m"
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

// runtimeHistogramQuantiles is the fixed set of quantiles emitted for every
// runtime histogram. A small, bounded set keeps cardinality predictable and
// turns a 100+-bucket distribution into a handful of operationally useful
// numbers.
var runtimeHistogramQuantiles = []float64{0.5, 0.9, 0.95, 0.99}

func runtimeSummarySamples(name string, h *metrics.Float64Histogram, duration string) []statekit.PrometheusSample {
	if h == nil {
		return nil
	}
	out := make([]statekit.PrometheusSample, 0, len(runtimeHistogramQuantiles))
	for _, q := range runtimeHistogramQuantiles {
		out = append(out, statekit.PrometheusSample{
			Name: name,
			Labels: map[string]string{
				"quantile": prometheusFloat(q),
				"duration": duration,
			},
			Value: runtimeHistogramQuantile(h, q),
		})
	}
	return out
}

func runtimeHistogramQuantile(h *metrics.Float64Histogram, q float64) float64 {
	if h == nil || len(h.Counts) == 0 {
		return 0
	}
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var cum uint64
	for i, count := range h.Counts {
		if count == 0 {
			continue
		}
		if float64(cum+count) >= target {
			lo := h.Buckets[i]
			hi := math.Inf(1)
			if i+1 < len(h.Buckets) {
				hi = h.Buckets[i+1]
			}
			if math.IsInf(lo, -1) {
				if math.IsInf(hi, 1) {
					return 0
				}
				return hi
			}
			if math.IsInf(hi, 1) {
				return lo
			}
			frac := (target - float64(cum)) / float64(count)
			return lo + frac*(hi-lo)
		}
		cum += count
	}
	return h.Buckets[len(h.Buckets)-1]
}
