package collectors

import (
	"bytes"
	"reflect"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

func TestRuntimeMetricsExportsRuntimeMetrics(t *testing.T) {
	reg := statekit.NewRegistry()
	runtimeMetrics := NewRuntimeMetrics(WithRuntimeMetricsFilter(func(desc metrics.Description) bool {
		return desc.Name == "/sched/goroutines:goroutines"
	}))
	if err := reg.RegisterCollectors(runtimeMetrics); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# HELP go_runtime_sched_goroutines_goroutines",
		"# TYPE go_runtime_sched_goroutines_goroutines gauge",
		"go_runtime_sched_goroutines_goroutines ",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("runtime prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestRuntimeMetricsRegistersAllDefaultDescriptors(t *testing.T) {
	reg := statekit.NewRegistry()
	if err := reg.RegisterCollectors(NewRuntimeMetrics()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeMetricsWhitelistAcceptsRuntimeName(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRuntimeMetricsWhitelist("/sched/goroutines:goroutines"))
	descs := runtimeMetrics.DescribePrometheus()
	if len(descs) != 1 {
		t.Fatalf("descs len = %d, want 1", len(descs))
	}
	if descs[0].Name != "go_runtime_sched_goroutines_goroutines" {
		t.Fatalf("desc name = %q, want goroutines metric", descs[0].Name)
	}
}

func TestRuntimeMetricsWhitelistAcceptsPrometheusName(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRuntimeMetricsWhitelist("go_runtime_sched_goroutines_goroutines"))
	descs := runtimeMetrics.DescribePrometheus()
	if len(descs) != 1 {
		t.Fatalf("descs len = %d, want 1", len(descs))
	}
	if descs[0].Name != "go_runtime_sched_goroutines_goroutines" {
		t.Fatalf("desc name = %q, want goroutines metric", descs[0].Name)
	}
}

func TestRuntimeMetricsValue(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRuntimeMetricsWhitelist("go_runtime_sched_goroutines_goroutines"))
	value, ok := runtimeMetrics.Value("go_runtime_sched_goroutines_goroutines")
	if !ok {
		t.Fatal("goroutines metric value not found")
	}
	if value <= 0 {
		t.Fatalf("goroutines metric value = %f, want positive", value)
	}
}

func TestRuntimeMetricsWhitelistBoundsCollectedSamples(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRecommendedRuntimeMetrics())
	samples := runtimeMetrics.CollectPrometheus()
	bound := len(RecommendedRuntimeMetrics) * len(runtimeHistogramQuantiles)
	if len(samples) > bound {
		t.Fatalf("len(samples) = %d, want <= %d (whitelist size * quantile count); samples=%#v",
			len(samples), bound, sampleNames(samples))
	}
}

func TestRuntimeMetricsHistogramEmitsFourQuantiles(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRuntimeMetricsWhitelist("go_runtime_gc_pauses_seconds"))
	descs := runtimeMetrics.DescribePrometheus()
	if len(descs) != 1 || descs[0].Type != statekit.PrometheusSummary {
		t.Fatalf("desc = %#v, want one summary descriptor", descs)
	}
	wantLabels := []string{"quantile", "duration"}
	if !reflect.DeepEqual(descs[0].Labels, wantLabels) {
		t.Fatalf("desc labels = %v, want %v", descs[0].Labels, wantLabels)
	}

	samples := runtimeMetrics.CollectPrometheus()
	if len(samples) != len(runtimeHistogramQuantiles) {
		t.Fatalf("len(samples) = %d, want %d (one per quantile); samples=%#v",
			len(samples), len(runtimeHistogramQuantiles), sampleNames(samples))
	}
	got := map[string]struct{}{}
	for _, s := range samples {
		if s.Name != "go_runtime_gc_pauses_seconds" {
			t.Fatalf("sample name = %q, want %q", s.Name, "go_runtime_gc_pauses_seconds")
		}
		if s.Labels["duration"] != "lifetime" {
			t.Fatalf("first-collect duration = %q, want lifetime", s.Labels["duration"])
		}
		got[s.Labels["quantile"]] = struct{}{}
	}
	for _, want := range []string{"0.5", "0.9", "0.95", "0.99"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("missing quantile=%q label; got=%v", want, got)
		}
	}
}

func TestRuntimeMetricsHistogramDeltaWindowing(t *testing.T) {
	r := NewRuntimeMetrics(WithRuntimeMetricsWhitelist("go_runtime_gc_pauses_seconds"))

	mkHist := func(counts ...uint64) *metrics.Float64Histogram {
		buckets := make([]float64, len(counts)+1)
		for i := range buckets {
			buckets[i] = float64(i)
		}
		return &metrics.Float64Histogram{Buckets: buckets, Counts: counts}
	}

	t0 := time.Unix(1_700_000_000, 0)
	name := "/sched/latencies:seconds"

	h, dur := r.histogramDelta(name, mkHist(10, 20, 30), t0)
	if dur != "lifetime" {
		t.Fatalf("first call duration = %q, want lifetime", dur)
	}
	if h.Counts[0] != 10 || h.Counts[1] != 20 || h.Counts[2] != 30 {
		t.Fatalf("first call returned wrong histogram: %v", h.Counts)
	}

	h, dur = r.histogramDelta(name, mkHist(15, 30, 45), t0.Add(30*time.Second))
	if dur != "lifetime" {
		t.Fatalf("30s call duration = %q, want lifetime (still warming up)", dur)
	}

	h, dur = r.histogramDelta(name, mkHist(20, 40, 60), t0.Add(70*time.Second))
	if dur != "1m" {
		t.Fatalf("70s call duration = %q, want 1m", dur)
	}
	if !reflect.DeepEqual(h.Counts, []uint64{10, 20, 30}) {
		t.Fatalf("delta counts at 70s = %v, want [10 20 30] (current - baseline at t=0)", h.Counts)
	}

	h, dur = r.histogramDelta(name, mkHist(25, 50, 75), t0.Add(90*time.Second))
	if dur != "1m" {
		t.Fatalf("90s call duration = %q, want 1m (baseline still t=0)", dur)
	}
	if !reflect.DeepEqual(h.Counts, []uint64{15, 30, 45}) {
		t.Fatalf("delta counts at 90s = %v, want [15 30 45]", h.Counts)
	}

	h, dur = r.histogramDelta(name, mkHist(40, 80, 120), t0.Add(140*time.Second))
	if dur != "1m" {
		t.Fatalf("140s call duration = %q, want 1m", dur)
	}
	if !reflect.DeepEqual(h.Counts, []uint64{20, 40, 60}) {
		t.Fatalf("delta counts at 140s = %v, want [20 40 60] (current - baseline at t=70s)", h.Counts)
	}
}

func sampleNames(samples []statekit.PrometheusSample) []string {
	out := make([]string, 0, len(samples))
	for _, s := range samples {
		out = append(out, s.Name)
	}
	return out
}

func TestRecommendedRuntimeMetrics(t *testing.T) {
	runtimeMetrics := NewRuntimeMetrics(WithRecommendedRuntimeMetrics())
	descs := runtimeMetrics.DescribePrometheus()
	if len(descs) == 0 {
		t.Fatal("recommended runtime metrics produced no descriptors")
	}

	got := map[string]struct{}{}
	for _, desc := range descs {
		got[desc.Name] = struct{}{}
	}

	for _, want := range []string{
		"go_runtime_sched_goroutines_goroutines",
		"go_runtime_gc_pauses_seconds",
		"go_runtime_cpu_classes_gc_pause_cpu_seconds",
		"go_runtime_memory_classes_total_bytes",
		"go_runtime_memory_classes_heap_released_bytes",
		"go_runtime_sched_latencies_seconds",
	} {
		if _, ok := got[want]; !ok {
			t.Fatalf("recommended runtime metrics missing %q; got %#v", want, got)
		}
	}
}
