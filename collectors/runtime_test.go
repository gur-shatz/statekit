package collectors

import (
	"bytes"
	"runtime/metrics"
	"strings"
	"testing"

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
