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
