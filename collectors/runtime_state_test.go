package collectors

import (
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

type fakeRuntimeMetrics struct {
	value float64
	ok    bool
}

func (f *fakeRuntimeMetrics) Value(string) (float64, bool) {
	return f.value, f.ok
}

func TestRuntimeIncreasingTrendCheck(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	metrics := &fakeRuntimeMetrics{ok: true}
	check := newRuntimeIncreasingTrendCheck(
		metrics,
		"runtime memory growth",
		"go_runtime_memory_classes_total_bytes",
		time.Minute,
		3,
		10,
		25,
		0,
		WithRuntimeTrendSampleInterval(0),
		withRuntimeTrendClock(func() time.Time { return now }),
	)

	metrics.value = 100
	if current := check.Current(); current.Status != statekit.Pass {
		t.Fatalf("first status = %s, want pass", current.Status)
	}

	now = now.Add(10 * time.Second)
	metrics.value = 250
	if current := check.Current(); current.Status != statekit.Pass {
		t.Fatalf("second status = %s, want pass before min samples", current.Status)
	}

	now = now.Add(10 * time.Second)
	metrics.value = 700
	current := check.Current()
	if current.Status != statekit.Fail {
		t.Fatalf("status = %s, want fail", current.Status)
	}
	if !strings.Contains(current.Reason, "above fail threshold") {
		t.Fatalf("reason = %q, want fail threshold", current.Reason)
	}
	if current.Data["growth_per_second"] != 30.0 {
		t.Fatalf("growth_per_second = %#v, want 30", current.Data["growth_per_second"])
	}
}

func TestRuntimeIncreasingTrendCheckPassesWhenGrowthStops(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	metrics := &fakeRuntimeMetrics{ok: true}
	check := newRuntimeIncreasingTrendCheck(
		metrics,
		"runtime memory growth",
		"go_runtime_memory_classes_total_bytes",
		time.Minute,
		2,
		10,
		0,
		0,
		WithRuntimeTrendSampleInterval(0),
		withRuntimeTrendClock(func() time.Time { return now }),
	)

	metrics.value = 200
	check.Current()

	now = now.Add(10 * time.Second)
	metrics.value = 150
	current := check.Current()
	if current.Status != statekit.Pass {
		t.Fatalf("status = %s, want pass", current.Status)
	}
	if current.Data["growth"].(float64) >= 0 {
		t.Fatalf("growth = %#v, want negative", current.Data["growth"])
	}
}

func TestRuntimeIncreasingTrendCheckMissingMetric(t *testing.T) {
	check := newRuntimeIncreasingTrendCheck(
		&fakeRuntimeMetrics{},
		"runtime memory growth",
		"missing",
		time.Minute,
		2,
		10,
		0,
		0,
		withRuntimeTrendClock(func() time.Time { return time.Now() }),
	)

	current := check.Current()
	if current.Status != statekit.Down {
		t.Fatalf("status = %s, want down", current.Status)
	}
}
