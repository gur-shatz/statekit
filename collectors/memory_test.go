package collectors

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gur-shatz/statekit"
)

var errMemoryProfileTest = errors.New("profile failed")

func TestMemoryMetricsCollectPrometheus(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{
			GoAllocBytes:        10,
			GoTotalAllocBytes:   20,
			GoSysBytes:          30,
			GoHeapAllocBytes:    40,
			GoHeapSysBytes:      50,
			GoHeapIdleBytes:     60,
			GoHeapReleasedBytes: 70,
			GoStackInuseBytes:   80,
			GoObjects:           90,
			OSRSSBytes:          100,
			OSRSSAvailable:      true,
			OSPeakRSSBytes:      110,
			OSPeakRSSAvailable:  true,
		}
	}))

	samples := metrics.CollectPrometheus()
	got := map[string]float64{}
	for _, sample := range samples {
		got[sample.Name] = sample.Value
	}

	for name, want := range map[string]float64{
		"process_memory_go_alloc_bytes":         10,
		"process_memory_go_total_alloc_bytes":   20,
		"process_memory_go_sys_bytes":           30,
		"process_memory_go_heap_alloc_bytes":    40,
		"process_memory_go_heap_sys_bytes":      50,
		"process_memory_go_heap_idle_bytes":     60,
		"process_memory_go_heap_released_bytes": 70,
		"process_memory_go_stack_inuse_bytes":   80,
		"process_memory_go_objects":             90,
		"process_memory_os_rss_bytes":           100,
		"process_memory_os_peak_rss_bytes":      110,
	} {
		if got[name] != want {
			t.Fatalf("%s = %f, want %f; got %#v", name, got[name], want, got)
		}
	}
}

func TestMemoryMetricsDescribeByteUnits(t *testing.T) {
	for _, desc := range NewMemoryMetrics().DescribePrometheus() {
		if strings.HasSuffix(desc.Name, "_bytes") && desc.Unit != "bytes" {
			t.Fatalf("%s unit = %q, want bytes", desc.Name, desc.Unit)
		}
		if !strings.HasSuffix(desc.Name, "_bytes") && desc.Unit != "" {
			t.Fatalf("%s unexpected unit = %q", desc.Name, desc.Unit)
		}
	}
}

func TestMemoryMetricsOmitsUnavailableOSPrometheusSamples(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 30}
	}))

	for _, sample := range metrics.CollectPrometheus() {
		if sample.Name == "process_memory_os_rss_bytes" || sample.Name == "process_memory_os_peak_rss_bytes" {
			t.Fatalf("unexpected unavailable OS sample: %#v", sample)
		}
	}
}

func TestMemoryUsageCheckUsesOSRSSWhenAvailable(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{
			GoSysBytes:     50,
			OSRSSBytes:     120,
			OSRSSAvailable: true,
		}
	}))
	check := NewMemoryUsageCheck(metrics, "memory", 100, 200)

	current := check.Current()
	if current.Status != statekit.Warn {
		t.Fatalf("status = %s, want warn", current.Status)
	}
	if current.Data["usage_bytes"] != uint64(120) {
		t.Fatalf("usage_bytes = %#v, want 120", current.Data["usage_bytes"])
	}
	if current.Data["usage_source"] != "os_rss_bytes" {
		t.Fatalf("usage_source = %#v, want os_rss_bytes", current.Data["usage_source"])
	}
}

func TestMemoryUsageCheckFallsBackToGoSys(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 220}
	}))
	check := NewMemoryUsageCheck(metrics, "memory", 100, 200)

	current := check.Current()
	if current.Status != statekit.Fail {
		t.Fatalf("status = %s, want fail", current.Status)
	}
	if current.Data["usage_source"] != "go_sys_bytes" {
		t.Fatalf("usage_source = %#v, want go_sys_bytes", current.Data["usage_source"])
	}
}

func TestNewMemoryTracker(t *testing.T) {
	tracker := NewMemoryTracker("memory", 100, 200, withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoHeapAllocBytes: 10}
	}))
	if tracker.Metrics == nil {
		t.Fatal("tracker metrics is nil")
	}
	if tracker.State == nil {
		t.Fatal("tracker state is nil")
	}
	if tracker.State.Name() != "memory" {
		t.Fatalf("tracker state name = %q, want memory", tracker.State.Name())
	}
}

func TestMemoryThresholdsFromOSFallback(t *testing.T) {
	warnAt, failAt, available, ok := MemoryThresholdsFromOS(0.50, 0.75)
	if !ok {
		if available != 1<<30 {
			t.Fatalf("fallback available = %d, want %d", available, uint64(1<<30))
		}
		if warnAt != 512<<20 || failAt != 768<<20 {
			t.Fatalf("fallback thresholds = %d/%d, want %d/%d", warnAt, failAt, uint64(512<<20), uint64(768<<20))
		}
		return
	}
	if available == 0 || warnAt == 0 || failAt == 0 {
		t.Fatalf("thresholds should be non-zero when available memory is reported: warn=%d fail=%d available=%d", warnAt, failAt, available)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name  string
		bytes uint64
		want  string
	}{
		{name: "bytes", bytes: 512, want: "512 B"},
		{name: "kib", bytes: 1536, want: "1.50 KiB"},
		{name: "mib", bytes: 153042296, want: "145.95 MiB"},
		{name: "gib", bytes: 3 << 30, want: "3.00 GiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatBytes(tt.bytes); got != tt.want {
				t.Fatalf("FormatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

func TestMemoryUsageCheckWritesHeapProfileOnThresholdCrossing(t *testing.T) {
	values := []uint64{50, 120, 130, 220, 210, 80, 120}
	var profiles []string
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		v := values[0]
		values = values[1:]
		return MemorySnapshot{GoSysBytes: v}
	}))
	check := NewMemoryUsageCheck(
		metrics,
		"memory",
		100,
		200,
		WithMemoryHeapProfileDir(t.TempDir()),
		withMemoryHeapProfileWriter(func(dir string) (string, error) {
			path := dir + "/heap.pprof"
			profiles = append(profiles, path)
			return path, nil
		}),
	)

	for range 7 {
		check.Current()
	}

	if len(profiles) != 3 {
		t.Fatalf("profiles written = %d, want 3: %#v", len(profiles), profiles)
	}
}

func TestMemoryUsageCheckReportsHeapProfileError(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 120}
	}))
	check := NewMemoryUsageCheck(
		metrics,
		"memory",
		100,
		200,
		WithMemoryHeapProfileDir(t.TempDir()),
		withMemoryHeapProfileWriter(func(string) (string, error) {
			return "", errMemoryProfileTest
		}),
	)

	current := check.Current()
	if current.Data["heap_profile_error"] != errMemoryProfileTest.Error() {
		t.Fatalf("heap_profile_error = %#v, want %q", current.Data["heap_profile_error"], errMemoryProfileTest.Error())
	}
}

func TestMemoryHandlerFuncJSON(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 120}
	}))
	response := httptest.NewRecorder()

	MemoryHandlerFunc(metrics, "json")(response, httptest.NewRequest(http.MethodGet, "/memory", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := response.Body.String()
	for _, want := range []string{`"usage": "120 B"`, `"usage_bytes": 120`, `"usage_source": "go_sys_bytes"`, `"go_sys": "120 B"`, `"go_sys_bytes": 120`} {
		if !strings.Contains(body, want) {
			t.Fatalf("memory json missing %q:\n%s", want, body)
		}
	}
}

func TestMemoryHandlerFuncYAML(t *testing.T) {
	metrics := NewMemoryMetrics(withMemoryReader(func() MemorySnapshot {
		return MemorySnapshot{GoSysBytes: 120}
	}))
	response := httptest.NewRecorder()

	metrics.Handler("yaml").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/memory", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/yaml; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := response.Body.String()
	for _, want := range []string{"usage: 120 B", "usage_bytes: 120", "usage_source: go_sys_bytes", "go_sys: 120 B", "go_sys_bytes: 120"} {
		if !strings.Contains(body, want) {
			t.Fatalf("memory yaml missing %q:\n%s", want, body)
		}
	}
}

func TestMemoryHandlerFuncUnsupportedFormat(t *testing.T) {
	response := httptest.NewRecorder()

	MemoryHandlerFunc(NewMemoryMetrics(), "toml")(response, httptest.NewRequest(http.MethodGet, "/memory", nil))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	if !strings.Contains(response.Body.String(), `unsupported memory format "toml"`) {
		t.Fatalf("body = %q", response.Body.String())
	}
}
