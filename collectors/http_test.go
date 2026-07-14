package collectors

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

func TestHTTPMetricsExposeLocalMeasurements(t *testing.T) {
	metrics := NewHTTPMetrics()
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))

	if metrics.Requests() != 2 {
		t.Fatalf("requests = %d, want 2", metrics.Requests())
	}
	if metrics.Errors() != 1 {
		t.Fatalf("errors = %d, want 1", metrics.Errors())
	}
	if metrics.ErrorRatio() != 0.5 {
		t.Fatalf("error ratio = %f, want 0.5", metrics.ErrorRatio())
	}
	if metrics.ErrorPercentage() != 50 {
		t.Fatalf("error percentage = %f, want 50", metrics.ErrorPercentage())
	}
	if metrics.RequestsPerSecond() <= 0 {
		t.Fatalf("requests per second = %f, want positive", metrics.RequestsPerSecond())
	}
	if metrics.ErrorsPerSecond() <= 0 {
		t.Fatalf("errors per second = %f, want positive", metrics.ErrorsPerSecond())
	}
	if metrics.AverageLatency() < 0 {
		t.Fatalf("average latency = %s, want non-negative", metrics.AverageLatency())
	}
}

func TestHTTPMetricsExposeCurrentDistributions(t *testing.T) {
	metrics := NewHTTPMetrics(WithHTTPMetricsSnapshotRefresh(0))
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fail":
			http.Error(w, "boom", http.StatusInternalServerError)
		case "/bad-gateway":
			http.Error(w, "upstream", http.StatusBadGateway)
		case "/missing":
			http.NotFound(w, r)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bad-gateway", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/missing", nil))

	if got := metrics.ResponseCodes()[http.StatusNoContent]; got != 1 {
		t.Fatalf("204 responses = %d, want 1", got)
	}
	if got := metrics.ResponseCodes()[http.StatusInternalServerError]; got != 1 {
		t.Fatalf("500 responses = %d, want 1", got)
	}
	if got := metrics.ErrorURLs()["/fail"][http.StatusInternalServerError]; got != 1 {
		t.Fatalf("/fail 500 errors = %d, want 1", got)
	}
	if got := metrics.ErrorURLs()["/bad-gateway"][http.StatusBadGateway]; got != 1 {
		t.Fatalf("/bad-gateway 502 errors = %d, want 1", got)
	}
	if got := metrics.UnknownURLs()["/missing"]; got != 1 {
		t.Fatalf("/missing unknown URLs = %d, want 1", got)
	}
}

func TestHTTPMetricsExposePathMeasurements(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		WithHTTPMetricsSnapshotRefresh(0),
		withHTTPMetricsClock(func() time.Time { return now }),
	)

	metrics.observe("/fast", http.StatusNoContent, 10*time.Millisecond)
	metrics.observe("/slow", http.StatusInternalServerError, 100*time.Millisecond)

	snap := metrics.Snapshot()
	if got, want := snap.Paths["-"].Requests, uint64(2); got != want {
		t.Fatalf("aggregate requests = %d, want %d", got, want)
	}
	if got, want := snap.Paths["-"].Errors, uint64(1); got != want {
		t.Fatalf("aggregate errors = %d, want %d", got, want)
	}
	if got, want := snap.Paths["/slow"].AverageLatency, 100*time.Millisecond; got != want {
		t.Fatalf("/slow latency = %s, want %s", got, want)
	}
	if got, want := snap.Paths["/fast"].AverageLatency, 10*time.Millisecond; got != want {
		t.Fatalf("/fast latency = %s, want %s", got, want)
	}
	if got, want := snap.Paths["/slow"].ResponseCodes[http.StatusInternalServerError], uint64(1); got != want {
		t.Fatalf("/slow 500s = %d, want %d", got, want)
	}
}

func TestHTTPMetricsPrometheusExportsGlobalMeasurements(t *testing.T) {
	reg := statekit.NewRegistry()
	metrics := NewHTTPMetrics()
	if err := reg.RegisterCollectors(metrics); err != nil {
		t.Fatal(err)
	}

	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/missing", nil))

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# TYPE http_server_requests_total counter",
		"# TYPE http_server_errors_total counter",
		"# HELP http_server_requests_per_second HTTP requests per second by path over the current 5m0s estimated window.",
		"# TYPE http_server_requests_per_second gauge",
		"# HELP http_server_errors_per_second HTTP 5xx responses per second by path over the current 5m0s estimated window.",
		"# TYPE http_server_errors_per_second gauge",
		"# HELP http_server_average_latency_seconds Average HTTP request latency in seconds by path over the current 5m0s estimated window.",
		"# TYPE http_server_average_latency_seconds gauge",
		"# HELP http_server_response_codes HTTP responses by status code over the current 5m0s estimated window.",
		"# TYPE http_server_response_codes gauge",
		"# HELP http_server_error_urls HTTP 5xx responses by path and status code over the current 5m0s estimated window.",
		"# TYPE http_server_error_urls gauge",
		"# HELP http_server_unknown_urls HTTP 404 responses by path over the current 5m0s estimated window.",
		"# TYPE http_server_unknown_urls gauge",
		"http_server_requests_total 2",
		"http_server_errors_total 1",
		`http_server_requests_per_second{path="-"} 0.006667`,
		`http_server_errors_per_second{path="-"} 0.003333`,
		`http_server_response_codes{code="404"} 1`,
		`http_server_response_codes{code="500"} 1`,
		`http_server_error_urls{code="500",path="/fail"} 1`,
		`http_server_unknown_urls{path="/missing"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestHTTPMetricsPrometheusExportsRollingAverageLatencyByPath(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		WithHTTPMetricsSnapshotRefresh(0),
		withHTTPMetricsClock(func() time.Time { return now }),
	)
	reg := statekit.NewRegistry()
	if err := reg.RegisterCollectors(metrics); err != nil {
		t.Fatal(err)
	}

	// Record a full previous bucket for each path.
	for range 10 {
		metrics.observe("/fast", http.StatusNoContent, 10*time.Millisecond)
		metrics.observe("/slow", http.StatusNoContent, 100*time.Millisecond)
	}

	// At 30 seconds into the next minute, the prior bucket has 50% weight.
	now = now.Add(90 * time.Second)
	for range 5 {
		metrics.observe("/fast", http.StatusNoContent, 20*time.Millisecond)
		metrics.observe("/slow", http.StatusNoContent, 200*time.Millisecond)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		`http_server_average_latency_seconds{path="/fast"} 0.015`,
		`http_server_average_latency_seconds{path="/slow"} 0.15`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestHTTPChecksAggregateOverMetrics(t *testing.T) {
	metrics := NewHTTPMetrics()
	httpState := statekit.NewStateAggregator("http")
	httpState.Add(
		NewHTTPErrorRatioCheck(metrics, "http errors", 2, 0.25, 0.5, 0),
		NewHTTPAverageLatencyCheck(metrics, "http latency", time.Nanosecond, 0, 0),
	)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))

	snap := httpState.Snapshot()
	if snap.Status != statekit.Fail {
		t.Fatalf("http state = %v, want fail: %+v", snap.Status, snap)
	}
	if len(snap.Checks) != 2 {
		t.Fatalf("checks len = %d, want 2: %+v", len(snap.Checks), snap)
	}
	errorCheck := snap.Checks[0]
	if errorCheck.Status != statekit.Fail {
		t.Fatalf("error check = %v, want fail: %+v", errorCheck.Status, errorCheck)
	}
	if !strings.Contains(errorCheck.Reason, "error ratio") {
		t.Fatalf("error check reason = %q, want error ratio", errorCheck.Reason)
	}
	if got := errorCheck.Data["requests"]; got != uint64(2) {
		t.Fatalf("requests data = %#v, want 2", got)
	}
	if got := errorCheck.Data["error_ratio"]; got != float64(0.5) {
		t.Fatalf("error_ratio data = %#v, want 0.5", got)
	}
}

func TestHTTPCheckCanBeUsedDirectly(t *testing.T) {
	metrics := NewHTTPMetrics()
	check := NewHTTPErrorsPerSecondCheck(metrics, "http error rate", 0, 0.000001, 0)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))

	current := check.Current()
	if current.Status != statekit.Fail {
		t.Fatalf("current check = %v, want fail: %+v", current.Status, current)
	}

	snap := check.Snapshot()
	if snap.Status != statekit.Fail {
		t.Fatalf("direct check = %v, want fail: %+v", snap.Status, snap)
	}
	if got := snap.Data["errors"]; got != uint64(1) {
		t.Fatalf("errors data = %#v, want 1", got)
	}
	if got, ok := snap.Data["errors_per_second"].(float64); !ok || got <= 0 {
		t.Fatalf("errors_per_second data = %#v, want positive float64", snap.Data["errors_per_second"])
	}
}

func TestHTTPErrorRatioWaitsForMinimumSamples(t *testing.T) {
	metrics := NewHTTPMetrics()
	check := NewHTTPErrorRatioCheck(metrics, "http errors", 2, 0, 0.01, 0)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))

	snap := check.Snapshot()
	if snap.Status != statekit.Pass {
		t.Fatalf("check = %v, want pass before minimum samples: %+v", snap.Status, snap)
	}
	if got := snap.Data["error_ratio"]; got != float64(1) {
		t.Fatalf("error_ratio data = %#v, want 1", got)
	}
}

func TestHTTPMetricsWindowAgesMeasurements(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		withHTTPMetricsClock(func() time.Time { return now }),
	)
	check := NewHTTPErrorRatioCheck(metrics, "http errors", 1, 0, 0.01, 0)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	if snap := check.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("check before aging = %v, want fail: %+v", snap.Status, snap)
	}

	now = now.Add(2 * time.Minute)
	snap := check.Snapshot()
	if snap.Status != statekit.Pass {
		t.Fatalf("check after aging = %v, want pass: %+v", snap.Status, snap)
	}
	if got := snap.Data["requests"]; got != uint64(0) {
		t.Fatalf("aged requests = %#v, want 0", got)
	}
	if total := metrics.totalRequests.Load(); total != 1 {
		t.Fatalf("total requests = %d, want cumulative counter to remain 1", total)
	}
}

func TestHTTPMetricsSnapshotIsCachedBriefly(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		WithHTTPMetricsSnapshotRefresh(time.Second),
		withHTTPMetricsClock(func() time.Time { return now }),
	)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	first := metrics.Snapshot()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	second := metrics.Snapshot()
	if second.Requests != first.Requests {
		t.Fatalf("cached requests = %d, want %d", second.Requests, first.Requests)
	}

	now = now.Add(time.Second)
	third := metrics.Snapshot()
	if third.Requests != 2 {
		t.Fatalf("refreshed requests = %d, want 2", third.Requests)
	}
}

func TestHTTPMetricsStorageGrowsByPathAndStatusNotObservations(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		withHTTPMetricsClock(func() time.Time { return now }),
	)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for range 100 {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	}

	metrics.mu.Lock()
	if got, want := len(metrics.paths), 2; got != want {
		metrics.mu.Unlock()
		t.Fatalf("paths len = %d, want %d", got, want)
	}
	if _, ok := metrics.paths["-"]; !ok {
		metrics.mu.Unlock()
		t.Fatal("missing aggregate path")
	}
	if got, want := len(metrics.paths["/ok"].statuses), 1; got != want {
		metrics.mu.Unlock()
		t.Fatalf("/ok statuses len = %d, want %d", got, want)
	}
	metrics.mu.Unlock()

	now = now.Add(2 * time.Minute)
	metrics.Snapshot()

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if got := len(metrics.paths); got != 0 {
		t.Fatalf("paths len after aging = %d, want 0", got)
	}
}

func TestHTTPCheckRefreshesDataWithoutStatusTransition(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(withHTTPMetricsClock(func() time.Time { return now }))
	check := NewHTTPErrorCountCheck(metrics, "http errors", 0, 0, 0)
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	first := check.Snapshot()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	now = now.Add(time.Second)
	second := check.Snapshot()

	if first.Status != statekit.Pass || second.Status != statekit.Pass {
		t.Fatalf("status = %v then %v, want pass", first.Status, second.Status)
	}
	if got := second.Data["requests"]; got != uint64(2) {
		t.Fatalf("requests data = %#v, want 2", got)
	}
	if !first.ChangedAt.Equal(second.ChangedAt) {
		t.Fatalf("changed_at moved on data-only update: %s != %s", second.ChangedAt, first.ChangedAt)
	}
}

func TestHTTPCheckReasonIsStableAcrossMeasurementChanges(t *testing.T) {
	metrics := NewHTTPMetrics(WithHTTPMetricsSnapshotRefresh(0))
	check := NewHTTPAverageLatencyCheck(metrics, "http latency", time.Nanosecond, 0, 0)
	requests := 0
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		time.Sleep(time.Duration(requests) * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	first := check.Snapshot()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	second := check.Snapshot()

	if first.Status != statekit.Warn || second.Status != statekit.Warn {
		t.Fatalf("status = %v then %v, want warn", first.Status, second.Status)
	}
	if first.Reason != second.Reason {
		t.Fatalf("reason changed for same rule: %q != %q", second.Reason, first.Reason)
	}
	if !first.ChangedAt.Equal(second.ChangedAt) {
		t.Fatalf("changed_at moved on same-rule measurement update: %s != %s", second.ChangedAt, first.ChangedAt)
	}
	if len(second.History) != len(first.History) {
		t.Fatalf("history grew on same-rule measurement update: %d != %d", len(second.History), len(first.History))
	}
}

func TestHTTPAverageLatencyCheckReportsSlowPaths(t *testing.T) {
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	metrics := NewHTTPMetrics(
		WithHTTPMetricsWindow(time.Minute),
		WithHTTPMetricsSnapshotRefresh(0),
		withHTTPMetricsClock(func() time.Time { return now }),
	)
	check := NewHTTPAverageLatencyCheck(metrics, "http latency", 10*time.Millisecond, 0, 0)

	metrics.observe("/fast", http.StatusNoContent, time.Millisecond)
	metrics.observe("/slow", http.StatusNoContent, 100*time.Millisecond)

	snap := check.Snapshot()
	if snap.Status != statekit.Warn {
		t.Fatalf("status = %v, want warn: %+v", snap.Status, snap)
	}
	slowPaths, ok := snap.Data["slow_paths"].([]map[string]any)
	if !ok {
		t.Fatalf("slow_paths = %#v, want []map[string]any", snap.Data["slow_paths"])
	}
	if len(slowPaths) == 0 {
		t.Fatal("slow_paths is empty")
	}
	if got, want := slowPaths[0]["path"], "/slow"; got != want {
		t.Fatalf("first slow path = %#v, want %q", got, want)
	}
}
