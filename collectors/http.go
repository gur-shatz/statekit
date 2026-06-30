// Package collectors contains ready-to-register metric collectors for common
// runtime and server instrumentation.
package collectors

import (
	"fmt"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gur-shatz/statekit"
	counters "github.com/gur-shatz/statekit/util"
)

const (
	defaultHTTPMetricsWindow   = 5 * time.Minute
	defaultHTTPMetricsSnapshot = time.Second
	httpMetricsGlobalPath      = "-"
)

// HTTPOption configures HTTPMetrics.
type HTTPOption func(*HTTPMetrics)

// HTTPMetrics records a deliberately small global view of an HTTP server.
// Local getters read epoch-weighted current-window estimates. Prometheus also
// receives cumulative counters for scrape-compatible totals.
type HTTPMetrics struct {
	mu              sync.Mutex
	window          time.Duration
	snapshotRefresh time.Duration
	now             func() time.Time
	paths           map[string]*httpPathMetrics
	cachedAt        time.Time
	cachedSnapshot  HTTPMetricsSnapshot

	totalRequests atomic.Uint64
	totalErrors   atomic.Uint64
}

const (
	httpCounterRequests = iota
	httpCounterErrors
	httpCounterDurationNanos
	httpCounterWidth
)

type httpPathMetrics struct {
	totals   *counters.EpochWeightedCounters
	statuses map[int]*counters.EpochWeightedCounters
}

// HTTPMetricsSnapshot is a point-in-time view of the current estimated window.
// It does not contain lifetime totals; Prometheus total counters are exported
// separately from HTTPMetrics.
type HTTPMetricsSnapshot struct {
	// Window is the interval that all estimated fields in this snapshot cover.
	Window time.Duration

	// Requests is the estimated number of HTTP requests observed during Window.
	Requests uint64

	// RequestsPerSecond is Requests divided by Window.Seconds().
	RequestsPerSecond float64

	// Errors is the estimated number of 5xx HTTP responses observed during Window.
	Errors uint64

	// ErrorsPerSecond is Errors divided by Window.Seconds().
	ErrorsPerSecond float64

	// Paths contains estimated measurements grouped by request path.
	// The path "-" is the aggregate across all paths.
	Paths map[string]HTTPPathMetricsSnapshot

	// ResponseCodes estimates responses by HTTP status code during Window.
	ResponseCodes map[int]uint64

	// ErrorURLs estimates 5xx responses by request path and status code during Window.
	ErrorURLs map[string]map[int]uint64

	// UnknownURLs estimates 404 responses by request path during Window.
	UnknownURLs map[string]uint64
}

// HTTPPathMetricsSnapshot is a point-in-time estimated-window view for one
// request path.
type HTTPPathMetricsSnapshot struct {
	// Requests is the estimated number of HTTP requests for this path.
	Requests uint64

	// RequestsPerSecond is Requests divided by Window.Seconds().
	RequestsPerSecond float64

	// Errors is the estimated number of 5xx HTTP responses for this path.
	Errors uint64

	// ErrorsPerSecond is Errors divided by Window.Seconds().
	ErrorsPerSecond float64

	// AverageLatency is the estimated mean request latency for this path.
	AverageLatency time.Duration

	// ResponseCodes estimates responses by HTTP status code for this path.
	ResponseCodes map[int]uint64
}

// NewHTTPMetrics creates a current-window HTTP metrics collector. The default
// local measurement window is five minutes.
func NewHTTPMetrics(opts ...HTTPOption) *HTTPMetrics {
	m := &HTTPMetrics{
		window:          defaultHTTPMetricsWindow,
		snapshotRefresh: defaultHTTPMetricsSnapshot,
		now:             time.Now,
		paths:           map[string]*httpPathMetrics{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithHTTPMetricsWindow sets the estimated window used by local getters,
// snapshots, state checks, and non-total Prometheus gauges.
func WithHTTPMetricsWindow(window time.Duration) HTTPOption {
	return func(m *HTTPMetrics) {
		if window > 0 {
			m.window = window
		}
	}
}

// WithHTTPMetricsSnapshotRefresh sets how often Snapshot recomputes the
// estimated-window measurement cache. Local getters share that cached snapshot.
func WithHTTPMetricsSnapshotRefresh(interval time.Duration) HTTPOption {
	return func(m *HTTPMetrics) {
		if interval >= 0 {
			m.snapshotRefresh = interval
		}
	}
}

func withHTTPMetricsClock(now func() time.Time) HTTPOption {
	return func(m *HTTPMetrics) {
		if now != nil {
			m.now = now
		}
	}
}

// Middleware records every request handled by next into the current estimated
// window and cumulative Prometheus total counters.
func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			m.observe(r.URL.Path, rec.status, time.Since(start))
		}()

		next.ServeHTTP(rec, r)
	})
}

// Handler is an alias for Middleware.
func (m *HTTPMetrics) Handler(next http.Handler) http.Handler {
	return m.Middleware(next)
}

// Window returns the interval used by current local measurement estimates.
func (m *HTTPMetrics) Window() time.Duration {
	return m.window
}

// Requests returns the estimated number of requests in the current window.
func (m *HTTPMetrics) Requests() uint64 {
	return m.Snapshot().Requests
}

// RequestsPerSecond returns estimated requests per second over the current window.
func (m *HTTPMetrics) RequestsPerSecond() float64 {
	return m.Snapshot().RequestsPerSecond
}

// Errors returns the estimated number of 5xx responses in the current window.
func (m *HTTPMetrics) Errors() uint64 {
	return m.Snapshot().Errors
}

// ErrorsPerSecond returns estimated 5xx responses per second over the current window.
func (m *HTTPMetrics) ErrorsPerSecond() float64 {
	return m.Snapshot().ErrorsPerSecond
}

// ErrorRatio returns Errors divided by Requests for the current estimated window.
func (m *HTTPMetrics) ErrorRatio() float64 {
	return m.Snapshot().ErrorRatio()
}

// ErrorPercentage returns ErrorRatio multiplied by 100 for the current estimated window.
func (m *HTTPMetrics) ErrorPercentage() float64 {
	return m.Snapshot().ErrorPercentage()
}

// AverageLatency returns estimated mean request latency over the current window.
func (m *HTTPMetrics) AverageLatency() time.Duration {
	return m.Snapshot().Paths[httpMetricsGlobalPath].AverageLatency
}

// ResponseCodes returns estimated response counts by HTTP status code over the current window.
func (m *HTTPMetrics) ResponseCodes() map[int]uint64 {
	return maps.Clone(m.Snapshot().ResponseCodes)
}

// ErrorURLs returns estimated 5xx response counts by path and status code over the current window.
func (m *HTTPMetrics) ErrorURLs() map[string]map[int]uint64 {
	return cloneHTTPStatusDistribution(m.Snapshot().ErrorURLs)
}

// UnknownURLs returns estimated 404 response counts by path over the current window.
func (m *HTTPMetrics) UnknownURLs() map[string]uint64 {
	return maps.Clone(m.Snapshot().UnknownURLs)
}

// Paths returns estimated measurements grouped by request path.
func (m *HTTPMetrics) Paths() map[string]HTTPPathMetricsSnapshot {
	return cloneHTTPPathMetricsSnapshots(m.Snapshot().Paths)
}

// Snapshot returns a coherent copy of the current estimated-window measurements.
// Repeated calls return a cached snapshot until the snapshot refresh interval
// has elapsed. The default refresh interval is one second; set it to zero to
// recompute on every call.
func (m *HTTPMetrics) Snapshot() HTTPMetricsSnapshot {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cachedAt.IsZero() && m.snapshotRefresh > 0 && now.Sub(m.cachedAt) < m.snapshotRefresh {
		return cloneHTTPMetricsSnapshot(m.cachedSnapshot)
	}

	snap := HTTPMetricsSnapshot{
		Window:        m.window,
		Paths:         map[string]HTTPPathMetricsSnapshot{},
		ResponseCodes: map[int]uint64{},
		ErrorURLs:     map[string]map[int]uint64{},
		UnknownURLs:   map[string]uint64{},
	}
	timestamp := httpMetricsTimestamp(now)
	seconds := m.window.Seconds()
	for path, pathMetrics := range m.paths {
		values := make([]int64, httpCounterWidth)
		pathMetrics.totals.CurrentWindowValue(timestamp, values)
		requests := uint64(max(values[httpCounterRequests], 0))
		errors := uint64(max(values[httpCounterErrors], 0))
		pathDurationNanos := max(values[httpCounterDurationNanos], 0)
		if requests == 0 && errors == 0 && pathDurationNanos == 0 {
			delete(m.paths, path)
			continue
		}

		pathSnap := HTTPPathMetricsSnapshot{
			Requests:      requests,
			Errors:        errors,
			ResponseCodes: map[int]uint64{},
		}
		if requests > 0 {
			pathSnap.AverageLatency = time.Duration(pathDurationNanos / int64(requests))
		}
		if seconds > 0 {
			pathSnap.RequestsPerSecond = float64(requests) / seconds
			pathSnap.ErrorsPerSecond = float64(errors) / seconds
		}

		for status, statusMetrics := range pathMetrics.statuses {
			statusValues := make([]int64, statusMetrics.Len())
			statusMetrics.CurrentWindowValue(timestamp, statusValues)
			count := uint64(max(statusValues[0], 0))
			if count == 0 {
				delete(pathMetrics.statuses, status)
				continue
			}
			pathSnap.ResponseCodes[status] += count
			if path == httpMetricsGlobalPath {
				snap.ResponseCodes[status] += count
				continue
			}
			if status >= http.StatusInternalServerError {
				if snap.ErrorURLs[path] == nil {
					snap.ErrorURLs[path] = map[int]uint64{}
				}
				snap.ErrorURLs[path][status] += count
			}
			if status == http.StatusNotFound {
				snap.UnknownURLs[path] += count
			}
		}
		snap.Paths[path] = pathSnap
	}

	if global := snap.Paths[httpMetricsGlobalPath]; global.Requests > 0 || global.Errors > 0 {
		snap.Requests = global.Requests
		snap.RequestsPerSecond = global.RequestsPerSecond
		snap.Errors = global.Errors
		snap.ErrorsPerSecond = global.ErrorsPerSecond
	}
	m.cachedAt = now
	m.cachedSnapshot = snap
	return cloneHTTPMetricsSnapshot(snap)
}

// ErrorRatio returns Errors divided by Requests for this snapshot's estimated window.
func (s HTTPMetricsSnapshot) ErrorRatio() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Requests)
}

// ErrorPercentage returns ErrorRatio multiplied by 100 for this snapshot's estimated window.
func (s HTTPMetricsSnapshot) ErrorPercentage() float64 {
	return s.ErrorRatio() * 100
}

func (m *HTTPMetrics) DescribePrometheus() []statekit.PrometheusDesc {
	window := m.window.String()
	return []statekit.PrometheusDesc{
		{
			Name: "http_server_requests_total",
			Help: "Total HTTP requests handled.",
			Type: statekit.PrometheusCounter,
		},
		{
			Name: "http_server_errors_total",
			Help: "Total HTTP requests completed with a 5xx status.",
			Type: statekit.PrometheusCounter,
		},
		{
			Name:   "http_server_requests_per_second",
			Help:   fmt.Sprintf("HTTP requests per second by path over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"path"},
		},
		{
			Name:   "http_server_errors_per_second",
			Help:   fmt.Sprintf("HTTP 5xx responses per second by path over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"path"},
		},
		{
			Name:   "http_server_average_latency_seconds",
			Help:   fmt.Sprintf("Average HTTP request latency in seconds by path over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"path"},
		},
		{
			Name:   "http_server_response_codes",
			Help:   fmt.Sprintf("HTTP responses by status code over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"code"},
		},
		{
			Name:   "http_server_error_urls",
			Help:   fmt.Sprintf("HTTP 5xx responses by path and status code over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"path", "code"},
		},
		{
			Name:   "http_server_unknown_urls",
			Help:   fmt.Sprintf("HTTP 404 responses by path over the current %s estimated window.", window),
			Type:   statekit.PrometheusGauge,
			Labels: []string{"path"},
		},
	}
}

func (m *HTTPMetrics) CollectPrometheus() []statekit.PrometheusSample {
	snap := m.Snapshot()
	samples := []statekit.PrometheusSample{
		{Name: "http_server_requests_total", Value: float64(m.totalRequests.Load())},
		{Name: "http_server_errors_total", Value: float64(m.totalErrors.Load())},
	}
	for code, count := range snap.ResponseCodes {
		samples = append(samples, statekit.PrometheusSample{
			Name:   "http_server_response_codes",
			Labels: map[string]string{"code": fmt.Sprint(code)},
			Value:  float64(count),
		})
	}
	for path, pathSnap := range snap.Paths {
		samples = append(samples,
			statekit.PrometheusSample{
				Name:   "http_server_requests_per_second",
				Labels: map[string]string{"path": path},
				Value:  pathSnap.RequestsPerSecond,
			},
			statekit.PrometheusSample{
				Name:   "http_server_errors_per_second",
				Labels: map[string]string{"path": path},
				Value:  pathSnap.ErrorsPerSecond,
			},
			statekit.PrometheusSample{
				Name:   "http_server_average_latency_seconds",
				Labels: map[string]string{"path": path},
				Value:  pathSnap.AverageLatency.Seconds(),
			},
		)
	}
	for path, byStatus := range snap.ErrorURLs {
		for code, count := range byStatus {
			samples = append(samples, statekit.PrometheusSample{
				Name:   "http_server_error_urls",
				Labels: map[string]string{"path": path, "code": fmt.Sprint(code)},
				Value:  float64(count),
			})
		}
	}
	for path, count := range snap.UnknownURLs {
		samples = append(samples, statekit.PrometheusSample{
			Name:   "http_server_unknown_urls",
			Labels: map[string]string{"path": path},
			Value:  float64(count),
		})
	}
	return samples
}

func (m *HTTPMetrics) observe(path string, status int, duration time.Duration) {
	m.totalRequests.Add(1)
	if status >= http.StatusInternalServerError {
		m.totalErrors.Add(1)
	}

	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paths == nil {
		m.paths = map[string]*httpPathMetrics{}
	}

	m.observePathLocked(httpMetricsGlobalPath, status, duration, now)
	m.observePathLocked(path, status, duration, now)
}

func (m *HTTPMetrics) observePathLocked(path string, status int, duration time.Duration, now time.Time) {
	pathMetrics := m.paths[path]
	if pathMetrics == nil {
		pathMetrics = &httpPathMetrics{
			totals:   counters.NewEpochWeightedCounters(httpMetricsSpan(m.window), httpMetricsTimestamp(now), httpCounterWidth),
			statuses: map[int]*counters.EpochWeightedCounters{},
		}
		m.paths[path] = pathMetrics
	}

	values := [httpCounterWidth]int64{
		httpCounterRequests:      1,
		httpCounterDurationNanos: duration.Nanoseconds(),
	}
	if status >= http.StatusInternalServerError {
		values[httpCounterErrors] = 1
	}
	pathMetrics.totals.Add(httpMetricsTimestamp(now), values[:])

	statusMetrics := pathMetrics.statuses[status]
	if statusMetrics == nil {
		statusMetrics = counters.NewEpochWeightedCounters(httpMetricsSpan(m.window), httpMetricsTimestamp(now), 1)
		pathMetrics.statuses[status] = statusMetrics
	}
	statusMetrics.Add(httpMetricsTimestamp(now), []int64{1})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.wrote = true
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if !r.wrote {
		r.wrote = true
	}
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func cloneHTTPStatusDistribution(in map[string]map[int]uint64) map[string]map[int]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[int]uint64, len(in))
	for path, byStatus := range in {
		out[path] = maps.Clone(byStatus)
	}
	return out
}

func cloneHTTPMetricsSnapshot(s HTTPMetricsSnapshot) HTTPMetricsSnapshot {
	s.Paths = cloneHTTPPathMetricsSnapshots(s.Paths)
	s.ResponseCodes = maps.Clone(s.ResponseCodes)
	s.ErrorURLs = cloneHTTPStatusDistribution(s.ErrorURLs)
	s.UnknownURLs = maps.Clone(s.UnknownURLs)
	return s
}

func cloneHTTPPathMetricsSnapshots(in map[string]HTTPPathMetricsSnapshot) map[string]HTTPPathMetricsSnapshot {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]HTTPPathMetricsSnapshot, len(in))
	for path, snap := range in {
		snap.ResponseCodes = maps.Clone(snap.ResponseCodes)
		out[path] = snap
	}
	return out
}

func httpMetricsSpan(window time.Duration) uint32 {
	seconds := uint32(window / time.Second)
	if seconds == 0 {
		return 1
	}
	return seconds
}

func httpMetricsTimestamp(t time.Time) uint32 {
	return uint32(t.Unix())
}
