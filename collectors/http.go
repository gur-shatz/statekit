// Package collectors contains ready-to-register metric collectors for common
// runtime and server instrumentation.
package collectors

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gur-shatz/statekit"
)

const (
	defaultHTTPMetricsWindow   = 5 * time.Minute
	defaultHTTPMetricsSnapshot = time.Second
)

// HTTPOption configures HTTPMetrics.
type HTTPOption func(*HTTPMetrics)

// HTTPMetrics records a deliberately small global view of an HTTP server.
// Local getters read the current rolling window. Prometheus also receives
// cumulative counters for scrape-compatible totals.
type HTTPMetrics struct {
	mu              sync.Mutex
	window          time.Duration
	snapshotRefresh time.Duration
	now             func() time.Time
	observations    []httpObservation
	cachedAt        time.Time
	cachedSnapshot  HTTPMetricsSnapshot

	totalRequests atomic.Uint64
	totalErrors   atomic.Uint64
}

type httpObservation struct {
	at       time.Time
	status   int
	duration time.Duration
}

// HTTPMetricsSnapshot is a point-in-time view of the current rolling window.
// It does not contain lifetime totals; Prometheus total counters are exported
// separately from HTTPMetrics.
type HTTPMetricsSnapshot struct {
	// Window is the rolling interval that all fields in this snapshot cover.
	Window time.Duration

	// Requests is the number of HTTP requests observed during Window.
	Requests uint64

	// RequestsPerSecond is Requests divided by Window.Seconds().
	RequestsPerSecond float64

	// Errors is the number of 5xx HTTP responses observed during Window.
	Errors uint64

	// ErrorsPerSecond is Errors divided by Window.Seconds().
	ErrorsPerSecond float64

	// AverageLatency is the mean request latency for requests observed during Window.
	AverageLatency time.Duration
}

// NewHTTPMetrics creates a current-window HTTP metrics collector. The default
// local measurement window is five minutes.
func NewHTTPMetrics(opts ...HTTPOption) *HTTPMetrics {
	m := &HTTPMetrics{
		window:          defaultHTTPMetricsWindow,
		snapshotRefresh: defaultHTTPMetricsSnapshot,
		now:             time.Now,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithHTTPMetricsWindow sets the rolling window used by local getters,
// snapshots, state checks, and non-total Prometheus gauges.
func WithHTTPMetricsWindow(window time.Duration) HTTPOption {
	return func(m *HTTPMetrics) {
		if window > 0 {
			m.window = window
		}
	}
}

// WithHTTPMetricsSnapshotRefresh sets how often Snapshot recomputes the
// rolling-window measurement cache. Local getters share that cached snapshot.
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

// Middleware records every request handled by next into the current rolling
// window and cumulative Prometheus total counters.
func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			m.observe(rec.status, time.Since(start))
		}()

		next.ServeHTTP(rec, r)
	})
}

// Handler is an alias for Middleware.
func (m *HTTPMetrics) Handler(next http.Handler) http.Handler {
	return m.Middleware(next)
}

// Window returns the rolling interval used by current local measurements.
func (m *HTTPMetrics) Window() time.Duration {
	return m.window
}

// Requests returns the number of requests observed in the current rolling window.
func (m *HTTPMetrics) Requests() uint64 {
	return m.Snapshot().Requests
}

// RequestsPerSecond returns requests per second over the current rolling window.
func (m *HTTPMetrics) RequestsPerSecond() float64 {
	return m.Snapshot().RequestsPerSecond
}

// Errors returns the number of 5xx responses observed in the current rolling window.
func (m *HTTPMetrics) Errors() uint64 {
	return m.Snapshot().Errors
}

// ErrorsPerSecond returns 5xx responses per second over the current rolling window.
func (m *HTTPMetrics) ErrorsPerSecond() float64 {
	return m.Snapshot().ErrorsPerSecond
}

// ErrorRatio returns Errors divided by Requests for the current rolling window.
func (m *HTTPMetrics) ErrorRatio() float64 {
	return m.Snapshot().ErrorRatio()
}

// ErrorPercentage returns ErrorRatio multiplied by 100 for the current rolling window.
func (m *HTTPMetrics) ErrorPercentage() float64 {
	return m.Snapshot().ErrorPercentage()
}

// AverageLatency returns mean request latency over the current rolling window.
func (m *HTTPMetrics) AverageLatency() time.Duration {
	return m.Snapshot().AverageLatency
}

// Snapshot returns a coherent copy of the current rolling-window measurements.
// Repeated calls return a cached snapshot until the snapshot refresh interval
// has elapsed. The default refresh interval is one second; set it to zero to
// recompute on every call.
func (m *HTTPMetrics) Snapshot() HTTPMetricsSnapshot {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cachedAt.IsZero() && m.snapshotRefresh > 0 && now.Sub(m.cachedAt) < m.snapshotRefresh {
		return m.cachedSnapshot
	}

	m.pruneLocked(now)
	snap := HTTPMetricsSnapshot{Window: m.window}
	var durationSum time.Duration
	for _, obs := range m.observations {
		snap.Requests++
		if obs.status >= http.StatusInternalServerError {
			snap.Errors++
		}
		durationSum += obs.duration
	}
	if snap.Requests > 0 {
		snap.AverageLatency = durationSum / time.Duration(snap.Requests)
	}
	seconds := m.window.Seconds()
	if seconds > 0 {
		snap.RequestsPerSecond = float64(snap.Requests) / seconds
		snap.ErrorsPerSecond = float64(snap.Errors) / seconds
	}
	m.cachedAt = now
	m.cachedSnapshot = snap
	return snap
}

// ErrorRatio returns Errors divided by Requests for this snapshot's rolling window.
func (s HTTPMetricsSnapshot) ErrorRatio() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Requests)
}

// ErrorPercentage returns ErrorRatio multiplied by 100 for this snapshot's rolling window.
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
			Name: "http_server_requests_per_second",
			Help: fmt.Sprintf("HTTP requests per second over the current %s rolling window.", window),
			Type: statekit.PrometheusGauge,
		},
		{
			Name: "http_server_errors_per_second",
			Help: fmt.Sprintf("HTTP 5xx responses per second over the current %s rolling window.", window),
			Type: statekit.PrometheusGauge,
		},
		{
			Name: "http_server_average_latency_seconds",
			Help: fmt.Sprintf("Average HTTP request latency in seconds over the current %s rolling window.", window),
			Type: statekit.PrometheusGauge,
		},
	}
}

func (m *HTTPMetrics) CollectPrometheus() []statekit.PrometheusSample {
	snap := m.Snapshot()
	return []statekit.PrometheusSample{
		{Name: "http_server_requests_total", Value: float64(m.totalRequests.Load())},
		{Name: "http_server_errors_total", Value: float64(m.totalErrors.Load())},
		{Name: "http_server_requests_per_second", Value: snap.RequestsPerSecond},
		{Name: "http_server_errors_per_second", Value: snap.ErrorsPerSecond},
		{Name: "http_server_average_latency_seconds", Value: snap.AverageLatency.Seconds()},
	}
}

func (m *HTTPMetrics) observe(status int, duration time.Duration) {
	m.totalRequests.Add(1)
	if status >= http.StatusInternalServerError {
		m.totalErrors.Add(1)
	}

	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(now)
	m.observations = append(m.observations, httpObservation{at: now, status: status, duration: duration})
}

func (m *HTTPMetrics) pruneLocked(now time.Time) {
	cutoff := now.Add(-m.window)
	keep := 0
	for keep < len(m.observations) && m.observations[keep].at.Before(cutoff) {
		keep++
	}
	if keep == 0 {
		return
	}
	copy(m.observations, m.observations[keep:])
	m.observations = m.observations[:len(m.observations)-keep]
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
