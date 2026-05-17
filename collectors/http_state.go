package collectors

import (
	"fmt"
	"maps"
	"time"

	"github.com/gur-shatz/statekit"
)

type HTTPCheckState struct {
	Status statekit.Status
	Reason string
	Data   map[string]any
}

type HTTPCurrentFunc func() HTTPCheckState

type HTTPCheck struct {
	inner   *statekit.ManualState
	current HTTPCurrentFunc
}

func newHTTPCheck(name string, current HTTPCurrentFunc) *HTTPCheck {
	if name == "" {
		name = "http check"
	}
	if current == nil {
		current = func() HTTPCheckState {
			return HTTPCheckState{Status: statekit.Pass, Data: map[string]any{}}
		}
	}
	return &HTTPCheck{
		inner:   statekit.NewManualState(name),
		current: current,
	}
}

func (c *HTTPCheck) Name() string {
	return c.inner.Name()
}

func (c *HTTPCheck) Current() HTTPCheckState {
	current := c.current()
	return HTTPCheckState{
		Status: current.Status,
		Reason: current.Reason,
		Data:   maps.Clone(current.Data),
	}
}

func (c *HTTPCheck) Snapshot() statekit.Snapshot {
	current := c.Current()
	c.inner.Set(current.Status, current.Reason, current.Data)
	return c.inner.Snapshot()
}

func NewHTTPAverageLatencyCheck(metrics *HTTPMetrics, name string, warnAt, failAt, downAt time.Duration) *HTTPCheck {
	return newHTTPCheck(name, func() HTTPCheckState {
		snap, ok := httpSnapshot(metrics)
		if !ok {
			return missingHTTPMetricsState()
		}
		data := map[string]any{"average_latency_seconds": snap.AverageLatency.Seconds()}
		status, threshold := durationThresholdStatus(snap.AverageLatency, warnAt, failAt, downAt)
		if snap.Requests == 0 || status == statekit.Pass {
			return httpCheckState(snap, statekit.Pass, "", data)
		}
		return httpCheckState(snap, status, fmt.Sprintf("average latency %dms above %s threshold %dms", snap.AverageLatency.Milliseconds(), status, threshold.Milliseconds()), data)
	})
}

func NewHTTPErrorsPerSecondCheck(metrics *HTTPMetrics, name string, warnAt, failAt, downAt float64) *HTTPCheck {
	return newHTTPCheck(name, func() HTTPCheckState {
		snap, ok := httpSnapshot(metrics)
		if !ok {
			return missingHTTPMetricsState()
		}
		data := map[string]any{"errors_per_second": snap.ErrorsPerSecond}
		status, threshold := thresholdStatus(snap.ErrorsPerSecond, warnAt, failAt, downAt)
		if status == statekit.Pass {
			return httpCheckState(snap, statekit.Pass, "", data)
		}
		return httpCheckState(snap, status, fmt.Sprintf("%.3f errors per second above %s threshold %.3f/s", snap.ErrorsPerSecond, status, threshold), data)
	})
}

func NewHTTPErrorRatioCheck(metrics *HTTPMetrics, name string, minRequests uint64, warnAt, failAt, downAt float64) *HTTPCheck {
	minRequests = maxUint64(1, minRequests)
	return newHTTPCheck(name, func() HTTPCheckState {
		snap, ok := httpSnapshot(metrics)
		if !ok {
			return missingHTTPMetricsState()
		}
		ratio := snap.ErrorRatio()
		data := map[string]any{
			"error_ratio":  ratio,
			"min_requests": minRequests,
		}
		status, threshold := thresholdStatus(ratio, warnAt, failAt, downAt)
		if snap.Requests < minRequests || status == statekit.Pass {
			return httpCheckState(snap, statekit.Pass, "", data)
		}
		return httpCheckState(snap, status, fmt.Sprintf("error ratio %.3f above %s threshold %.3f", ratio, status, threshold), data)
	})
}

func NewHTTPErrorPercentageCheck(metrics *HTTPMetrics, name string, minRequests uint64, warnAt, failAt, downAt float64) *HTTPCheck {
	minRequests = maxUint64(1, minRequests)
	return newHTTPCheck(name, func() HTTPCheckState {
		snap, ok := httpSnapshot(metrics)
		if !ok {
			return missingHTTPMetricsState()
		}
		percentage := snap.ErrorPercentage()
		data := map[string]any{
			"error_percentage": percentage,
			"min_requests":     minRequests,
		}
		status, threshold := thresholdStatus(percentage, warnAt, failAt, downAt)
		if snap.Requests < minRequests || status == statekit.Pass {
			return httpCheckState(snap, statekit.Pass, "", data)
		}
		return httpCheckState(snap, status, fmt.Sprintf("error percentage %.2f%% above %s threshold %.2f%%", percentage, status, threshold), data)
	})
}

func NewHTTPErrorCountCheck(metrics *HTTPMetrics, name string, warnAt, failAt, downAt uint64) *HTTPCheck {
	return newHTTPCheck(name, func() HTTPCheckState {
		snap, ok := httpSnapshot(metrics)
		if !ok {
			return missingHTTPMetricsState()
		}
		data := map[string]any{"errors": snap.Errors}
		status, threshold := uintThresholdStatus(snap.Errors, warnAt, failAt, downAt)
		if status == statekit.Pass {
			return httpCheckState(snap, statekit.Pass, "", data)
		}
		return httpCheckState(snap, status, fmt.Sprintf("%d errors above %s threshold %d", snap.Errors, status, threshold), data)
	})
}

func httpSnapshot(metrics *HTTPMetrics) (HTTPMetricsSnapshot, bool) {
	if metrics == nil {
		return HTTPMetricsSnapshot{}, false
	}
	return metrics.Snapshot(), true
}

func missingHTTPMetricsState() HTTPCheckState {
	return HTTPCheckState{
		Status: statekit.Down,
		Reason: "missing HTTP metrics",
		Data:   map[string]any{},
	}
}

func httpCheckState(snap HTTPMetricsSnapshot, status statekit.Status, reason string, data map[string]any) HTTPCheckState {
	if data == nil {
		data = map[string]any{}
	}
	for k, v := range httpMetricsData(snap) {
		data[k] = v
	}
	return HTTPCheckState{Status: status, Reason: reason, Data: data}
}

func httpMetricsData(snap HTTPMetricsSnapshot) map[string]any {
	return map[string]any{
		"window":              snap.Window.String(),
		"requests":            snap.Requests,
		"requests_per_second": snap.RequestsPerSecond,
		"errors":              snap.Errors,
		"errors_per_second":   snap.ErrorsPerSecond,
		"error_ratio":         snap.ErrorRatio(),
		"error_percentage":    snap.ErrorPercentage(),
		"average_latency_ms":  snap.AverageLatency.Milliseconds(),
	}
}

func uintThresholdStatus(value, warnAt, failAt, downAt uint64) (statekit.Status, uint64) {
	switch {
	case downAt > 0 && value >= downAt:
		return statekit.Down, downAt
	case failAt > 0 && value >= failAt:
		return statekit.Fail, failAt
	case warnAt > 0 && value >= warnAt:
		return statekit.Warn, warnAt
	default:
		return statekit.Pass, 0
	}
}

func thresholdStatus(value, warnAt, failAt, downAt float64) (statekit.Status, float64) {
	switch {
	case downAt > 0 && value >= downAt:
		return statekit.Down, downAt
	case failAt > 0 && value >= failAt:
		return statekit.Fail, failAt
	case warnAt > 0 && value >= warnAt:
		return statekit.Warn, warnAt
	default:
		return statekit.Pass, 0
	}
}

func lowerThresholdStatus(value, warnBelow, failBelow, downBelow float64) (statekit.Status, float64) {
	switch {
	case downBelow > 0 && value <= downBelow:
		return statekit.Down, downBelow
	case failBelow > 0 && value <= failBelow:
		return statekit.Fail, failBelow
	case warnBelow > 0 && value <= warnBelow:
		return statekit.Warn, warnBelow
	default:
		return statekit.Pass, 0
	}
}

func durationThresholdStatus(value, warnAt, failAt, downAt time.Duration) (statekit.Status, time.Duration) {
	switch {
	case downAt > 0 && value >= downAt:
		return statekit.Down, downAt
	case failAt > 0 && value >= failAt:
		return statekit.Fail, failAt
	case warnAt > 0 && value >= warnAt:
		return statekit.Warn, warnAt
	default:
		return statekit.Pass, 0
	}
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
