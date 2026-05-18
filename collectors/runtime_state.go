package collectors

import (
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

const defaultRuntimeTrendSampleInterval = time.Second

type RuntimeCheckState struct {
	Status statekit.Status
	Reason string
	Data   map[string]any
}

type RuntimeCurrentFunc func() RuntimeCheckState

type RuntimeCheck struct {
	inner   *statekit.ManualState
	current RuntimeCurrentFunc
}

func newRuntimeCheck(name string, current RuntimeCurrentFunc) *RuntimeCheck {
	if name == "" {
		name = "runtime check"
	}
	if current == nil {
		current = func() RuntimeCheckState {
			return RuntimeCheckState{Status: statekit.Pass, Data: map[string]any{}}
		}
	}
	return &RuntimeCheck{
		inner:   statekit.NewManualState(name),
		current: current,
	}
}

func (c *RuntimeCheck) Name() string {
	return c.inner.Name()
}

func (c *RuntimeCheck) Current() RuntimeCheckState {
	current := c.current()
	return RuntimeCheckState{
		Status: current.Status,
		Reason: current.Reason,
		Data:   maps.Clone(current.Data),
	}
}

func (c *RuntimeCheck) Snapshot() statekit.Snapshot {
	current := c.Current()
	c.inner.Set(current.Status, current.Reason, current.Data)
	return c.inner.Snapshot()
}

type RuntimeTrendOption func(*runtimeTrendConfig)

type runtimeTrendConfig struct {
	sampleInterval time.Duration
	now            func() time.Time
}

type runtimeTrendSample struct {
	at    time.Time
	value float64
}

type runtimeScalarSource interface {
	Value(name string) (float64, bool)
}

type runtimeTrendCheck struct {
	mu             sync.Mutex
	metrics        runtimeScalarSource
	metricName     string
	window         time.Duration
	minSamples     int
	sampleInterval time.Duration
	now            func() time.Time
	samples        []runtimeTrendSample
}

func NewRuntimeIncreasingTrendCheck(metrics *RuntimeMetrics, name, metricName string, window time.Duration, minSamples int, warnPerSecond, failPerSecond, downPerSecond float64, opts ...RuntimeTrendOption) *RuntimeCheck {
	return newRuntimeIncreasingTrendCheck(metrics, name, metricName, window, minSamples, warnPerSecond, failPerSecond, downPerSecond, opts...)
}

func newRuntimeIncreasingTrendCheck(metrics runtimeScalarSource, name, metricName string, window time.Duration, minSamples int, warnPerSecond, failPerSecond, downPerSecond float64, opts ...RuntimeTrendOption) *RuntimeCheck {
	cfg := runtimeTrendConfig{
		sampleInterval: defaultRuntimeTrendSampleInterval,
		now:            time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	check := &runtimeTrendCheck{
		metrics:        metrics,
		metricName:     metricName,
		window:         window,
		minSamples:     maxInt(2, minSamples),
		sampleInterval: cfg.sampleInterval,
		now:            cfg.now,
	}
	return newRuntimeCheck(name, func() RuntimeCheckState {
		return check.current(warnPerSecond, failPerSecond, downPerSecond)
	})
}

func WithRuntimeTrendSampleInterval(interval time.Duration) RuntimeTrendOption {
	return func(c *runtimeTrendConfig) {
		if interval >= 0 {
			c.sampleInterval = interval
		}
	}
}

func withRuntimeTrendClock(now func() time.Time) RuntimeTrendOption {
	return func(c *runtimeTrendConfig) {
		if now != nil {
			c.now = now
		}
	}
}

func (c *runtimeTrendCheck) current(warnPerSecond, failPerSecond, downPerSecond float64) RuntimeCheckState {
	if c.metrics == nil {
		return RuntimeCheckState{Status: statekit.Down, Reason: "missing runtime metrics", Data: map[string]any{}}
	}
	value, ok := c.metrics.Value(c.metricName)
	if !ok {
		return RuntimeCheckState{
			Status: statekit.Down,
			Reason: fmt.Sprintf("runtime metric %q is missing or not scalar", c.metricName),
			Data:   map[string]any{"metric": c.metricName},
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.addSampleLocked(now, value)
	c.pruneLocked(now)

	data := c.trendDataLocked()
	data["metric"] = c.metricName
	data["window"] = c.window.String()
	data["sample_interval"] = c.sampleInterval.String()

	if len(c.samples) < c.minSamples {
		data["min_samples"] = c.minSamples
		return RuntimeCheckState{Status: statekit.Pass, Data: data}
	}

	growthPerSecond, _ := data["growth_per_second"].(float64)
	status, threshold := thresholdStatus(growthPerSecond, warnPerSecond, failPerSecond, downPerSecond)
	if growthPerSecond <= 0 || status == statekit.Pass {
		return RuntimeCheckState{Status: statekit.Pass, Data: data}
	}
	return RuntimeCheckState{
		Status: status,
		Reason: fmt.Sprintf("%s growing by %.3f per second above %s threshold %.3f/s", c.metricName, growthPerSecond, status, threshold),
		Data:   data,
	}
}

func (c *runtimeTrendCheck) addSampleLocked(now time.Time, value float64) {
	if len(c.samples) > 0 && c.sampleInterval > 0 && now.Sub(c.samples[len(c.samples)-1].at) < c.sampleInterval {
		c.samples[len(c.samples)-1] = runtimeTrendSample{at: now, value: value}
		return
	}
	c.samples = append(c.samples, runtimeTrendSample{at: now, value: value})
}

func (c *runtimeTrendCheck) pruneLocked(now time.Time) {
	if c.window <= 0 {
		return
	}
	cutoff := now.Add(-c.window)
	keep := 0
	for keep < len(c.samples) && c.samples[keep].at.Before(cutoff) {
		keep++
	}
	if keep == 0 {
		return
	}
	copy(c.samples, c.samples[keep:])
	c.samples = c.samples[:len(c.samples)-keep]
}

func (c *runtimeTrendCheck) trendDataLocked() map[string]any {
	data := map[string]any{
		"samples": len(c.samples),
	}
	if len(c.samples) == 0 {
		return data
	}

	first := c.samples[0]
	last := c.samples[len(c.samples)-1]
	elapsed := last.at.Sub(first.at).Seconds()
	growth := last.value - first.value
	var growthPerSecond float64
	if elapsed > 0 {
		growthPerSecond = growth / elapsed
	}
	data["first_value"] = first.value
	data["current_value"] = last.value
	data["growth"] = growth
	data["elapsed_seconds"] = elapsed
	data["growth_per_second"] = growthPerSecond
	return data
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
