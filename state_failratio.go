package statekit

import (
	"sync"
	"time"

	counters "github.com/gur-shatz/statekit/util"
)

type FailRatioPolicy interface {
	Evaluate(FailRatioSnapshot) (Status, string, map[string]any)
}

type FailRatioSnapshot struct {
	Window    time.Duration `json:"window" yaml:"window"`
	Total     int           `json:"total" yaml:"total"`
	Failures  int           `json:"failures" yaml:"failures"`
	Passes    int           `json:"passes" yaml:"passes"`
	FailRatio float64       `json:"fail_ratio" yaml:"fail_ratio"`
}

const (
	failRatioCounterTotal = iota
	failRatioCounterFailures
	failRatioCounterWidth
)

// FailRatio tracks pass/fail outcomes over an epoch-weighted estimated window
// and evaluates them into state.
type FailRatio struct {
	mu                 sync.RWMutex
	tracker            stateTracker
	window             time.Duration
	policy             FailRatioPolicy
	counters           *counters.EpochWeightedCounters
	cumulativeTotal    int64
	cumulativeFailures int64
	now                clock
}

func NewFailRatio(name string, window time.Duration, policy FailRatioPolicy, opts ...Option) *FailRatio {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.now == nil {
		o.now = defaultClock
	}
	if policy == nil {
		policy = RatioPolicy{FailAt: 1, MinSamples: 1}
	}
	f := &FailRatio{
		tracker: newStateTracker(name, o.importance, o.help, o.now),
		window:  window,
		policy:  policy,
		now:     o.now,
	}
	if window > 0 {
		now := o.now()
		f.counters = counters.NewEpochWeightedCounters(failRatioSpan(window), failRatioTimestamp(now), failRatioCounterWidth)
	}
	return f
}

func (f *FailRatio) Name() string {
	return f.tracker.name
}

func (f *FailRatio) Pass() {
	f.record(false)
}

func (f *FailRatio) Fail() {
	f.record(true)
}

func (f *FailRatio) record(failed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	if f.window <= 0 {
		f.cumulativeTotal++
		if failed {
			f.cumulativeFailures++
		}
	} else {
		values := [failRatioCounterWidth]int64{failRatioCounterTotal: 1}
		if failed {
			values[failRatioCounterFailures] = 1
		}
		f.counters.Add(failRatioTimestamp(now), values[:])
	}
	f.updateLocked(now)
}

func (f *FailRatio) Snapshot() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateLocked(f.now())
	return f.tracker.snapshot(nil)
}

func (f *FailRatio) RatioSnapshot() FailRatioSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ratioSnapshotLocked(f.now())
}

func (f *FailRatio) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{
		{
			Name: "fail_ratio",
			Help: "Ratio of failed outcomes in the configured estimated window.",
			Type: PrometheusGauge,
		},
		{
			Name: "fail_ratio_total",
			Help: "Total outcomes in the configured estimated window.",
			Type: PrometheusGauge,
		},
		{
			Name: "fail_ratio_failures",
			Help: "Failed outcomes in the configured estimated window.",
			Type: PrometheusGauge,
		},
	}
}

func (f *FailRatio) CollectPrometheus() []PrometheusSample {
	snap := f.RatioSnapshot()
	labels := map[string]string{"name": f.Name()}
	return []PrometheusSample{
		{Name: "fail_ratio", Labels: labels, Value: snap.FailRatio},
		{Name: "fail_ratio_total", Labels: labels, Value: float64(snap.Total)},
		{Name: "fail_ratio_failures", Labels: labels, Value: float64(snap.Failures)},
	}
}

func (f *FailRatio) updateLocked(now time.Time) {
	ratio := f.ratioSnapshotLocked(now)
	status, reason, data := f.policy.Evaluate(ratio)
	f.tracker.set(status, reason, data)
}

func (f *FailRatio) ratioSnapshotLocked(now time.Time) FailRatioSnapshot {
	total, failures := f.ratioCountsLocked(now)
	var ratio float64
	if total > 0 {
		ratio = float64(failures) / float64(total)
	}
	return FailRatioSnapshot{
		Window:    f.window,
		Total:     total,
		Failures:  failures,
		Passes:    total - failures,
		FailRatio: ratio,
	}
}

func (f *FailRatio) ratioCountsLocked(now time.Time) (int, int) {
	if f.window <= 0 {
		return int(f.cumulativeTotal), int(f.cumulativeFailures)
	}
	if f.counters == nil {
		f.counters = counters.NewEpochWeightedCounters(failRatioSpan(f.window), failRatioTimestamp(now), failRatioCounterWidth)
	}

	values := make([]int64, failRatioCounterWidth)
	f.counters.CurrentWindowValue(failRatioTimestamp(now), values)
	total := int(max(values[failRatioCounterTotal], 0))
	failures := int(max(values[failRatioCounterFailures], 0))
	if failures > total {
		failures = total
	}
	return total, failures
}

// AllFailedPolicy marks the state bad when every estimated outcome in the
// window failed.
type AllFailedPolicy struct {
	MinSamples int
	BadStatus  Status
}

func AllFailed(minSamples int, badStatus Status) AllFailedPolicy {
	return AllFailedPolicy{MinSamples: minSamples, BadStatus: badStatus}
}

func (p AllFailedPolicy) Evaluate(s FailRatioSnapshot) (Status, string, map[string]any) {
	if p.MinSamples <= 0 {
		p.MinSamples = 1
	}
	if p.BadStatus == Pass {
		p.BadStatus = Fail
	}
	if s.Total >= p.MinSamples && s.Failures == s.Total {
		return p.BadStatus, "all outcomes failed", failRatioData(s)
	}
	return Pass, "", failRatioData(s)
}

// RatioPolicy marks the state warn/fail/down when the window's failure ratio
// crosses configured thresholds. Zero thresholds are ignored.
type RatioPolicy struct {
	MinSamples int
	WarnAt     float64
	FailAt     float64
	DownAt     float64
}

func (p RatioPolicy) Evaluate(s FailRatioSnapshot) (Status, string, map[string]any) {
	data := failRatioData(s)
	if p.MinSamples <= 0 {
		p.MinSamples = 1
	}
	if s.Total < p.MinSamples {
		return Pass, "", data
	}
	switch {
	case p.DownAt > 0 && s.FailRatio >= p.DownAt:
		return Down, "failure ratio crossed down threshold", data
	case p.FailAt > 0 && s.FailRatio >= p.FailAt:
		return Fail, "failure ratio crossed fail threshold", data
	case p.WarnAt > 0 && s.FailRatio >= p.WarnAt:
		return Warn, "failure ratio crossed warn threshold", data
	default:
		return Pass, "", data
	}
}

func failRatioData(s FailRatioSnapshot) map[string]any {
	return map[string]any{
		"window":     s.Window.String(),
		"total":      s.Total,
		"failures":   s.Failures,
		"passes":     s.Passes,
		"fail_ratio": s.FailRatio,
	}
}

func failRatioSpan(window time.Duration) uint32 {
	seconds := uint32(window / time.Second)
	if seconds == 0 {
		return 1
	}
	return seconds
}

func failRatioTimestamp(t time.Time) uint32 {
	return uint32(t.Unix())
}
