package statekit

import (
	"sync"
	"time"
)

type FailRatioPolicy interface {
	Evaluate(FailRatioSnapshot) (Status, string, any)
}

type FailRatioSnapshot struct {
	Window    time.Duration `json:"window" yaml:"window"`
	Total     int           `json:"total" yaml:"total"`
	Failures  int           `json:"failures" yaml:"failures"`
	Passes    int           `json:"passes" yaml:"passes"`
	FailRatio float64       `json:"fail_ratio" yaml:"fail_ratio"`
}

type failRatioEvent struct {
	at     time.Time
	failed bool
}

// FailRatio tracks pass/fail outcomes over a sliding time window and evaluates
// them into state.
type FailRatio struct {
	mu      sync.RWMutex
	tracker stateTracker
	window  time.Duration
	policy  FailRatioPolicy
	events  []failRatioEvent
	now     clock
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
	return &FailRatio{
		tracker: newStateTracker(name, o.importance, o.help, o.now),
		window:  window,
		policy:  policy,
		now:     o.now,
	}
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
	f.events = append(f.events, failRatioEvent{at: f.now(), failed: failed})
	f.pruneLocked()
	f.updateLocked()
}

func (f *FailRatio) Snapshot() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneLocked()
	f.updateLocked()
	return f.tracker.snapshot(nil)
}

func (f *FailRatio) RatioSnapshot() FailRatioSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneLocked()
	return f.ratioSnapshotLocked()
}

func (f *FailRatio) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{
		{
			Name: "fail_ratio",
			Help: "Ratio of failed outcomes in the configured sliding window.",
			Type: PrometheusGauge,
		},
		{
			Name: "fail_ratio_total",
			Help: "Total outcomes in the configured sliding window.",
			Type: PrometheusGauge,
		},
		{
			Name: "fail_ratio_failures",
			Help: "Failed outcomes in the configured sliding window.",
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

func (f *FailRatio) pruneLocked() {
	if f.window <= 0 {
		return
	}
	cutoff := f.now().Add(-f.window)
	keep := 0
	for keep < len(f.events) && f.events[keep].at.Before(cutoff) {
		keep++
	}
	if keep > 0 {
		copy(f.events, f.events[keep:])
		f.events = f.events[:len(f.events)-keep]
	}
}

func (f *FailRatio) updateLocked() {
	ratio := f.ratioSnapshotLocked()
	status, message, data := f.policy.Evaluate(ratio)
	f.tracker.set(status, message, data)
}

func (f *FailRatio) ratioSnapshotLocked() FailRatioSnapshot {
	failures := 0
	for _, event := range f.events {
		if event.failed {
			failures++
		}
	}
	total := len(f.events)
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

// AllFailedPolicy marks the state bad when every sample in the window failed.
type AllFailedPolicy struct {
	MinSamples int
	BadStatus  Status
}

func AllFailed(minSamples int, badStatus Status) AllFailedPolicy {
	return AllFailedPolicy{MinSamples: minSamples, BadStatus: badStatus}
}

func (p AllFailedPolicy) Evaluate(s FailRatioSnapshot) (Status, string, any) {
	if p.MinSamples <= 0 {
		p.MinSamples = 1
	}
	if p.BadStatus == Pass {
		p.BadStatus = Fail
	}
	if s.Total >= p.MinSamples && s.Failures == s.Total {
		return p.BadStatus, "all outcomes failed", s
	}
	return Pass, "", s
}

// RatioPolicy marks the state warn/fail/down when the window's failure ratio
// crosses configured thresholds. Zero thresholds are ignored.
type RatioPolicy struct {
	MinSamples int
	WarnAt     float64
	FailAt     float64
	DownAt     float64
}

func (p RatioPolicy) Evaluate(s FailRatioSnapshot) (Status, string, any) {
	if p.MinSamples <= 0 {
		p.MinSamples = 1
	}
	if s.Total < p.MinSamples {
		return Pass, "", s
	}
	switch {
	case p.DownAt > 0 && s.FailRatio >= p.DownAt:
		return Down, "failure ratio crossed down threshold", s
	case p.FailAt > 0 && s.FailRatio >= p.FailAt:
		return Fail, "failure ratio crossed fail threshold", s
	case p.WarnAt > 0 && s.FailRatio >= p.WarnAt:
		return Warn, "failure ratio crossed warn threshold", s
	default:
		return Pass, "", s
	}
}
