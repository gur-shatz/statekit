package statekit

import (
	"fmt"
	"sync"
	"time"
)

// FailMap tracks named failures by name. Callers report items as ok with Pass
// (which removes them) or as failing with Fail (which adds or refreshes them).
// The state is Pass while the map is empty and Fail while any item is in the
// map. The reason carries the count of failing items; the data carries the
// map with per-item since-time and failure duration.
//
// Primary use case: walking a collection and flagging the bad items so the
// snapshot tells you both that there is corruption and where.
type FailMap struct {
	mu       sync.RWMutex
	tracker  stateTracker
	failures map[string]failMapEntry
	now      clock
	WithMetrics
}

type failMapEntry struct {
	since  time.Time
	reason string
	data   map[string]any
}

func NewFailMap(name string, opts ...Option) *FailMap {
	return new(FailMap).Init(name, opts...)
}

func (this *FailMap) Init(name string, opts ...Option) *FailMap {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.now == nil {
		o.now = defaultClock
	}
	this.tracker = newStateTracker(name, o.importance, o.help, o.now)
	this.failures = make(map[string]failMapEntry)
	this.now = o.now
	this.WithMetrics = WithMetrics{}
	return this
}

func (this *FailMap) Name() string {
	return this.tracker.name
}

// Pass removes the named item from the failure map. If the item was not in
// the map this is a no-op.
func (this *FailMap) Pass(name string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if _, ok := this.failures[name]; !ok {
		return
	}
	delete(this.failures, name)
	this.updateLocked()
}

// Fail adds the named item to the failure map. If the item is already in the
// map its since-time is preserved so the reported failure duration keeps
// growing; only reason and data are refreshed.
func (this *FailMap) Fail(name string, reason string, data map[string]any) {
	this.mu.Lock()
	defer this.mu.Unlock()
	entry, ok := this.failures[name]
	if !ok {
		entry.since = this.now()
	}
	entry.reason = reason
	entry.data = data
	this.failures[name] = entry
	this.updateLocked()
}

// Set passes or fails the named item depending on ok. Reason and data are
// ignored on the ok branch.
func (this *FailMap) Set(name string, ok bool, reason string, data map[string]any) {
	if ok {
		this.Pass(name)
	} else {
		this.Fail(name, reason, data)
	}
}

// Failing returns the current count of items in the failure map.
func (this *FailMap) Failing() int {
	this.mu.RLock()
	defer this.mu.RUnlock()
	return len(this.failures)
}

func (this *FailMap) Snapshot() Snapshot {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.updateLocked()
	snap := this.tracker.snapshot(nil)
	snap.Metrics = this.metrics()
	return snap
}

func (this *FailMap) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{
		{
			Name: "fail_map_count",
			Help: "Number of items currently in the failure map.",
			Type: PrometheusGauge,
		},
	}
}

func (this *FailMap) CollectPrometheus() []PrometheusSample {
	this.mu.RLock()
	count := len(this.failures)
	this.mu.RUnlock()
	return []PrometheusSample{
		{Name: "fail_map_count", Labels: map[string]string{"name": this.Name()}, Value: float64(count)},
	}
}

func (this *FailMap) updateLocked() {
	if len(this.failures) == 0 {
		this.tracker.set(Pass, "", nil)
		return
	}
	this.tracker.set(Fail, fmt.Sprintf("%d failing", len(this.failures)), this.dataLocked())
}

func (this *FailMap) dataLocked() map[string]any {
	now := this.now()
	items := make(map[string]any, len(this.failures))
	for name, entry := range this.failures {
		item := map[string]any{
			"since":           entry.since,
			"secs_in_failure": int64(now.Sub(entry.since).Seconds()),
		}
		if entry.reason != "" {
			item["reason"] = entry.reason
		}
		if entry.data != nil {
			item["data"] = entry.data
		}
		items[name] = item
	}
	return map[string]any{
		"count": len(items),
		"items": items,
	}
}
