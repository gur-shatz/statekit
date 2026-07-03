package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ChartStore is the separate timeseries component behind the historical
// charting endpoints. It records, per time bucket, which states were
// triggering (non-pass), so bucket size is proportional to how much is wrong,
// not to fleet size. A fixed window with round-robin overwrite gives a hard,
// precomputable bound, and the interface is the seam for a future disk or
// tsdb backend.
//
// Scope is "fleet" (or empty) for the whole fleet, or "target:{key}" for one
// target.
type ChartStore interface {
	Record(bucket time.Time, triggering []TriggeringState)
	Range(scope string, from, to time.Time, buckets int) ([]BucketCounts, error)
	Bucket(scope string, t time.Time) ([]TriggeringState, error)
}

// TriggeringState is one charting store entry. Label is the display name
// ("target:check"), captured at write time so reads need no join against the
// current layers: a state that was triggering an hour ago may no longer exist
// in L1/L2.
type TriggeringState struct {
	Identity  string `json:"identity"`
	TargetKey string `json:"target_key,omitempty"`
	Label     string `json:"label"`
	Status    string `json:"status"`
}

// BucketCounts is one element of the GET /state/timeline response.
type BucketCounts struct {
	T      time.Time      `json:"t"`
	Counts map[string]int `json:"counts"` // warn/fail/down only
}

const chartScopeFleet = "fleet"

// MemoryChartStore is the in-memory ChartStore backend and currently the only
// implementation; a file or tsdb backend would assert the same interface.
var _ ChartStore = (*MemoryChartStore)(nil)

// MemoryChartStore keeps a fixed ring of time buckets in memory. Writes for a
// bucket older than the window overwrite the slot round-robin, so memory is
// bounded by window x contemporaneous triggering states.
type MemoryChartStore struct {
	mu         sync.RWMutex
	bucketSize time.Duration
	buckets    []chartBucket
}

type chartBucket struct {
	t      time.Time
	states map[string]TriggeringState // by identity
}

// NewMemoryChartStore creates a chart store of window buckets of bucketSize
// each, e.g. NewMemoryChartStore(time.Minute, 24*60) for 24h at 1m
// resolution.
func NewMemoryChartStore(bucketSize time.Duration, window int) *MemoryChartStore {
	if bucketSize <= 0 {
		bucketSize = time.Minute
	}
	if window <= 0 {
		window = 24 * 60
	}
	return &MemoryChartStore{
		bucketSize: bucketSize,
		buckets:    make([]chartBucket, window),
	}
}

func (this *MemoryChartStore) Record(bucket time.Time, triggering []TriggeringState) {
	t := bucket.Truncate(this.bucketSize)
	this.mu.Lock()
	defer this.mu.Unlock()
	slot := &this.buckets[this.slotIndex(t)]
	if !slot.t.Equal(t) {
		slot.t = t
		slot.states = nil
	}
	if len(triggering) == 0 {
		return
	}
	if slot.states == nil {
		slot.states = make(map[string]TriggeringState, len(triggering))
	}
	for _, state := range triggering {
		slot.states[state.Identity] = state
	}
}

func (this *MemoryChartStore) Range(scope string, from, to time.Time, buckets int) ([]BucketCounts, error) {
	if buckets <= 0 {
		return nil, fmt.Errorf("buckets must be positive, got %d", buckets)
	}
	if !to.After(from) {
		return nil, fmt.Errorf("empty range: from %v to %v", from, to)
	}
	matches, err := scopeMatcher(scope)
	if err != nil {
		return nil, err
	}
	step := to.Sub(from) / time.Duration(buckets)
	if step <= 0 {
		step = time.Nanosecond
	}
	// One status per identity per display bucket, worst wins, so a state
	// spanning several native buckets is counted once.
	byBucket := make([]map[string]string, buckets)

	this.mu.RLock()
	for i := range this.buckets {
		slot := &this.buckets[i]
		if slot.t.IsZero() || slot.t.Before(from) || !slot.t.Before(to) {
			continue
		}
		index := int(slot.t.Sub(from) / step)
		if index >= buckets {
			index = buckets - 1
		}
		for identity, state := range slot.states {
			if !matches(state) {
				continue
			}
			if byBucket[index] == nil {
				byBucket[index] = map[string]string{}
			}
			if statusRank(state.Status) > statusRank(byBucket[index][identity]) {
				byBucket[index][identity] = state.Status
			}
		}
	}
	this.mu.RUnlock()

	out := make([]BucketCounts, buckets)
	for i := range out {
		out[i] = BucketCounts{T: from.Add(time.Duration(i) * step), Counts: map[string]int{}}
		for _, status := range byBucket[i] {
			out[i].Counts[status]++
		}
	}
	return out, nil
}

func (this *MemoryChartStore) Bucket(scope string, t time.Time) ([]TriggeringState, error) {
	matches, err := scopeMatcher(scope)
	if err != nil {
		return nil, err
	}
	bucketTime := t.Truncate(this.bucketSize)

	this.mu.RLock()
	slot := &this.buckets[this.slotIndex(bucketTime)]
	out := make([]TriggeringState, 0, len(slot.states))
	if slot.t.Equal(bucketTime) {
		for _, state := range slot.states {
			if matches(state) {
				out = append(out, state)
			}
		}
	}
	this.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Label != out[j].Label {
			return out[i].Label < out[j].Label
		}
		return out[i].Identity < out[j].Identity
	})
	return out, nil
}

func (this *MemoryChartStore) slotIndex(t time.Time) int {
	index := int((t.UnixNano() / int64(this.bucketSize)) % int64(len(this.buckets)))
	if index < 0 {
		index += len(this.buckets)
	}
	return index
}

func scopeMatcher(scope string) (func(TriggeringState) bool, error) {
	switch {
	case scope == "" || scope == chartScopeFleet:
		return func(TriggeringState) bool { return true }, nil
	case strings.HasPrefix(scope, "target:"):
		key := strings.TrimPrefix(scope, "target:")
		return func(state TriggeringState) bool { return state.TargetKey == key }, nil
	default:
		return nil, fmt.Errorf("unknown scope %q: want fleet or target:{key}", scope)
	}
}
