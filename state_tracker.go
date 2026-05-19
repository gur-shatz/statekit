package statekit

import "time"

const defaultMaxHistory = 10

type stateTracker struct {
	name       string
	importance Importance
	help       string
	status     Status
	reason     string
	data       map[string]any
	updatedAt  time.Time
	changedAt  time.Time
	history    []HistoryEntry
	maxHistory int
	now        clock
}

func newStateTracker(name string, importance Importance, help string, now clock) stateTracker {
	if now == nil {
		now = defaultClock
	}
	t := now()
	return stateTracker{
		name:       name,
		importance: importance,
		help:       help,
		status:     Pass,
		updatedAt:  t,
		changedAt:  t,
		history:    []HistoryEntry{},
		maxHistory: defaultMaxHistory,
		now:        now,
	}
}

func (t *stateTracker) set(status Status, reason string, data map[string]any) {
	now := t.now()
	t.updatedAt = now
	if status == t.status && len(t.history) > 0 {
		t.reason = reason
		t.data = data
		return
	}
	t.changedAt = now
	t.history = append(t.history, HistoryEntry{
		Timestamp: now,
		Status:    status,
		Reason:    reason,
		Data:      data,
	})
	if len(t.history) > t.maxHistory {
		t.history = t.history[len(t.history)-t.maxHistory:]
	}
	t.status = status
	t.reason = reason
	t.data = data
}

func (t *stateTracker) snapshot(children []Snapshot) Snapshot {
	now := t.now()
	history := make([]HistoryEntry, len(t.history))
	copy(history, t.history)
	for i := range history {
		history[i].SecsAgo = int64(now.Sub(history[i].Timestamp).Seconds())
		history[i].Reason = history[i].Reason + ""
	}
	checks := make([]Snapshot, len(children))
	copy(checks, children)
	return Snapshot{
		Name:           t.name,
		Status:         t.status,
		Importance:     t.importance,
		Help:           t.help,
		Reason:         t.reason,
		UpdatedAt:      t.updatedAt,
		UpdatedSecsAgo: int64(now.Sub(t.updatedAt).Seconds()),
		Data:           t.data,
		ChangedAt:      t.changedAt,
		ChangedSecsAgo: int64(now.Sub(t.changedAt).Seconds()),
		History:        history,
		Checks:         checks,
	}
}
