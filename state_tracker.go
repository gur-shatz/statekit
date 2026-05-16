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
		changedAt:  t,
		history:    []HistoryEntry{{Timestamp: t, Status: Pass}},
		maxHistory: defaultMaxHistory,
		now:        now,
	}
}

func (t *stateTracker) set(status Status, reason string, data map[string]any) {
	if status == Pass {
		reason = ""
	}
	if status == t.status && reason == t.reason {
		return
	}
	now := t.now()
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
	}
	checks := make([]Snapshot, len(children))
	copy(checks, children)
	return Snapshot{
		Name:           t.name,
		Status:         t.status,
		Importance:     t.importance,
		Help:           t.help,
		Reason:         t.reason,
		Data:           t.data,
		ChangedAt:      t.changedAt,
		ChangedSecsAgo: int64(now.Sub(t.changedAt).Seconds()),
		History:        history,
		Checks:         checks,
	}
}
