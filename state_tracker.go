package statekit

import "time"

const defaultMaxHistory = 10

type stateTracker struct {
	name       string
	importance Importance
	status     Status
	message    string
	data       any
	changedAt  time.Time
	history    []HistoryEntry
	maxHistory int
	now        clock
}

func newStateTracker(name string, importance Importance, now clock) stateTracker {
	if now == nil {
		now = defaultClock
	}
	t := now()
	return stateTracker{
		name:       name,
		importance: importance,
		status:     Pass,
		changedAt:  t,
		history:    []HistoryEntry{{Timestamp: t, Status: Pass}},
		maxHistory: defaultMaxHistory,
		now:        now,
	}
}

func (t *stateTracker) set(status Status, message string, data any) {
	if status == t.status && message == t.message {
		return
	}
	now := t.now()
	t.changedAt = now
	t.history = append(t.history, HistoryEntry{
		Timestamp: now,
		Status:    status,
		Message:   message,
		Data:      data,
	})
	if len(t.history) > t.maxHistory {
		t.history = t.history[len(t.history)-t.maxHistory:]
	}
	t.status = status
	t.message = message
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
		Name:            t.name,
		Status:          t.status,
		Importance:      t.importance,
		Message:         t.message,
		Data:            t.data,
		ChangedAt:       t.changedAt,
		TimeInStateSecs: int64(now.Sub(t.changedAt).Seconds()),
		History:         history,
		Checks:          checks,
	}
}
