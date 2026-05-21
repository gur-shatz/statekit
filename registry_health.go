package statekit

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// healthState is the synthetic state automatically maintained by Registry.
// Its status is the worst of all registered states (Informational children
// are capped at Warn, matching aggregate semantics). Its data carries the
// distribution of statuses; its reason groups non-pass states by status.
//
// healthState is not added to Registry.states. Registry.Snapshot evaluates
// it from the snapshots of every other registered state and prepends it to
// the returned slice so it is the first state in every display.
type healthState struct {
	mu      sync.Mutex
	tracker stateTracker
}

func newHealthState(name string, now clock) *healthState {
	if now == nil {
		now = defaultClock
	}
	return &healthState{
		tracker: newStateTracker(name, Important, "Most severe status across registered states.", now),
	}
}

func (this *healthState) Name() string {
	return this.tracker.name
}

func (this *healthState) Snapshot() Snapshot {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.tracker.snapshot(nil)
}

func (this *healthState) update(snaps []Snapshot) Snapshot {
	this.mu.Lock()
	defer this.mu.Unlock()
	status, reason, data := evaluateHealth(snaps)
	this.tracker.set(status, reason, data)
	return this.tracker.snapshot(nil)
}

func evaluateHealth(snaps []Snapshot) (Status, string, map[string]any) {
	counts := map[Status]int{}
	byStatus := map[Status][]string{}
	worst := Pass
	for _, snap := range snaps {
		contribution := snap.Status
		if snap.Importance == Informational && contribution > Warn {
			contribution = Warn
		}
		counts[contribution]++
		if contribution > Pass {
			byStatus[contribution] = append(byStatus[contribution], snap.Name)
		}
		if contribution > worst {
			worst = contribution
		}
	}
	data := map[string]any{}
	for _, status := range []Status{Pass, Warn, Fail, Down} {
		if counts[status] > 0 {
			data[status.String()] = counts[status]
		}
	}
	if len(data) == 0 {
		data = nil
	}
	return worst, formatHealthReason(byStatus), data
}

func formatHealthReason(byStatus map[Status][]string) string {
	parts := make([]string, 0, 3)
	for _, status := range []Status{Down, Fail, Warn} {
		names := byStatus[status]
		if len(names) == 0 {
			continue
		}
		sort.Strings(names)
		parts = append(parts, fmt.Sprintf("%s:%s", status.String(), strings.Join(names, ",")))
	}
	return strings.Join(parts, " ")
}
