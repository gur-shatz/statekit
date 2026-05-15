package statekit

import "sync"

// AggregateState derives its status from child states. Children can be added
// progressively as subsystems initialize.
type AggregateState struct {
	mu       sync.RWMutex
	tracker  stateTracker
	children []aggregateChild
}

type aggregateChild struct {
	state       State
	worstStatus Status
}

func NewStateAggregator(name string, opts ...Option) *AggregateState {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &AggregateState{tracker: newStateTracker(name, o.importance, o.now)}
}

func (s *AggregateState) Name() string {
	return s.tracker.name
}

func (s *AggregateState) Add(children ...State) {
	s.checkAddable(children)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range children {
		s.children = append(s.children, aggregateChild{state: child, worstStatus: Down})
	}
}

func (s *AggregateState) AddInformational(children ...State) {
	s.AddWithWorstStatus(Warn, children...)
}

func (s *AggregateState) AddWithWorstStatus(worstStatus Status, children ...State) {
	s.checkAddable(children)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range children {
		s.children = append(s.children, aggregateChild{state: child, worstStatus: worstStatus})
	}
}

// checkAddable panics if any child is nil or would introduce a cycle. Cycles
// are programming errors: a state tree must be a DAG so Snapshot() terminates.
func (s *AggregateState) checkAddable(children []State) {
	for _, child := range children {
		if child == nil {
			panic("statekit: AggregateState.Add called with nil child")
		}
		if child == State(s) {
			panic("statekit: AggregateState.Add would create a cycle (child is the parent)")
		}
		if agg, ok := child.(*AggregateState); ok && agg.containsState(s) {
			panic("statekit: AggregateState.Add would create a cycle (parent reachable from child)")
		}
	}
}

// containsState reports whether target is anywhere in this aggregate's subtree.
func (s *AggregateState) containsState(target State) bool {
	s.mu.RLock()
	children := append([]aggregateChild(nil), s.children...)
	s.mu.RUnlock()
	for _, c := range children {
		if c.state == target {
			return true
		}
		if agg, ok := c.state.(*AggregateState); ok && agg.containsState(target) {
			return true
		}
	}
	return false
}

func (s *AggregateState) Snapshot() Snapshot {
	s.mu.RLock()
	children := append([]aggregateChild(nil), s.children...)
	s.mu.RUnlock()

	childSnaps := make([]Snapshot, 0, len(children))
	status := Pass
	message := ""
	for _, child := range children {
		snap := child.state.Snapshot()
		childSnaps = append(childSnaps, snap)
		contribution := aggregateContribution(snap, child.worstStatus)
		if contribution > status {
			status = contribution
			message = snap.Name
			if snap.Message != "" {
				message += ": " + snap.Message
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracker.set(status, message, nil)
	return s.tracker.snapshot(childSnaps)
}

func aggregateContribution(s Snapshot, worstStatus Status) Status {
	contribution := s.Status
	if s.Importance == Informational && contribution > Warn {
		contribution = Warn
	}
	if contribution > worstStatus {
		contribution = worstStatus
	}
	return contribution
}
