package statekit

import "sync"

// AggregatedState is a state that owns a set of leaf checks.
//
// Aggregates intentionally reject other AggregatedState values as children:
// compose checks into one aggregate layer, then register multiple aggregates at
// the registry level if the application wants separate groups.
type AggregatedState interface {
	State
	AddCheck(...State)
	AddInformationalCheck(...State)
	AddCheckWithWorstStatus(Status, ...State)
}

// AggregateState derives its status from child states. Children can be added
// progressively as subsystems initialize.
type AggregateState struct {
	mu       sync.RWMutex
	tracker  stateTracker
	children []aggregateChild
	WithMetrics
}

type aggregateChild struct {
	state       State
	worstStatus Status
}

func NewStateAggregator(name string, opts ...Option) *AggregateState {
	return new(AggregateState).Init(name, opts...)
}

func (s *AggregateState) Init(name string, opts ...Option) *AggregateState {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	s.tracker = newStateTracker(name, o.importance, o.help, o.now)
	s.children = nil
	s.WithMetrics = WithMetrics{}
	return s
}

func (s *AggregateState) Name() string {
	return s.tracker.name
}

func (s *AggregateState) Add(children ...State) {
	s.AddCheck(children...)
}

func (s *AggregateState) AddCheck(children ...State) {
	s.addWithWorstStatus(Down, children...)
}

// Deprecated: use AddCheck.
func (s *AggregateState) AddTest(children ...State) {
	s.AddCheck(children...)
}

func (s *AggregateState) AddInformational(children ...State) {
	s.AddInformationalCheck(children...)
}

func (s *AggregateState) AddInformationalCheck(children ...State) {
	s.AddCheckWithWorstStatus(Warn, children...)
}

// Deprecated: use AddInformationalCheck.
func (s *AggregateState) AddInformationalTest(children ...State) {
	s.AddInformationalCheck(children...)
}

func (s *AggregateState) AddWithWorstStatus(worstStatus Status, children ...State) {
	s.AddCheckWithWorstStatus(worstStatus, children...)
}

func (s *AggregateState) AddCheckWithWorstStatus(worstStatus Status, children ...State) {
	s.addWithWorstStatus(worstStatus, children...)
}

// Deprecated: use AddCheckWithWorstStatus.
func (s *AggregateState) AddTestWithWorstStatus(worstStatus Status, children ...State) {
	s.AddCheckWithWorstStatus(worstStatus, children...)
}

func (s *AggregateState) addWithWorstStatus(worstStatus Status, children ...State) {
	s.checkAddable(children)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range children {
		s.children = append(s.children, aggregateChild{state: child, worstStatus: worstStatus})
	}
}

// checkAddable panics if any child is nil or an aggregate. Aggregate children
// are programming errors: checks should be leaves so Snapshot cannot recurse
// back into an aggregate graph.
func (s *AggregateState) checkAddable(children []State) {
	for _, child := range children {
		if child == nil {
			panic("statekit: AggregateState.AddCheck called with nil child")
		}
		if isAggregatedState(child) {
			panic("statekit: AggregateState.AddCheck called with aggregate child: we don't allow recursive checks")
		}
	}
}

func isAggregatedState(state State) bool {
	_, ok := state.(AggregatedState)
	return ok
}

func (s *AggregateState) Snapshot() Snapshot {
	s.mu.RLock()
	children := append([]aggregateChild(nil), s.children...)
	s.mu.RUnlock()

	childSnaps := make([]Snapshot, 0, len(children))
	status := Pass
	reason := ""
	for _, child := range children {
		snap := child.state.Snapshot()
		childSnaps = append(childSnaps, snap)
		contribution := aggregateContribution(snap, child.worstStatus)
		if contribution > status {
			status = contribution
			reason = snap.Name
			if snap.Reason != "" {
				reason += ": " + snap.Reason
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracker.set(status, reason, nil)
	snap := s.tracker.snapshot(childSnaps)
	snap.Metrics = s.metrics()
	return snap
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
