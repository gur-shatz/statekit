package statekit

import "sync"

// ManualState is a state object whose status is explicitly set by the caller.
type ManualState struct {
	mu      sync.RWMutex
	tracker stateTracker
}

func NewManualState(name string, opts ...Option) *ManualState {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &ManualState{tracker: newStateTracker(name, o.importance, o.help, o.now)}
}

func (s *ManualState) Name() string {
	return s.tracker.name
}

func (s *ManualState) Set(status Status, reason string, data map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracker.set(status, reason, data)
}

func (s *ManualState) Pass(reason string, data map[string]any) {
	s.Set(Pass, reason, data)
}

func (s *ManualState) Warn(reason string, data map[string]any) {
	s.Set(Warn, reason, data)
}

func (s *ManualState) Fail(reason string, data map[string]any) {
	s.Set(Fail, reason, data)
}

func (s *ManualState) Down(reason string, data map[string]any) {
	s.Set(Down, reason, data)
}

func (s *ManualState) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tracker.snapshot(nil)
}
