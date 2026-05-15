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
	return &ManualState{tracker: newStateTracker(name, o.importance, o.now)}
}

func (s *ManualState) Name() string {
	return s.tracker.name
}

func (s *ManualState) Set(status Status, message string, data any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracker.set(status, message, data)
}

func (s *ManualState) Pass(message string, data any) {
	s.Set(Pass, message, data)
}

func (s *ManualState) Warn(message string, data any) {
	s.Set(Warn, message, data)
}

func (s *ManualState) Fail(message string, data any) {
	s.Set(Fail, message, data)
}

func (s *ManualState) Down(message string, data any) {
	s.Set(Down, message, data)
}

func (s *ManualState) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tracker.snapshot(nil)
}
