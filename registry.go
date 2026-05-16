package statekit

import (
	"fmt"
	"slices"
	"sync"
)

type Registry struct {
	mu         sync.RWMutex
	labels     map[string]string
	labelOrder []string
	states     []State
	collectors []PrometheusCollector
	descs      map[string]PrometheusDesc
}

type RegistryOption func(*Registry)

func WithLabel(name, value string) RegistryOption {
	return func(r *Registry) {
		if _, exists := r.labels[name]; !exists {
			r.labelOrder = append(r.labelOrder, name)
		}
		r.labels[name] = value
	}
}

func NewRegistry(opts ...RegistryOption) *Registry {
	r := &Registry{
		labels: map[string]string{},
		descs:  map[string]PrometheusDesc{},
	}
	for _, opt := range opts {
		opt(r)
	}
	r.rememberDesc(PrometheusDesc{
		Name: "state_level",
		Help: "Current state level: pass=1, warn=2, fail=3, down=4.",
		Type: PrometheusGauge,
	})
	r.rememberDesc(PrometheusDesc{
		Name: "state_time_in_state_seconds",
		Help: "Seconds since the state last changed status or reason.",
		Type: PrometheusGauge,
	})
	return r
}

func (r *Registry) Register(s State) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, s)
	if c, ok := s.(PrometheusCollector); ok {
		r.collectors = append(r.collectors, c)
		for _, desc := range c.DescribePrometheus() {
			if err := r.rememberDesc(desc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Registry) RegisterCollectors(collectors ...PrometheusCollector) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range collectors {
		r.collectors = append(r.collectors, c)
		for _, desc := range c.DescribePrometheus() {
			if err := r.rememberDesc(desc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Registry) Snapshot() []Snapshot {
	r.mu.RLock()
	states := append([]State(nil), r.states...)
	r.mu.RUnlock()

	snaps := make([]Snapshot, 0, len(states))
	for _, state := range states {
		snaps = append(snaps, state.Snapshot())
	}
	return snaps
}

func (r *Registry) rememberDesc(desc PrometheusDesc) error {
	if existing, ok := r.descs[desc.Name]; ok {
		if existing.Help != desc.Help || existing.Type != desc.Type || !slices.Equal(existing.Labels, desc.Labels) {
			return fmt.Errorf("conflicting prometheus descriptor %q", desc.Name)
		}
	}
	r.descs[desc.Name] = desc
	return nil
}
