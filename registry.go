package statekit

import (
	"fmt"
	"slices"
	"sync"
)

type Registry struct {
	mu          sync.RWMutex
	version     string
	labels      map[string]string
	labelOrder  []string
	states      []State
	collectors  []PrometheusCollector
	descs       map[string]PrometheusDesc
	health      *healthState
	escalations EscalationSource
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

// WithVersion attaches a version string to the registry. It surfaces as the
// top-level "version" field on the state display document so scrapers can
// see which build produced the report.
func WithVersion(version string) RegistryOption {
	return func(r *Registry) {
		r.version = version
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
	r.health = newHealthState("health", defaultClock)
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

	snaps := make([]Snapshot, 0, len(states)+1)
	for _, state := range states {
		if collection, ok := state.(StateCollection); ok {
			snaps = append(snaps, collection.Snapshots()...)
			continue
		}
		snaps = append(snaps, state.Snapshot())
	}
	healthSnap := r.health.update(snaps)
	return append([]Snapshot{healthSnap}, snaps...)
}

// SetHealthData records an external key/value pair that is merged into the
// synthetic "health" state's data map on every Snapshot. It is useful for
// surfacing process-wide facts (for example build/version information)
// alongside the health rollup. External values override the computed status
// counts (pass/warn/fail/down) when keys collide.
func (r *Registry) SetHealthData(key string, value any) {
	r.health.setData(key, value)
}

// Version returns the version configured via WithVersion. Empty if none was set.
func (r *Registry) Version() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
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
