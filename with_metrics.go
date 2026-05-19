package statekit

import "sync"

type WithMetrics struct {
	mu         sync.RWMutex
	collectors []PrometheusCollector
}

func (m *WithMetrics) AddMetric(collectors ...PrometheusCollector) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		m.collectors = append(m.collectors, collector)
	}
}

func (m *WithMetrics) Metrics() []PrometheusCollectorSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	collectors := append([]PrometheusCollector(nil), m.collectors...)
	m.mu.RUnlock()

	out := make([]PrometheusCollectorSnapshot, 0, len(collectors))
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		out = append(out, prometheusCollectorSnapshot(collector))
	}
	return out
}
