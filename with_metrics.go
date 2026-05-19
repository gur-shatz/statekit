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

func (m *WithMetrics) metrics() []collectorSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	collectors := append([]PrometheusCollector(nil), m.collectors...)
	m.mu.RUnlock()

	out := make([]collectorSnapshot, 0, len(collectors))
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		out = append(out, prometheusCollectorSnapshot(collector))
	}
	return out
}
