package statekit

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Counter struct {
	name string
	help string
	n    atomic.Uint64
}

func NewCounter(name, help string) *Counter {
	return new(Counter).Init(name, help)
}

func (c *Counter) Init(name, help string) *Counter {
	c.name = name
	c.help = help
	c.n.Store(0)
	return c
}

func (c *Counter) Add(v uint64) {
	c.n.Add(v)
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) Get() uint64 {
	return c.n.Load()
}

func (c *Counter) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{{Name: c.name, Help: c.help, Type: PrometheusCounter}}
}

func (c *Counter) CollectPrometheus() []PrometheusSample {
	return []PrometheusSample{{Name: c.name, Value: float64(c.Get())}}
}

type counterVecMetric struct {
	labels  map[string]string
	counter Counter
}

type CounterVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	labelNames []string
	counters   map[string]*counterVecMetric
}

func NewCounterVec(name, help string, labelNames ...string) *CounterVec {
	return new(CounterVec).Init(name, help, labelNames...)
}

func (v *CounterVec) Init(name, help string, labelNames ...string) *CounterVec {
	v.name = name
	v.help = help
	v.labelNames = append(v.labelNames[:0], labelNames...)
	v.counters = map[string]*counterVecMetric{}
	return v
}

func (v *CounterVec) WithLabelValues(labelValues ...string) *Counter {
	counter, err := v.GetMetricWithLabelValues(labelValues...)
	if err != nil {
		panic(err)
	}
	return counter
}

func (v *CounterVec) GetMetricWithLabelValues(labelValues ...string) (*Counter, error) {
	if len(labelValues) != len(v.labelNames) {
		return nil, fmt.Errorf("counter vec %q expected %d label values, got %d", v.name, len(v.labelNames), len(labelValues))
	}
	key := labelValuesKey(labelValues)

	v.mu.RLock()
	metric := v.counters[key]
	v.mu.RUnlock()
	if metric != nil {
		return &metric.counter, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if metric = v.counters[key]; metric != nil {
		return &metric.counter, nil
	}
	metric = &counterVecMetric{
		labels: labelsMap(v.labelNames, labelValues),
	}
	metric.counter.Init(v.name, v.help)
	v.counters[key] = metric
	return &metric.counter, nil
}

func (v *CounterVec) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{{
		Name:   v.name,
		Help:   v.help,
		Type:   PrometheusCounter,
		Labels: append([]string(nil), v.labelNames...),
	}}
}

func (v *CounterVec) CollectPrometheus() []PrometheusSample {
	v.mu.RLock()
	metrics := make([]*counterVecMetric, 0, len(v.counters))
	for _, metric := range v.counters {
		metrics = append(metrics, metric)
	}
	v.mu.RUnlock()

	samples := make([]PrometheusSample, 0, len(metrics))
	for _, metric := range metrics {
		samples = append(samples, PrometheusSample{
			Name:   v.name,
			Labels: metric.labels,
			Value:  float64(metric.counter.Get()),
		})
	}
	return samples
}
