package statekit

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Gauge struct {
	name string
	help string
	v    atomic.Int64
}

func NewGauge(name, help string) *Gauge {
	return new(Gauge).Init(name, help)
}

func (g *Gauge) Init(name, help string) *Gauge {
	g.name = name
	g.help = help
	g.v.Store(0)
	return g
}

func (g *Gauge) Set(v int64) {
	g.v.Store(v)
}

func (g *Gauge) Add(v int64) {
	g.v.Add(v)
}

func (g *Gauge) Get() int64 {
	return g.v.Load()
}

func (g *Gauge) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{{Name: g.name, Help: g.help, Type: PrometheusGauge}}
}

func (g *Gauge) CollectPrometheus() []PrometheusSample {
	return []PrometheusSample{{Name: g.name, Value: float64(g.Get())}}
}

type gaugeVecMetric struct {
	labels map[string]string
	gauge  Gauge
}

type GaugeVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	labelNames []string
	gauges     map[string]*gaugeVecMetric
}

func NewGaugeVec(name, help string, labelNames ...string) *GaugeVec {
	return new(GaugeVec).Init(name, help, labelNames...)
}

func (v *GaugeVec) Init(name, help string, labelNames ...string) *GaugeVec {
	v.name = name
	v.help = help
	v.labelNames = append(v.labelNames[:0], labelNames...)
	v.gauges = map[string]*gaugeVecMetric{}
	return v
}

func (v *GaugeVec) WithLabelValues(labelValues ...string) *Gauge {
	gauge, err := v.GetMetricWithLabelValues(labelValues...)
	if err != nil {
		panic(err)
	}
	return gauge
}

func (v *GaugeVec) GetMetricWithLabelValues(labelValues ...string) (*Gauge, error) {
	if len(labelValues) != len(v.labelNames) {
		return nil, fmt.Errorf("gauge vec %q expected %d label values, got %d", v.name, len(v.labelNames), len(labelValues))
	}
	key := labelValuesKey(labelValues)

	v.mu.RLock()
	metric := v.gauges[key]
	v.mu.RUnlock()
	if metric != nil {
		return &metric.gauge, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if metric = v.gauges[key]; metric != nil {
		return &metric.gauge, nil
	}
	metric = &gaugeVecMetric{
		labels: labelsMap(v.labelNames, labelValues),
	}
	metric.gauge.Init(v.name, v.help)
	v.gauges[key] = metric
	return &metric.gauge, nil
}

func (v *GaugeVec) DescribePrometheus() []PrometheusDesc {
	return []PrometheusDesc{{
		Name:   v.name,
		Help:   v.help,
		Type:   PrometheusGauge,
		Labels: append([]string(nil), v.labelNames...),
	}}
}

func (v *GaugeVec) CollectPrometheus() []PrometheusSample {
	v.mu.RLock()
	metrics := make([]*gaugeVecMetric, 0, len(v.gauges))
	for _, metric := range v.gauges {
		metrics = append(metrics, metric)
	}
	v.mu.RUnlock()

	samples := make([]PrometheusSample, 0, len(metrics))
	for _, metric := range metrics {
		samples = append(samples, PrometheusSample{
			Name:   v.name,
			Labels: metric.labels,
			Value:  float64(metric.gauge.Get()),
		})
	}
	return samples
}
