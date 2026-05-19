// Package promhelpers provides small helpers for assembling
// []statekit.PrometheusSample slices from common Go containers, so that
// PrometheusCollector implementations don't have to spell out the loop every
// time.
package promhelpers

import (
	"github.com/gur-shatz/statekit"
)

// Numeric is any value type that can be converted to a Prometheus sample value.
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// SamplesFromMap turns a map keyed by label value into one sample per entry,
// all carrying the given metric name and a single label (labelKey -> map key).
// An empty map yields a nil slice. Sample order is not defined.
func SamplesFromMap[V Numeric](name, labelKey string, m map[string]V) []statekit.PrometheusSample {
	if len(m) == 0 {
		return nil
	}
	samples := make([]statekit.PrometheusSample, 0, len(m))
	for k, v := range m {
		samples = append(samples, statekit.PrometheusSample{
			Name:   name,
			Labels: map[string]string{labelKey: k},
			Value:  float64(v),
		})
	}
	return samples
}

// SamplesFromMapWithTotal behaves like SamplesFromMap and additionally appends
// one unlabeled sample whose value is the sum of m, named totalName. If
// totalName is empty the total sample is omitted. An empty map still yields a
// single total sample with value 0 when totalName is set, so the total metric
// scrapes cleanly even on a cold start.
func SamplesFromMapWithTotal[V Numeric](name, labelKey, totalName string, m map[string]V) []statekit.PrometheusSample {
	var sum V
	samples := make([]statekit.PrometheusSample, 0, len(m)+1)
	for k, v := range m {
		samples = append(samples, statekit.PrometheusSample{
			Name:   name,
			Labels: map[string]string{labelKey: k},
			Value:  float64(v),
		})
		sum += v
	}
	if totalName != "" {
		samples = append(samples, statekit.PrometheusSample{
			Name:  totalName,
			Value: float64(sum),
		})
	}
	if len(samples) == 0 {
		return nil
	}
	return samples
}
