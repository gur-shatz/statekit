// Package promhelpers provides small helpers for assembling
// []statekit.PrometheusSample slices from common Go containers, so that
// PrometheusCollector implementations don't have to spell out the loop every
// time.
package promhelpers

import (
	"fmt"

	"github.com/gur-shatz/statekit"
)

// Numeric is any value type that can be converted to a Prometheus sample value.
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// SamplesFromMap turns a map keyed by label value into one sample per entry,
// all carrying the given metric name and a single label (labelKey -> map key),
// and appends one unlabeled "<name>_total" sample whose value is the sum of m.
// An empty map yields a nil slice. Sample order is not defined.
func SamplesFromMap[V Numeric](name, labelKey string, m map[string]V) []statekit.PrometheusSample {
	if len(m) == 0 {
		return nil
	}
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
	samples = append(samples, statekit.PrometheusSample{
		Name:  fmt.Sprintf("%s_total", name),
		Value: float64(sum),
	})
	return samples
}
