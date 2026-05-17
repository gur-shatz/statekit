package collectors

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/gur-shatz/statekit"
)

func histogramSamples(name string, labels map[string]string, buckets []float64, counts []uint64, sum float64) []statekit.PrometheusSample {
	total := uint64(0)
	samples := make([]statekit.PrometheusSample, 0, len(counts)+2)
	for i, count := range counts {
		total += count
		upper := math.Inf(1)
		if i < len(buckets) {
			upper = buckets[i]
		}
		bucketLabels := cloneLabels(labels)
		bucketLabels["le"] = prometheusFloat(upper)
		samples = append(samples, statekit.PrometheusSample{
			Name:   name + "_bucket",
			Labels: bucketLabels,
			Value:  float64(total),
		})
	}
	samples = append(samples,
		statekit.PrometheusSample{
			Name:   name + "_sum",
			Labels: cloneLabels(labels),
			Value:  sum,
		},
		statekit.PrometheusSample{
			Name:   name + "_count",
			Labels: cloneLabels(labels),
			Value:  float64(total),
		},
	)
	return samples
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

func prometheusFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

func prometheusMetricName(prefix, name string) string {
	var b strings.Builder
	b.WriteString(prefix)
	lastUnderscore := strings.HasSuffix(prefix, "_")
	for _, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
		if valid {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "statekit_metric"
	}
	if out[0] >= '0' && out[0] <= '9' {
		return fmt.Sprintf("m_%s", out)
	}
	return out
}
