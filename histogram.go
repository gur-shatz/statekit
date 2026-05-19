package statekit

import (
	"math"
	"sort"
	"sync"
)

type Histogram struct {
	mu      sync.RWMutex
	buckets map[string]*histogramBucket
	values  []float64
	total   float64
}

type histogramBucket struct {
	key   string
	value float64
	count uint64
}

type HistogramSnapshot struct {
	Total   float64           `json:"total" yaml:"total"`
	Count   uint64            `json:"count" yaml:"count"`
	Buckets []HistogramBucket `json:"buckets" yaml:"buckets"`
}

type HistogramBucket struct {
	Key        string  `json:"key" yaml:"key"`
	Value      float64 `json:"value" yaml:"value"`
	Count      uint64  `json:"count" yaml:"count"`
	Percentage float64 `json:"percentage" yaml:"percentage"`
}

func NewHistogram() *Histogram {
	return &Histogram{buckets: map[string]*histogramBucket{}}
}

func (h *Histogram) Add(key string, value float64) {
	if h == nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buckets == nil {
		h.buckets = map[string]*histogramBucket{}
	}
	bucket := h.buckets[key]
	if bucket == nil {
		bucket = &histogramBucket{key: key}
		h.buckets[key] = bucket
	}
	bucket.value += value
	bucket.count++
	h.values = append(h.values, value)
	h.total += value
}

func (h *Histogram) Snapshot() HistogramSnapshot {
	if h == nil {
		return HistogramSnapshot{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := HistogramSnapshot{
		Total: h.total,
		Count: uint64(len(h.values)),
	}
	out.Buckets = make([]HistogramBucket, 0, len(h.buckets))
	for _, bucket := range h.buckets {
		percentage := 0.0
		if h.total != 0 {
			percentage = bucket.value / h.total * 100
		}
		out.Buckets = append(out.Buckets, HistogramBucket{
			Key:        bucket.key,
			Value:      bucket.value,
			Count:      bucket.count,
			Percentage: percentage,
		})
	}
	sortHistogramBuckets(out.Buckets, true)
	return out
}

func (h *Histogram) Percentile(q float64) float64 {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	values := append([]float64(nil), h.values...)
	h.mu.RUnlock()
	return percentile(values, q)
}

func (s HistogramSnapshot) Top(n int) []HistogramBucket {
	buckets := append([]HistogramBucket(nil), s.Buckets...)
	sortHistogramBuckets(buckets, true)
	return firstHistogramBuckets(buckets, n)
}

func (s HistogramSnapshot) TopPercent(percent float64) []HistogramBucket {
	buckets := append([]HistogramBucket(nil), s.Buckets...)
	sortHistogramBuckets(buckets, true)
	return histogramBucketsUntilPercent(buckets, percent)
}

func (s HistogramSnapshot) Bottom(n int) []HistogramBucket {
	buckets := append([]HistogramBucket(nil), s.Buckets...)
	sortHistogramBuckets(buckets, false)
	return firstHistogramBuckets(buckets, n)
}

func sortHistogramBuckets(buckets []HistogramBucket, descending bool) {
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Value == buckets[j].Value {
			return buckets[i].Key < buckets[j].Key
		}
		if descending {
			return buckets[i].Value > buckets[j].Value
		}
		return buckets[i].Value < buckets[j].Value
	})
}

func firstHistogramBuckets(buckets []HistogramBucket, n int) []HistogramBucket {
	if n <= 0 {
		return nil
	}
	if n > len(buckets) {
		n = len(buckets)
	}
	return append([]HistogramBucket(nil), buckets[:n]...)
}

func histogramBucketsUntilPercent(buckets []HistogramBucket, percent float64) []HistogramBucket {
	if percent <= 0 {
		return nil
	}
	if percent > 100 {
		percent = 100
	}
	out := make([]HistogramBucket, 0, len(buckets))
	total := 0.0
	for _, bucket := range buckets {
		out = append(out, bucket)
		total += bucket.Percentage
		if total >= percent {
			break
		}
	}
	return out
}

func percentile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if q > 1 {
		q = q / 100
	}
	if q <= 0 {
		q = 0
	}
	if q >= 1 {
		q = 1
	}
	sort.Float64s(values)
	if len(values) == 1 {
		return values[0]
	}
	rank := q * float64(len(values)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return values[lower]
	}
	weight := rank - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}
