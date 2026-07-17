package storage

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

const (
	defaultMetricsRetention = 30 * time.Minute
	defaultMetricSeriesCap  = 100
)

// MetricsStore is the bounded timeseries store used by the metrics API.
// Samples are keyed by scrape_path; callers normally pass samples for one key
// at a time. Implementations may discard samples to enforce their bounds.
type MetricsStore interface {
	IngestMetrics(key string, descs []statekit.PrometheusDesc, samples []statekit.PrometheusSample, observedAt time.Time)
	Metrics(key string, from, to time.Time) (MetricsDocument, error)
}

// MetricsStoreStats describes the live, retained contents of a metrics store.
// EstimatedBytes includes point slice capacity and approximate Go map/string
// overhead; it is intended for operational monitoring, not heap accounting.
type MetricsStoreStats struct {
	Keys           int64
	Metrics        int64
	Series         int64
	Points         int64
	Labels         int64
	Strings        int64
	EstimatedBytes uint64
}

type MetricsStatsProvider interface {
	MetricsStats() MetricsStoreStats
}

// MetricsDocument is the result returned for one keyed item.
type MetricsDocument struct {
	Key     string         `json:"key"`
	From    int64          `json:"from"`
	To      int64          `json:"to"`
	Metrics []MetricSeries `json:"metrics"`
}

type MetricSeries struct {
	Name   string              `json:"name"`
	Help   string              `json:"help,omitempty"`
	Type   string              `json:"type,omitempty"`
	Unit   string              `json:"unit,omitempty"`
	Series []MeasurementSeries `json:"series"`
}

// MeasurementSeries keeps timestamps separate from measurements. Both arrays
// have the same length and timestamps are Unix seconds.
type MeasurementSeries struct {
	Labels     map[string]string `json:"labels,omitempty"`
	Timestamps []int64           `json:"timestamps"`
	Values     []float64         `json:"values"`
	Constant   bool              `json:"constant"`
}

// MemoryMetricsStore interns all repeated strings and stores label sets as
// integer postings. Time and value columns are compact, parallel slices.
type MemoryMetricsStore struct {
	mu          sync.RWMutex
	retention   time.Duration
	seriesCap   int
	dict        stringDictionary
	keys        map[uint32]map[uint32]map[string]*encodedSeries
	descs       map[uint32]encodedDesc
	lastCompact int64
}

type stringDictionary struct {
	ids    map[string]uint32
	values []string
}

type encodedDesc struct {
	help uint32
	typ  uint32
	unit uint32
}

type encodedSeries struct {
	// postings is [label-name-id, label-value-id, ...], sorted by name ID.
	postings      []uint32
	timestamps    []int64
	values        []float64
	constantValue float64
	constantCount int
}

// NewMemoryMetricsStore creates a metrics store. Non-positive arguments use
// the defaults: 30 minutes of history and 100 label sets per metric and key.
func NewMemoryMetricsStore(retention time.Duration, seriesCap int) *MemoryMetricsStore {
	if retention <= 0 {
		retention = defaultMetricsRetention
	}
	if seriesCap <= 0 {
		seriesCap = defaultMetricSeriesCap
	}
	return &MemoryMetricsStore{
		retention: retention,
		seriesCap: seriesCap,
		dict:      stringDictionary{ids: map[string]uint32{}, values: []string{""}},
		keys:      map[uint32]map[uint32]map[string]*encodedSeries{},
		descs:     map[uint32]encodedDesc{},
	}
}

func (this *MemoryMetricsStore) IngestMetrics(key string, descs []statekit.PrometheusDesc, samples []statekit.PrometheusSample, observedAt time.Time) {
	if key == "" {
		return
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	ts := observedAt.Unix()

	this.mu.Lock()
	defer this.mu.Unlock()

	// Enforce the cardinality cap on each observation, retaining the largest
	// values. Stable tie-breaking makes the retained set deterministic.
	byMetric := map[string][]statekit.PrometheusSample{}
	for _, sample := range samples {
		if sample.Name != "" && !math.IsNaN(sample.Value) && !math.IsInf(sample.Value, 0) {
			byMetric[sample.Name] = append(byMetric[sample.Name], sample)
		}
	}
	if len(byMetric) == 0 {
		return
	}
	keyID := this.dict.intern(key)
	removed := false
	for name, metricSamples := range byMetric {
		sort.SliceStable(metricSamples, func(i, j int) bool {
			if metricSamples[i].Value != metricSamples[j].Value {
				return metricSamples[i].Value > metricSamples[j].Value
			}
			return canonicalLabels(metricSamples[i].Labels) < canonicalLabels(metricSamples[j].Labels)
		})
		if len(metricSamples) > this.seriesCap {
			metricSamples = metricSamples[:this.seriesCap]
		}
		metricID := this.dict.intern(name)
		if desc, ok := metricDescriptor(name, descs); ok {
			this.descs[metricID] = encodedDesc{
				help: this.dict.intern(desc.Help),
				typ:  this.dict.intern(string(desc.Type)),
				unit: this.dict.intern(desc.Unit),
			}
		}
		if this.keys[keyID] == nil {
			this.keys[keyID] = map[uint32]map[string]*encodedSeries{}
		}
		if this.keys[keyID][metricID] == nil {
			this.keys[keyID][metricID] = map[string]*encodedSeries{}
		}
		for _, sample := range metricSamples {
			postings, signature := this.encodeLabels(sample.Labels)
			series := this.keys[keyID][metricID][signature]
			if series == nil {
				series = &encodedSeries{postings: postings}
				this.keys[keyID][metricID][signature] = series
			}
			n := len(series.timestamps)
			if n > 0 {
				lastTimestamp := series.timestamps[n-1]
				if ts < lastTimestamp {
					continue
				}
				if ts == lastTimestamp {
					if series.values[n-1] == sample.Value {
						continue
					}
					series.values[n-1] = sample.Value
					updateConstantRun(series)
					continue
				}
			}
			series.timestamps = append(series.timestamps, ts)
			series.values = append(series.values, sample.Value)
			if n > 0 && series.constantValue == sample.Value {
				series.constantCount++
			} else {
				series.constantValue = sample.Value
				series.constantCount = 1
			}
		}
		removed = this.enforceSeriesCapLocked(this.keys[keyID][metricID]) || removed
	}
	removed = this.trimLocked(ts-int64(this.retention/time.Second)) || removed
	if removed && (this.lastCompact == 0 || ts-this.lastCompact >= 60) {
		this.compactDictionaryLocked()
		this.lastCompact = ts
	}
}

func (this *MemoryMetricsStore) Metrics(key string, from, to time.Time) (MetricsDocument, error) {
	if key == "" {
		return MetricsDocument{}, fmt.Errorf("metrics key is empty")
	}
	if to.IsZero() {
		to = time.Now()
	}
	if from.IsZero() {
		from = to.Add(-this.retention)
	}
	if !to.After(from) {
		return MetricsDocument{}, fmt.Errorf("empty metrics range")
	}
	out := MetricsDocument{Key: key, From: from.Unix(), To: to.Unix(), Metrics: []MetricSeries{}}

	this.mu.Lock()
	defer this.mu.Unlock()
	queryNow := to.Unix()
	if this.trimLocked(queryNow - int64(this.retention/time.Second)) {
		this.compactDictionaryLocked()
		this.lastCompact = queryNow
	}
	keyID, ok := this.dict.ids[key]
	if !ok {
		return out, nil
	}
	for metricID, encoded := range this.keys[keyID] {
		desc := this.descs[metricID]
		metric := MetricSeries{
			Name:   this.dict.lookup(metricID),
			Help:   this.dict.lookup(desc.help),
			Type:   this.dict.lookup(desc.typ),
			Unit:   this.dict.lookup(desc.unit),
			Series: []MeasurementSeries{},
		}
		for _, series := range encoded {
			start := sort.Search(len(series.timestamps), func(i int) bool { return series.timestamps[i] >= from.Unix() })
			end := sort.Search(len(series.timestamps), func(i int) bool { return series.timestamps[i] > to.Unix() })
			if start == end {
				continue
			}
			values := append([]float64(nil), series.values[start:end]...)
			metric.Series = append(metric.Series, MeasurementSeries{
				Labels:     this.decodeLabels(series.postings),
				Timestamps: append([]int64(nil), series.timestamps[start:end]...),
				Values:     values,
				Constant:   constantRunEndingAt(series, end) >= end-start,
			})
		}
		if len(metric.Series) == 0 {
			continue
		}
		sort.Slice(metric.Series, func(i, j int) bool {
			return canonicalLabels(metric.Series[i].Labels) < canonicalLabels(metric.Series[j].Labels)
		})
		out.Metrics = append(out.Metrics, metric)
	}
	sort.Slice(out.Metrics, func(i, j int) bool { return out.Metrics[i].Name < out.Metrics[j].Name })
	return out, nil
}

func (this *MemoryMetricsStore) MetricsStats() MetricsStoreStats {
	this.mu.RLock()
	defer this.mu.RUnlock()
	stats := MetricsStoreStats{
		Keys:           int64(len(this.keys)),
		Strings:        int64(len(this.dict.values) - 1),
		EstimatedBytes: uint64(cap(this.dict.values))*16 + uint64(len(this.dict.ids))*40,
	}
	for _, value := range this.dict.values {
		stats.EstimatedBytes += uint64(len(value))
	}
	for _, metrics := range this.keys {
		stats.Metrics += int64(len(metrics))
		stats.EstimatedBytes += 64 + uint64(len(metrics))*32
		for _, seriesMap := range metrics {
			stats.Series += int64(len(seriesMap))
			stats.EstimatedBytes += 64 + uint64(len(seriesMap))*48
			for signature, series := range seriesMap {
				stats.Points += int64(len(series.timestamps))
				stats.Labels += int64(len(series.postings) / 2)
				stats.EstimatedBytes += 96 + uint64(len(signature))
				stats.EstimatedBytes += uint64(cap(series.postings)) * 4
				stats.EstimatedBytes += uint64(cap(series.timestamps)) * 8
				stats.EstimatedBytes += uint64(cap(series.values)) * 8
			}
		}
	}
	stats.EstimatedBytes += uint64(len(this.descs)) * 48
	return stats
}

func (this *MemoryMetricsStore) enforceSeriesCapLocked(seriesMap map[string]*encodedSeries) bool {
	if len(seriesMap) <= this.seriesCap {
		return false
	}
	type rankedSeries struct {
		signature string
		value     float64
	}
	ranked := make([]rankedSeries, 0, len(seriesMap))
	for signature, series := range seriesMap {
		ranked = append(ranked, rankedSeries{signature: signature, value: series.values[len(series.values)-1]})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].value != ranked[j].value {
			return ranked[i].value > ranked[j].value
		}
		return ranked[i].signature < ranked[j].signature
	})
	for _, item := range ranked[this.seriesCap:] {
		delete(seriesMap, item.signature)
	}
	return true
}

func (this *MemoryMetricsStore) trimLocked(cutoff int64) bool {
	removed := false
	for keyID, metrics := range this.keys {
		for metricID, seriesMap := range metrics {
			for signature, series := range seriesMap {
				first := sort.Search(len(series.timestamps), func(i int) bool { return series.timestamps[i] >= cutoff })
				if first > 0 {
					series.timestamps = append(series.timestamps[:0], series.timestamps[first:]...)
					series.values = append(series.values[:0], series.values[first:]...)
					if series.constantCount > len(series.values) {
						series.constantCount = len(series.values)
					}
				}
				if len(series.timestamps) == 0 {
					delete(seriesMap, signature)
					removed = true
				}
			}
			if len(seriesMap) == 0 {
				delete(metrics, metricID)
				removed = true
			}
		}
		if len(metrics) == 0 {
			delete(this.keys, keyID)
			removed = true
		}
	}
	return removed
}

// compactDictionaryLocked releases strings belonging only to expired or
// cardinality-evicted series. It runs at most once a minute.
func (this *MemoryMetricsStore) compactDictionaryLocked() {
	oldDict := this.dict
	oldDescs := this.descs
	newDict := stringDictionary{ids: map[string]uint32{}, values: []string{""}}
	newKeys := map[uint32]map[uint32]map[string]*encodedSeries{}
	newDescs := map[uint32]encodedDesc{}
	for oldKeyID, metrics := range this.keys {
		keyID := newDict.intern(oldDict.lookup(oldKeyID))
		newKeys[keyID] = map[uint32]map[string]*encodedSeries{}
		for oldMetricID, seriesMap := range metrics {
			metricID := newDict.intern(oldDict.lookup(oldMetricID))
			newKeys[keyID][metricID] = map[string]*encodedSeries{}
			if _, exists := newDescs[metricID]; !exists {
				desc := oldDescs[oldMetricID]
				newDescs[metricID] = encodedDesc{
					help: newDict.intern(oldDict.lookup(desc.help)),
					typ:  newDict.intern(oldDict.lookup(desc.typ)),
					unit: newDict.intern(oldDict.lookup(desc.unit)),
				}
			}
			for _, series := range seriesMap {
				postings := make([]uint32, 0, len(series.postings))
				var signature strings.Builder
				for i := 0; i+1 < len(series.postings); i += 2 {
					nameID := newDict.intern(oldDict.lookup(series.postings[i]))
					valueID := newDict.intern(oldDict.lookup(series.postings[i+1]))
					postings = append(postings, nameID, valueID)
					signature.WriteString(strconv.FormatUint(uint64(nameID), 36))
					signature.WriteByte('=')
					signature.WriteString(strconv.FormatUint(uint64(valueID), 36))
					signature.WriteByte(',')
				}
				newKeys[keyID][metricID][signature.String()] = &encodedSeries{
					postings:      postings,
					timestamps:    series.timestamps,
					values:        series.values,
					constantValue: series.constantValue,
					constantCount: series.constantCount,
				}
			}
		}
	}
	this.dict = newDict
	this.keys = newKeys
	this.descs = newDescs
}

func (this *MemoryMetricsStore) encodeLabels(labels map[string]string) ([]uint32, string) {
	names := make([]string, 0, len(labels))
	for name := range labels {
		if name != "scrape_path" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	postings := make([]uint32, 0, 2*len(names))
	var signature strings.Builder
	for _, name := range names {
		nameID := this.dict.intern(name)
		valueID := this.dict.intern(labels[name])
		postings = append(postings, nameID, valueID)
		signature.WriteString(strconv.FormatUint(uint64(nameID), 36))
		signature.WriteByte('=')
		signature.WriteString(strconv.FormatUint(uint64(valueID), 36))
		signature.WriteByte(',')
	}
	return postings, signature.String()
}

func (this *MemoryMetricsStore) decodeLabels(postings []uint32) map[string]string {
	if len(postings) == 0 {
		return nil
	}
	labels := make(map[string]string, len(postings)/2)
	for i := 0; i+1 < len(postings); i += 2 {
		labels[this.dict.lookup(postings[i])] = this.dict.lookup(postings[i+1])
	}
	return labels
}

func (this *stringDictionary) intern(value string) uint32 {
	if value == "" {
		return 0
	}
	if id, ok := this.ids[value]; ok {
		return id
	}
	id := uint32(len(this.values))
	this.ids[value] = id
	this.values = append(this.values, value)
	return id
}

func (this *stringDictionary) lookup(id uint32) string {
	if int(id) >= len(this.values) {
		return ""
	}
	return this.values[id]
}

func canonicalLabels(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		if name != "scrape_path" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var out strings.Builder
	for _, name := range names {
		out.WriteString(name)
		out.WriteByte('=')
		out.WriteString(labels[name])
		out.WriteByte(0)
	}
	return out.String()
}

func metricDescriptor(sampleName string, descs []statekit.PrometheusDesc) (statekit.PrometheusDesc, bool) {
	for _, desc := range descs {
		if sampleName == desc.Name {
			return desc, true
		}
		if desc.Type == statekit.PrometheusCounter {
			if sampleName == desc.Name+"_total" || sampleName == desc.Name+"_created" {
				return desc, true
			}
		}
		if desc.Type == statekit.PrometheusHistogram || desc.Type == statekit.PrometheusSummary {
			if sampleName == desc.Name+"_sum" || sampleName == desc.Name+"_count" ||
				(desc.Type == statekit.PrometheusHistogram && sampleName == desc.Name+"_bucket") {
				return desc, true
			}
		}
	}
	return statekit.PrometheusDesc{}, false
}

func updateConstantRun(series *encodedSeries) {
	n := len(series.values)
	if n == 0 {
		series.constantValue = 0
		series.constantCount = 0
		return
	}
	series.constantValue = series.values[n-1]
	series.constantCount = 1
	for i := n - 2; i >= 0 && series.values[i] == series.constantValue; i-- {
		series.constantCount++
	}
}

func constantRunEndingAt(series *encodedSeries, end int) int {
	if end == len(series.values) {
		return series.constantCount
	}
	if end <= 0 {
		return 0
	}
	value := series.values[end-1]
	count := 1
	for i := end - 2; i >= 0 && series.values[i] == value; i-- {
		count++
	}
	return count
}
