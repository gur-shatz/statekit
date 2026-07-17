package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/gur-shatz/statekit"
)

// StorageStats is the operational view exposed by the storage state and
// metrics. Sizes are estimates of retained Go memory (including slice
// capacity and approximate map overhead), not runtime heap measurements.
type StorageStats struct {
	EstimatedSizeKiB         map[string]float64 `json:"estimated_size_kib"`
	Items                    map[string]int64   `json:"items"`
	MetricsAggregatorEnabled bool               `json:"metrics_aggregator_enabled"`
	BackendError             string             `json:"backend_error,omitempty"`
}

// StoreObservability makes a MemoryStore both visible as a state and
// scrapeable as Prometheus metrics. Register it with a statekit Registry:
//
//	reg.Register(store.Observability())
type StoreObservability struct {
	store    *MemoryStore
	state    *statekit.ManualState
	mu       sync.Mutex
	cachedAt time.Time
	cached   StorageStats
}

func newStoreObservability(store *MemoryStore) *StoreObservability {
	return &StoreObservability{
		store: store,
		state: statekit.NewManualState("storage",
			statekit.WithHelp("Storage utilization, retained item counts, and backend health.")),
	}
}

func (this *MemoryStore) Observability() *StoreObservability {
	return this.observability
}

func (this *StoreObservability) Name() string {
	return this.state.Name()
}

func (this *StoreObservability) Snapshot() statekit.Snapshot {
	stats := this.stats()
	data := map[string]any{
		"estimated_size_kib":         stats.EstimatedSizeKiB,
		"items":                      stats.Items,
		"metrics_aggregator_enabled": stats.MetricsAggregatorEnabled,
	}
	if stats.BackendError != "" {
		data["backend_error"] = stats.BackendError
		this.state.Warn(stats.BackendError, data)
	} else {
		this.state.Pass("storage operational", data)
	}
	return this.state.Snapshot()
}

func (this *StoreObservability) DescribePrometheus() []statekit.PrometheusDesc {
	return []statekit.PrometheusDesc{
		{
			Name: "statekit_storage_estimated_size_kib", Help: "Estimated retained storage memory in KiB by area.",
			Type: statekit.PrometheusGauge, Labels: []string{"area"},
		},
		{
			Name: "statekit_storage_items", Help: "Current number of retained storage items by kind.",
			Type: statekit.PrometheusGauge, Labels: []string{"kind"},
		},
		{
			Name: "statekit_storage_metrics_aggregator_enabled", Help: "Whether metrics timeseries aggregation is enabled (1) or disabled (0).",
			Type: statekit.PrometheusGauge,
		},
		{
			Name: "statekit_storage_backend_error", Help: "Whether a storage persistence backend currently reports an error.",
			Type: statekit.PrometheusGauge,
		},
	}
}

func (this *StoreObservability) CollectPrometheus() []statekit.PrometheusSample {
	stats := this.stats()
	samples := make([]statekit.PrometheusSample, 0, len(stats.EstimatedSizeKiB)+len(stats.Items)+2)
	areas := sortedFloatKeys(stats.EstimatedSizeKiB)
	for _, area := range areas {
		samples = append(samples, statekit.PrometheusSample{
			Name: "statekit_storage_estimated_size_kib", Labels: map[string]string{"area": area},
			Value: stats.EstimatedSizeKiB[area],
		})
	}
	kinds := sortedIntKeys(stats.Items)
	for _, kind := range kinds {
		samples = append(samples, statekit.PrometheusSample{
			Name: "statekit_storage_items", Labels: map[string]string{"kind": kind},
			Value: float64(stats.Items[kind]),
		})
	}
	enabled := 0.0
	if stats.MetricsAggregatorEnabled {
		enabled = 1
	}
	backendError := 0.0
	if stats.BackendError != "" {
		backendError = 1
	}
	return append(samples,
		statekit.PrometheusSample{Name: "statekit_storage_metrics_aggregator_enabled", Value: enabled},
		statekit.PrometheusSample{Name: "statekit_storage_backend_error", Value: backendError},
	)
}

// stats coalesces the state and Prometheus reads performed by one registry
// scrape. Deep size accounting walks the retained structures, so doing it
// twice back-to-back would make observability needlessly expensive.
func (this *StoreObservability) stats() StorageStats {
	this.mu.Lock()
	defer this.mu.Unlock()
	now := time.Now()
	if !this.cachedAt.IsZero() && now.Sub(this.cachedAt) < time.Second {
		return this.cached
	}
	this.cached = this.store.Stats()
	this.cachedAt = now
	return this.cached
}

func (this *MemoryStore) Stats() StorageStats {
	stats := StorageStats{
		EstimatedSizeKiB: map[string]float64{},
		Items:            map[string]int64{},
	}
	var currentBytes, transitionBytes, incidentBytes, muteBytes, bookkeepingBytes uint64

	this.mu.RLock()
	for key, target := range this.targets {
		stats.Items["targets"]++
		currentBytes += 48 + uint64(len(key)) + estimateTargetSummary(target)
	}
	for identity, detail := range this.details {
		stats.Items["states"]++
		currentBytes += 48 + uint64(len(identity)) + estimateStateDetail(detail)
	}
	for identity, ring := range this.timelines {
		transitionBytes += 48 + uint64(len(identity)) + uint64(unsafe.Sizeof(*ring))
		stats.Items["transition_series"]++
		stats.Items["transitions"] += int64(len(ring.entries))
		transitionBytes += uint64(cap(ring.entries)) * uint64(unsafe.Sizeof(Transition{}))
		for _, transition := range ring.entries {
			transitionBytes += uint64(len(transition.Status) + len(transition.Reason))
		}
	}
	for identity, incident := range this.incidents {
		stats.Items["incidents"]++
		incidentBytes += 48 + uint64(len(identity)) + estimateIncident(incident)
	}
	for _, events := range this.incidentEvents {
		stats.Items["incident_events"] += int64(len(events))
		incidentBytes += 64 + uint64(len(events))*48
		for key, event := range events {
			incidentBytes += uint64(len(key)) + estimateIncidentEvent(event)
		}
	}
	for identity, mute := range this.mutes {
		stats.Items["mutes"]++
		muteBytes += 48 + uint64(len(identity)) + uint64(unsafe.Sizeof(mute))
		muteBytes += uint64(len(mute.Identity) + len(mute.TargetKey) + len(mute.Name) + len(mute.Status) + len(mute.Reason) + len(mute.OriginalStatus))
	}
	for key, identities := range this.docScopeIdentities {
		bookkeepingBytes += 64 + uint64(len(key)) + uint64(len(identities))*40
	}
	for key, targets := range this.docScopeTargets {
		bookkeepingBytes += 64 + uint64(len(key)) + uint64(len(targets))*40
	}
	bookkeepingBytes += uint64(len(this.docLastSeen))*48 + uint64(len(this.docIntervals))*40
	chart := this.chart
	metrics := this.metrics
	cache := this.docCache
	journal := this.journal
	this.mu.RUnlock()

	var chartStats ChartStoreStats
	if provider, ok := chart.(ChartStatsProvider); ok {
		chartStats = provider.ChartStats()
	}
	stats.Items["chart_buckets"] = chartStats.Buckets
	stats.Items["chart_entries"] = chartStats.Entries

	var metricStats MetricsStoreStats
	stats.MetricsAggregatorEnabled = metrics != nil
	if provider, ok := metrics.(MetricsStatsProvider); ok {
		metricStats = provider.MetricsStats()
	}
	stats.Items["metric_keys"] = metricStats.Keys
	stats.Items["metric_families"] = metricStats.Metrics
	stats.Items["metric_series"] = metricStats.Series
	stats.Items["metric_points"] = metricStats.Points
	stats.Items["metric_label_pairs"] = metricStats.Labels
	stats.Items["metric_strings"] = metricStats.Strings

	var cacheBytes uint64
	if provider, ok := cache.(DocumentCacheStatsProvider); ok {
		cacheStats := provider.DocumentCacheStats()
		stats.Items["document_cache_entries"] = cacheStats.Entries
		cacheBytes = cacheStats.CapacityBytes
	}
	stats.BackendError = storageBackendError(journal, chart)

	areas := map[string]uint64{
		"current_state":  currentBytes,
		"transitions":    transitionBytes,
		"chart":          chartStats.EstimatedBytes,
		"metrics":        metricStats.EstimatedBytes,
		"incidents":      incidentBytes,
		"mutes":          muteBytes,
		"bookkeeping":    bookkeepingBytes,
		"document_cache": cacheBytes,
	}
	var total uint64
	var itemTotal int64
	for area, bytes := range areas {
		stats.EstimatedSizeKiB[area] = float64(bytes) / 1024
		total += bytes
	}
	for _, count := range stats.Items {
		itemTotal += count
	}
	stats.EstimatedSizeKiB["total"] = float64(total) / 1024
	stats.Items["total"] = itemTotal
	return stats
}

func storageBackendError(journal *Journal, chart ChartStore) string {
	var errors []string
	if journal != nil {
		if err := journal.Err(); err != nil {
			errors = append(errors, "journal: "+err.Error())
		}
	}
	if file, ok := chart.(*FileChartStore); ok {
		if err := file.Err(); err != nil {
			errors = append(errors, "chart: "+err.Error())
		}
	}
	if len(errors) == 0 {
		return ""
	}
	sort.Strings(errors)
	return fmt.Sprintf("storage backend error: %s", strings.Join(errors, "; "))
}

func estimateTargetSummary(target TargetSummary) uint64 {
	size := uint64(unsafe.Sizeof(target))
	size += uint64(len(target.Key) + len(target.Name) + len(target.ScrapePath) + len(target.WorstStatus) + len(target.MaterialHash))
	size += estimateStringMap(target.Labels) + estimateIntMap(target.StatusCounts)
	size += uint64(cap(target.AffectedStates)) * uint64(unsafe.Sizeof(AffectedState{}))
	for _, state := range target.AffectedStates {
		size += uint64(len(state.Name) + len(state.Status) + len(state.Reason))
	}
	size += uint64(cap(target.States)) * uint64(unsafe.Sizeof(StateHeader{}))
	for _, header := range target.States {
		size += estimateStateHeaderDynamic(header)
	}
	return size
}

func estimateStateDetail(detail StateDetail) uint64 {
	size := uint64(unsafe.Sizeof(detail)) + estimateStateHeaderDynamic(detail.StateHeader)
	size += uint64(len(detail.ScrapedFrom) + len(detail.ScrapePath) + len(detail.Help) + len(detail.GroupName) + len(detail.DataHash))
	size += estimateStringMap(detail.Labels)
	size += uint64(cap(detail.LabelPath)) * uint64(unsafe.Sizeof(statekit.StateDisplayLabel{}))
	for _, label := range detail.LabelPath {
		size += uint64(len(label.Name) + len(label.Value))
	}
	size += estimateJSONValue(detail.Data)
	size += uint64(cap(detail.Children)) * 16
	for _, child := range detail.Children {
		size += uint64(len(child))
	}
	return size
}

func estimateStateHeaderDynamic(header StateHeader) uint64 {
	size := uint64(len(header.Identity) + len(header.ParentIdentity) + len(header.TargetKey) + len(header.Name))
	size += uint64(len(header.Status) + len(header.OriginalStatus) + len(header.Reason) + len(header.OriginalReason) + len(header.Importance))
	if header.Mute != nil {
		size += uint64(unsafe.Sizeof(*header.Mute))
	}
	return size
}

func estimateIncident(incident Incident) uint64 {
	size := uint64(unsafe.Sizeof(incident))
	size += uint64(len(incident.Identity) + len(incident.Source) + len(incident.ScrapedFrom) + len(incident.ScrapePath))
	size += uint64(len(incident.ID) + len(incident.Type) + len(incident.Title) + len(incident.Status) + len(incident.Severity))
	size += estimateStringMap(incident.Labels) + estimateJSONValue(incident.Topics)
	return size
}

func estimateIncidentEvent(event IncidentEvent) uint64 {
	return uint64(unsafe.Sizeof(event)) + uint64(len(event.EventKey)+len(event.Identity)+len(event.Seq)+len(event.Topic)+len(event.Message)) + estimateJSONValue(event.Data)
}

func estimateStringMap(values map[string]string) uint64 {
	size := uint64(64 + len(values)*48)
	for key, value := range values {
		size += uint64(len(key) + len(value))
	}
	return size
}

func estimateIntMap(values map[string]int) uint64 {
	size := uint64(64 + len(values)*32)
	for key := range values {
		size += uint64(len(key))
	}
	return size
}

func estimateJSONValue(value any) uint64 {
	if value == nil {
		return 0
	}
	data, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return uint64(len(data))
}

func sortedFloatKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

var _ statekit.State = (*StoreObservability)(nil)
var _ statekit.PrometheusCollector = (*StoreObservability)(nil)
