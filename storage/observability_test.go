package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

func TestStorageObservabilityReportsSizesAndItems(t *testing.T) {
	metrics := NewMemoryMetricsStore(30*time.Minute, 100)
	store := NewMemoryStore(WithMetricsStore(metrics))
	now := time.Now()
	if err := store.IngestDocument(context.Background(), testDocument(), now); err != nil {
		t.Fatal(err)
	}
	metrics.IngestMetrics("issuer-east", nil, []statekit.PrometheusSample{{
		Name: "queue_depth", Labels: map[string]string{"queue": "default"}, Value: 4,
	}}, now)

	stats := store.Stats()
	if !stats.MetricsAggregatorEnabled {
		t.Fatal("metrics aggregator should be enabled")
	}
	if stats.Items["targets"] == 0 || stats.Items["states"] == 0 || stats.Items["metric_series"] != 1 || stats.Items["metric_points"] != 1 {
		t.Fatalf("items = %+v", stats.Items)
	}
	if stats.EstimatedSizeKiB["total"] <= 0 || stats.EstimatedSizeKiB["metrics"] <= 0 {
		t.Fatalf("estimated sizes = %+v", stats.EstimatedSizeKiB)
	}

	observer := store.Observability()
	snapshot := observer.Snapshot()
	if snapshot.Name != "storage" || snapshot.Status != statekit.Pass {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.Data["metrics_aggregator_enabled"] != true {
		t.Fatalf("snapshot data = %+v", snapshot.Data)
	}

	samples := observer.CollectPrometheus()
	assertStorageSample(t, samples, "statekit_storage_items", "kind", "metric_points", 1)
	assertStorageSample(t, samples, "statekit_storage_metrics_aggregator_enabled", "", "", 1)
	if got := sampleValue(samples, "statekit_storage_estimated_size_kib", "area", "total"); got <= 0 {
		t.Fatalf("total size sample = %v", got)
	}
}

func TestMetricsAggregationDisabledByDefault(t *testing.T) {
	store := NewMemoryStore()
	if store.MetricsStore() != nil {
		t.Fatal("metrics store should be nil unless explicitly configured")
	}
	stats := store.Stats()
	if stats.MetricsAggregatorEnabled {
		t.Fatal("stats should report metrics aggregation disabled")
	}
	assertStorageSample(t, store.Observability().CollectPrometheus(),
		"statekit_storage_metrics_aggregator_enabled", "", "", 0)

	handler := NewAPI(store).Handler()
	status := httptest.NewRecorder()
	handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/metrics/status", nil))
	if status.Code != http.StatusOK || status.Body.String() != "{\"enabled\":false}\n" {
		t.Fatalf("status response = %d %q", status.Code, status.Body.String())
	}
	timeseries := httptest.NewRecorder()
	handler.ServeHTTP(timeseries, httptest.NewRequest(http.MethodGet, "/metrics/timeseries?key=issuer", nil))
	if timeseries.Code != http.StatusNotFound {
		t.Fatalf("timeseries status = %d, want 404", timeseries.Code)
	}
}

func assertStorageSample(t *testing.T, samples []statekit.PrometheusSample, name, label, value string, want float64) {
	t.Helper()
	if got := sampleValue(samples, name, label, value); got != want {
		t.Fatalf("%s{%s=%q} = %v, want %v; samples = %+v", name, label, value, got, want, samples)
	}
}

func sampleValue(samples []statekit.PrometheusSample, name, label, value string) float64 {
	for _, sample := range samples {
		if sample.Name == name && (label == "" || sample.Labels[label] == value) {
			return sample.Value
		}
	}
	return -1
}
