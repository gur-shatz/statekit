package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

func TestMemoryMetricsStoreIngestQueryAndRetention(t *testing.T) {
	store := NewMemoryMetricsStore(2*time.Minute, 10)
	t0 := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	descs := []statekit.PrometheusDesc{{Name: "queue_depth", Help: "Queued jobs.", Type: statekit.PrometheusGauge}}

	store.IngestMetrics("edge > api", descs, []statekit.PrometheusSample{
		{Name: "queue_depth", Labels: map[string]string{"scrape_path": "edge > api", "queue": "fast"}, Value: 2},
	}, t0)
	store.IngestMetrics("edge > api", descs, []statekit.PrometheusSample{
		{Name: "queue_depth", Labels: map[string]string{"scrape_path": "edge > api", "queue": "fast"}, Value: 3},
	}, t0.Add(time.Minute))

	doc, err := store.Metrics("edge > api", t0, t0.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Metrics) != 1 || len(doc.Metrics[0].Series) != 1 {
		t.Fatalf("metrics = %+v", doc.Metrics)
	}
	series := doc.Metrics[0].Series[0]
	if len(series.Timestamps) != 2 || series.Values[0] != 2 || series.Values[1] != 3 {
		t.Fatalf("series = %+v", series)
	}
	if series.Labels["queue"] != "fast" {
		t.Fatalf("labels = %+v", series.Labels)
	}
	if _, exists := series.Labels["scrape_path"]; exists {
		t.Fatalf("scrape_path should be represented by document key: %+v", series.Labels)
	}

	store.IngestMetrics("edge > api", descs, []statekit.PrometheusSample{
		{Name: "queue_depth", Labels: map[string]string{"queue": "fast"}, Value: 4},
	}, t0.Add(3*time.Minute))
	doc, err = store.Metrics("edge > api", t0, t0.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.Metrics[0].Series[0].Timestamps; len(got) != 1 || got[0] != t0.Add(3*time.Minute).Unix() {
		t.Fatalf("timestamps after retention trim = %v", got)
	}
}

func TestMemoryMetricsStoreCapsSeriesByLargestValue(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 2)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{
		{Name: "work", Labels: map[string]string{"worker": "small"}, Value: 1},
		{Name: "work", Labels: map[string]string{"worker": "large"}, Value: 10},
		{Name: "work", Labels: map[string]string{"worker": "medium"}, Value: 5},
	}, now)

	doc, err := store.Metrics("api", now.Add(-time.Second), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Metrics) != 1 || len(doc.Metrics[0].Series) != 2 {
		t.Fatalf("metrics = %+v", doc.Metrics)
	}
	got := map[string]bool{}
	for _, series := range doc.Metrics[0].Series {
		got[series.Labels["worker"]] = true
	}
	if !got["large"] || !got["medium"] || got["small"] {
		t.Fatalf("retained workers = %+v", got)
	}

	store.IngestMetrics("api", nil, []statekit.PrometheusSample{
		{Name: "work", Labels: map[string]string{"worker": "huge"}, Value: 100},
		{Name: "work", Labels: map[string]string{"worker": "bigger"}, Value: 50},
	}, now.Add(time.Minute))
	doc, err = store.Metrics("api", now.Add(-time.Second), now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got = map[string]bool{}
	for _, series := range doc.Metrics[0].Series {
		got[series.Labels["worker"]] = true
	}
	if len(got) != 2 || !got["huge"] || !got["bigger"] {
		t.Fatalf("hard-capped workers after churn = %+v", got)
	}
}

func TestMetricsTimeseriesAPI(t *testing.T) {
	store := NewMemoryStore(WithMetricsStore(NewMemoryMetricsStore(0, 0)))
	now := time.Now()
	store.MetricsStore().IngestMetrics("region > api", []statekit.PrometheusDesc{{
		Name: "depth", Type: statekit.PrometheusGauge,
	}}, []statekit.PrometheusSample{{Name: "depth", Value: 7}}, now)

	request := httptest.NewRequest(http.MethodGet, "/metrics/timeseries?key="+url.QueryEscape("region > api"), nil)
	response := httptest.NewRecorder()
	NewAPI(store).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("ETag") == "" {
		t.Fatal("missing ETag")
	}
	var doc MetricsDocument
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Key != "region > api" || len(doc.Metrics) != 1 || doc.Metrics[0].Series[0].Values[0] != 7 {
		t.Fatalf("document = %+v", doc)
	}
}

func TestMemoryMetricsStoreReplacesDuplicateSecond(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 100, time.UTC)
	sample := statekit.PrometheusSample{Name: "depth", Value: 1}
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now)
	sample.Value = 2
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(500*time.Millisecond))

	doc, err := store.Metrics("api", now.Add(-time.Second), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	series := doc.Metrics[0].Series[0]
	if len(series.Values) != 1 || series.Values[0] != 2 {
		t.Fatalf("series = %+v", series)
	}
}
