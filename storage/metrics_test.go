package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
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

func TestMemoryMetricsStoreDiscardsOutOfOrderTimestamp(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	sample := statekit.PrometheusSample{Name: "depth", Value: 2}
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(2*time.Minute))
	sample.Value = 1
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(time.Minute))
	sample.Value = 3
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(3*time.Minute))

	doc, err := store.Metrics("api", now, now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	series := doc.Metrics[0].Series[0]
	wantTimestamps := []int64{now.Add(2 * time.Minute).Unix(), now.Add(3 * time.Minute).Unix()}
	wantValues := []float64{2, 3}
	if !slices.Equal(series.Timestamps, wantTimestamps) || !slices.Equal(series.Values, wantValues) {
		t.Fatalf("series = %+v, want timestamps %v values %v", series, wantTimestamps, wantValues)
	}
}

func TestMemoryMetricsStoreReportsConstantSeriesAndUnit(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	descs := []statekit.PrometheusDesc{{
		Name: "request_latency_seconds", Help: "Request latency.", Type: statekit.PrometheusGauge, Unit: "seconds",
	}}
	store.IngestMetrics("api", descs, []statekit.PrometheusSample{
		{Name: "request_latency_seconds", Labels: map[string]string{"route": "constant"}, Value: 1.234},
		{Name: "request_latency_seconds", Labels: map[string]string{"route": "changing"}, Value: 1},
	}, now)
	store.IngestMetrics("api", descs, []statekit.PrometheusSample{
		{Name: "request_latency_seconds", Labels: map[string]string{"route": "constant"}, Value: 1.234},
		{Name: "request_latency_seconds", Labels: map[string]string{"route": "changing"}, Value: 2},
	}, now.Add(time.Minute))

	doc, err := store.Metrics("api", now, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	metric := doc.Metrics[0]
	if metric.Unit != "seconds" || metric.Type != "gauge" || metric.Help != "Request latency." {
		t.Fatalf("metric metadata = %+v", metric)
	}
	byRoute := map[string]MeasurementSeries{}
	for _, series := range metric.Series {
		byRoute[series.Labels["route"]] = series
	}
	if !byRoute["constant"].Constant {
		t.Fatalf("constant series = %+v", byRoute["constant"])
	}
	if byRoute["changing"].Constant {
		t.Fatalf("changing series = %+v", byRoute["changing"])
	}
}

func TestMemoryMetricsStoreBecomesConstantWithinCurrentWindow(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	sample := statekit.PrometheusSample{Name: "depth", Value: 1}
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now)
	sample.Value = 2
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(time.Minute))
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(2*time.Minute))

	full, err := store.Metrics("api", now, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if full.Metrics[0].Series[0].Constant {
		t.Fatalf("series should not be constant while the old value is in the window: %+v", full.Metrics[0].Series[0])
	}

	current, err := store.Metrics("api", now.Add(time.Minute), now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !current.Metrics[0].Series[0].Constant {
		t.Fatalf("series should become constant after the old value leaves the window: %+v", current.Metrics[0].Series[0])
	}
}

func TestMemoryMetricsStoreUpdatesConstantCountWhenLatestSampleIsReplaced(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	sample := statekit.PrometheusSample{Name: "depth", Value: 4}
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now)
	sample.Value = 5
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(time.Minute))
	sample.Value = 4
	store.IngestMetrics("api", nil, []statekit.PrometheusSample{sample}, now.Add(time.Minute+500*time.Millisecond))

	doc, err := store.Metrics("api", now, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Metrics[0].Series[0].Constant {
		t.Fatalf("replacement should extend the trailing constant run: %+v", doc.Metrics[0].Series[0])
	}
}

func TestMemoryMetricsStoreAppliesHistogramUnitToGeneratedSeries(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	store.IngestMetrics("api", []statekit.PrometheusDesc{{
		Name: "request_duration_seconds", Type: statekit.PrometheusHistogram, Unit: "seconds",
	}}, []statekit.PrometheusSample{{
		Name: "request_duration_seconds_sum", Value: 2.5,
	}}, now)

	doc, err := store.Metrics("api", now.Add(-time.Second), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.Metrics[0]; got.Unit != "seconds" || got.Type != "histogram" {
		t.Fatalf("generated histogram series metadata = %+v", got)
	}
}

func TestMemoryMetricsStoreAppliesCounterTypeToTotalSeries(t *testing.T) {
	store := NewMemoryMetricsStore(time.Hour, 10)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	descs := []statekit.PrometheusDesc{{
		Name: "requests", Help: "Requests processed.", Type: statekit.PrometheusCounter,
	}}
	store.IngestMetrics("api", descs, []statekit.PrometheusSample{{
		Name: "requests_total", Value: 10,
	}}, now)
	store.IngestMetrics("api", descs, []statekit.PrometheusSample{{
		Name: "requests_total", Value: 14,
	}}, now.Add(time.Minute))

	doc, err := store.Metrics("api", now, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	metric := doc.Metrics[0]
	if metric.Name != "requests_total" || metric.Type != "counter" || metric.Help != "Requests processed." {
		t.Fatalf("counter total metadata = %+v", metric)
	}
	if metric.Series[0].Constant {
		t.Fatalf("increasing counter reported constant: %+v", metric.Series[0])
	}
}
