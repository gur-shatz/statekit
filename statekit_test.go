package statekit

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type embeddedManualState struct {
	ManualState
}

type embeddedAggregateState struct {
	AggregateState
}

func TestManualStateTracksHistory(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s := NewManualState("database", WithClock(func() time.Time { return now }))

	s.Fail("connection refused", map[string]any{"host": "db"})
	snap := s.Snapshot()

	if snap.Status != Fail {
		t.Fatalf("level = %v, want %v", snap.Status, Fail)
	}
	if snap.Reason != "connection refused" {
		t.Fatalf("reason = %q", snap.Reason)
	}
	if len(snap.History) != 1 {
		t.Fatalf("history len = %d, want 1", len(snap.History))
	}
	if snap.History[0].Status != Fail {
		t.Fatalf("history states = %+v", snap.History)
	}
	if snap.History[0].Reason != "connection refused" {
		t.Fatalf("history reason = %q, want %q", snap.History[0].Reason, "connection refused")
	}
	if got := snap.History[0].Data["host"]; got != "db" {
		t.Fatalf("history data host = %#v, want %q", got, "db")
	}
	if snap.History[0].SecsAgo != 0 {
		t.Fatalf("history secs ago = %+v, want zero at fixed clock", snap.History)
	}
}

func TestManualStateHistoryCarriesReasonAndDataPerTransition(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s := NewManualState("database", WithClock(func() time.Time { return now }))

	now = now.Add(time.Second)
	s.Warn("slow", map[string]any{"latency_ms": 120})
	now = now.Add(time.Second)
	s.Fail("connection refused", map[string]any{"host": "db"})
	now = now.Add(time.Second)
	s.Pass("recovered after retry", map[string]any{"retries": 3})

	snap := s.Snapshot()
	if len(snap.History) != 3 {
		t.Fatalf("history len = %d, want 3 transitions", len(snap.History))
	}
	// History is exposed most-recent-first.
	want := []struct {
		status Status
		reason string
		data   map[string]any
	}{
		{Pass, "recovered after retry", map[string]any{"retries": 3}},
		{Fail, "connection refused", map[string]any{"host": "db"}},
		{Warn, "slow", map[string]any{"latency_ms": 120}},
	}
	for i, w := range want {
		got := snap.History[i]
		if got.Status != w.status || got.Reason != w.reason {
			t.Fatalf("history[%d] = {status=%v reason=%q}, want {status=%v reason=%q}",
				i, got.Status, got.Reason, w.status, w.reason)
		}
		for k, v := range w.data {
			if got.Data[k] != v {
				t.Fatalf("history[%d].data[%q] = %#v, want %#v", i, k, got.Data[k], v)
			}
		}
	}
}

func TestManualStateInitSupportsEmbedding(t *testing.T) {
	var embedded embeddedManualState
	state := embedded.Init("database", WithHelp("Database health."))
	if state != &embedded.ManualState {
		t.Fatal("Init returned different manual state")
	}

	embedded.Warn("slow", nil)
	snap := embedded.Snapshot()
	if snap.Name != "database" || snap.Status != Warn || snap.Help != "Database health." {
		t.Fatalf("embedded manual snapshot = %+v", snap)
	}
}

func TestManualStateHistoryReportsSecondsAgo(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s := NewManualState("database", WithClock(func() time.Time { return now }))

	now = now.Add(2 * time.Second)
	s.Warn("slow", nil)
	now = now.Add(3 * time.Second)
	snap := s.Snapshot()

	if len(snap.History) != 1 {
		t.Fatalf("history len = %d, want 1", len(snap.History))
	}
	if got := snap.History[0].SecsAgo; got != 3 {
		t.Fatalf("warn history secs ago = %v, want 3", got)
	}
}

func TestManualStatePreservesReasonForPass(t *testing.T) {
	s := NewManualState("database")
	s.Warn("slow", nil)
	s.Pass("recovered", nil)

	snap := s.Snapshot()
	if snap.Reason != "recovered" {
		t.Fatalf("pass reason = %q, want %q", snap.Reason, "recovered")
	}
	// Most recent is at the head.
	mostRecent := snap.History[0]
	if mostRecent.Status != Pass || mostRecent.Reason != "recovered" {
		t.Fatalf("most-recent history entry = %+v, want pass with reason %q", mostRecent, "recovered")
	}
}

func TestManualStateUpdatesCurrentReasonAndDataWithoutHistoryForSameStatus(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s := NewManualState("database", WithClock(func() time.Time { return now }))

	s.Fail("latency above fail threshold", map[string]any{"latency_ms": 250})
	first := s.Snapshot()
	now = now.Add(2 * time.Second)
	s.Fail("latency above fail threshold", map[string]any{"latency_ms": 190})
	second := s.Snapshot()
	now = now.Add(2 * time.Second)
	s.Fail("connection refused", map[string]any{"latency_ms": 0})
	third := s.Snapshot()

	if len(second.History) != len(first.History) || len(third.History) != len(first.History) {
		t.Fatalf("history changed for same-status updates: %d %d %d", len(first.History), len(second.History), len(third.History))
	}
	if !first.ChangedAt.Equal(second.ChangedAt) || !first.ChangedAt.Equal(third.ChangedAt) {
		t.Fatalf("changed_at moved for same-status updates: %s %s %s", first.ChangedAt, second.ChangedAt, third.ChangedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) || !third.UpdatedAt.After(second.UpdatedAt) {
		t.Fatalf("updated_at did not move for same-status updates: %s %s %s", first.UpdatedAt, second.UpdatedAt, third.UpdatedAt)
	}
	if third.UpdatedSecsAgo != 0 {
		t.Fatalf("updated_secs_ago = %d, want 0 at fixed clock", third.UpdatedSecsAgo)
	}
	if third.Reason != "connection refused" {
		t.Fatalf("current reason = %q, want latest reason", third.Reason)
	}
	if got := third.Data["latency_ms"]; got != 0 {
		t.Fatalf("current data latency_ms = %#v, want latest data", got)
	}
}

func TestAggregateStateCanAddChildrenProgressively(t *testing.T) {
	root := NewStateAggregator("issuer")
	db := NewManualState("database")
	cache := NewManualState("cache", WithImportance(Informational))

	root.Add(db)
	if snap := root.Snapshot(); snap.Status != Pass || len(snap.Checks) != 1 {
		t.Fatalf("initial aggregate = level %v checks %d", snap.Status, len(snap.Checks))
	}

	cache.Down("cache unreachable", nil)
	root.Add(cache)
	snap := root.Snapshot()
	if snap.Status != Warn {
		t.Fatalf("non-critical down contribution = %v, want warn", snap.Status)
	}
	if len(snap.Checks) != 2 {
		t.Fatalf("checks len = %d, want 2", len(snap.Checks))
	}

	db.Fail("db failing", nil)
	if snap := root.Snapshot(); snap.Status != Fail {
		t.Fatalf("critical fail contribution = %v, want fail", snap.Status)
	}
}

func TestAggregateStateInitSupportsEmbedding(t *testing.T) {
	var embedded embeddedAggregateState
	state := embedded.Init("issuer", WithHelp("Issuer aggregate."))
	if state != &embedded.AggregateState {
		t.Fatal("Init returned different aggregate state")
	}

	db := NewManualState("database")
	db.Fail("connection refused", nil)
	embedded.AddCheck(db)

	snap := embedded.Snapshot()
	if snap.Name != "issuer" || snap.Status != Fail || snap.Help != "Issuer aggregate." {
		t.Fatalf("embedded aggregate snapshot = %+v", snap)
	}
	if len(snap.Checks) != 1 || snap.Checks[0].Name != "database" {
		t.Fatalf("embedded aggregate checks = %+v", snap.Checks)
	}
}

func TestAggregateStateCanCapChildContribution(t *testing.T) {
	root := NewStateAggregator("issuer")
	upstream := NewManualState("optional-upstream")

	upstream.Fail("upstream failing", nil)
	root.AddInformational(upstream)

	snap := root.Snapshot()
	if snap.Status != Warn {
		t.Fatalf("capped fail contribution = %v, want warn", snap.Status)
	}
	if snap.Reason != "optional-upstream: upstream failing" {
		t.Fatalf("reason = %q", snap.Reason)
	}
	if len(snap.Checks) != 1 || snap.Checks[0].Status != Fail {
		t.Fatalf("child snapshot should preserve real level: %+v", snap.Checks)
	}
}

func TestAggregateStateCanUseCustomWorstChildContribution(t *testing.T) {
	root := NewStateAggregator("issuer")
	telemetry := NewManualState("telemetry")

	telemetry.Down("telemetry unavailable", nil)
	root.AddWithWorstStatus(Pass, telemetry)

	snap := root.Snapshot()
	if snap.Status != Pass {
		t.Fatalf("capped down contribution = %v, want pass", snap.Status)
	}
	if snap.Reason != "" {
		t.Fatalf("reason = %q, want empty", snap.Reason)
	}
}

func TestAggregateStateAddCheckRejectsAggregates(t *testing.T) {
	root := NewStateAggregator("issuer")
	child := NewStateAggregator("database-group")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("AddCheck accepted aggregate child, want panic")
		}
	}()
	root.AddCheck(child)
}

func TestAggregateStateAddRejectsAggregates(t *testing.T) {
	root := NewStateAggregator("issuer")
	child := NewStateAggregator("database-group")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Add accepted aggregate child, want panic")
		}
	}()
	root.Add(child)
}

func TestRegistryEmitsStateAndCollectorPrometheus(t *testing.T) {
	reg := NewRegistry(WithLabel("component", "issuer"))
	state := NewManualState("database")
	state.Warn("slow", nil)
	if err := reg.Register(state); err != nil {
		t.Fatal(err)
	}

	counter := NewCounter("queries_total", "Total queries.")
	counter.Add(3)
	if err := reg.RegisterCollectors(counter); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# HELP queries_total Total queries.",
		"# TYPE queries_total counter",
		`queries_total{component="issuer"} 3`,
		`state_level{component="issuer",importance="important",state="database"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestRegistryPrometheusSampleLabelsOverrideRegistryLabels(t *testing.T) {
	reg := NewRegistry(WithLabel("example", "registry"), WithLabel("region", "local"))
	counter := NewCounterVec("requests_total", "Total requests.", "example")
	counter.WithLabelValues("sample").Add(1)
	if err := reg.RegisterCollectors(counter); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	want := `requests_total{example="sample",region="local"} 1`
	if !strings.Contains(text, want) {
		t.Fatalf("prometheus output missing %q:\n%s", want, text)
	}
}

func TestRegistryPrometheusRefreshesDynamicCollectorDescriptors(t *testing.T) {
	reg := NewRegistry()
	collector := &dynamicPrometheusCollector{}
	if err := reg.RegisterCollectors(collector); err != nil {
		t.Fatal(err)
	}
	collector.descs = []PrometheusDesc{{
		Name: "scraped_requests_total",
		Help: "Requests scraped from a target.",
		Type: PrometheusCounter,
	}}
	collector.samples = []PrometheusSample{{
		Name:  "scraped_requests_total",
		Value: 2,
	}}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# HELP scraped_requests_total Requests scraped from a target.",
		"# TYPE scraped_requests_total counter",
		"scraped_requests_total 2",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

type dynamicPrometheusCollector struct {
	descs   []PrometheusDesc
	samples []PrometheusSample
}

func (c *dynamicPrometheusCollector) DescribePrometheus() []PrometheusDesc {
	return c.descs
}

func (c *dynamicPrometheusCollector) CollectPrometheus() []PrometheusSample {
	return c.samples
}

func TestRegistryStateDisplayDocumentUsesLabelPath(t *testing.T) {
	reg := NewRegistry(
		WithLabel("system", "payments"),
		WithLabel("component", "issuer"),
	)
	state := NewManualState("database")
	state.Fail("connection refused", nil)
	if err := reg.Register(state); err != nil {
		t.Fatal(err)
	}

	doc := reg.StateDisplay()
	if doc.Kind != StateDisplayKind {
		t.Fatalf("kind = %q", doc.Kind)
	}
	if len(doc.LabelPath) != 2 {
		t.Fatalf("label path len = %d, want 2", len(doc.LabelPath))
	}
	if doc.LabelPath[0] != (StateDisplayLabel{Name: "system", Value: "payments"}) {
		t.Fatalf("first label = %+v", doc.LabelPath[0])
	}
	if doc.LabelPath[1] != (StateDisplayLabel{Name: "component", Value: "issuer"}) {
		t.Fatalf("second label = %+v", doc.LabelPath[1])
	}
	if len(doc.States) != 2 {
		t.Fatalf("states len = %d, want 2 (health + database)", len(doc.States))
	}
	if doc.States[0].Name != "health" || doc.States[0].Status != Fail {
		t.Fatalf("first state = %+v, want health/fail", doc.States[0])
	}
	if doc.States[0].Reason != "fail:database" {
		t.Fatalf("health reason = %q, want %q", doc.States[0].Reason, "fail:database")
	}
	if doc.States[1].Name != "database" || doc.States[1].Status != Fail {
		t.Fatalf("second state = %+v, want database/fail", doc.States[1])
	}
}

func TestRegistryStateDisplayHandlers(t *testing.T) {
	reg := NewRegistry(WithLabel("service", "checkout"))
	state := NewManualState("database")
	state.Warn("slow", nil)
	if err := reg.Register(state); err != nil {
		t.Fatal(err)
	}

	yamlResponse := httptest.NewRecorder()
	reg.StateDisplayYAMLHandler().ServeHTTP(yamlResponse, httptest.NewRequest("GET", "/state", nil))
	if got := yamlResponse.Header().Get("Content-Type"); got != "text/yaml; charset=utf-8" {
		t.Fatalf("yaml content type = %q", got)
	}
	text := yamlResponse.Body.String()
	for _, want := range []string{
		"kind: statekit.state.v1",
		"label_path:",
		"name: service",
		"value: checkout",
		"status: warn",
		"changed_at:",
		"history:",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("yaml display missing %q:\n%s", want, text)
		}
	}

	shortResponse := httptest.NewRecorder()
	reg.StateDisplayYAMLHandler().ServeHTTP(shortResponse, httptest.NewRequest("GET", "/state?format=short", nil))
	if got := shortResponse.Code; got != 200 {
		t.Fatalf("short yaml status = %d", got)
	}
	shortText := shortResponse.Body.String()
	if strings.Contains(shortText, "history:") {
		t.Fatalf("short yaml display should omit history:\n%s", shortText)
	}
	if !strings.Contains(shortText, "status: warn") {
		t.Fatalf("short yaml display missing state:\n%s", shortText)
	}

	badResponse := httptest.NewRecorder()
	reg.StateDisplayYAMLHandler().ServeHTTP(badResponse, httptest.NewRequest("GET", "/state?format=tiny", nil))
	if got := badResponse.Code; got != http.StatusBadRequest {
		t.Fatalf("bad format status = %d, want %d", got, http.StatusBadRequest)
	}
}

func TestLocalFormattingHelpers(t *testing.T) {
	state := NewManualState("database")
	state.Warn("slow", map[string]any{"latency_ms": 42})

	stateJSON, err := StateJSON(state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stateJSON), `"status": "warn"`) || !strings.Contains(string(stateJSON), `"data"`) {
		t.Fatalf("snapshot json = %s", stateJSON)
	}

	stateYAML, err := StateYAML(state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stateYAML), "status: warn") || !strings.Contains(string(stateYAML), "data:") {
		t.Fatalf("snapshot yaml = %s", stateYAML)
	}
	stateYaml, err := StateYaml(state)
	if err != nil {
		t.Fatal(err)
	}
	if string(stateYaml) != string(stateYAML) {
		t.Fatalf("StateYaml differs from StateYAML:\n%s\n%s", stateYaml, stateYAML)
	}
	snapshotJSON, err := SnapshotJSON(state)
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshotJSON) != string(stateJSON) {
		t.Fatalf("SnapshotJSON differs from StateJSON:\n%s\n%s", snapshotJSON, stateJSON)
	}

	stateResponse := httptest.NewRecorder()
	StateHandlerFunc(state, "yaml")(stateResponse, httptest.NewRequest(http.MethodGet, "/state", nil))
	if got := stateResponse.Header().Get("Content-Type"); got != "text/yaml; charset=utf-8" {
		t.Fatalf("state handler content type = %q", got)
	}
	if !strings.Contains(stateResponse.Body.String(), "status: warn") {
		t.Fatalf("state handler body = %s", stateResponse.Body.String())
	}

	counter := NewCounter("requests_total", "Total requests.")
	counter.Add(7)

	metricsJSON, err := PrometheusCollectorJSON(counter)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metricsJSON), `"metrics"`) ||
		!strings.Contains(string(metricsJSON), `"name": "requests_total"`) ||
		!strings.Contains(string(metricsJSON), `"value": 7`) {
		t.Fatalf("collector json = %s", metricsJSON)
	}

	metricsYAML, err := PrometheusCollectorYAML(counter)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metricsYAML), "metrics:") ||
		!strings.Contains(string(metricsYAML), "# counter - Total requests.") ||
		!strings.Contains(string(metricsYAML), "requests_total: 7") {
		t.Fatalf("collector yaml = %s", metricsYAML)
	}

	metricsResponse := httptest.NewRecorder()
	PrometheusCollectorHandlerFunc(counter, "json")(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if got := metricsResponse.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("metrics handler content type = %q", got)
	}
	if !strings.Contains(metricsResponse.Body.String(), `"requests_total"`) {
		t.Fatalf("metrics handler body = %s", metricsResponse.Body.String())
	}
}

func TestMetricsYAMLMapForm(t *testing.T) {
	counter := NewCounter("requests_total", "Total requests.")
	counter.Add(7)
	gv := NewGaugeVec("inflight", "In-flight requests.", "method", "path")
	gv.WithLabelValues("GET", "/x").Set(2)
	gv.WithLabelValues("POST", "/y").Set(5)

	state := NewManualState("api")
	state.Warn("slow", nil)
	state.AddMetric(counter)
	state.AddMetric(gv)

	out, err := StateYAML(state)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	for _, want := range []string{
		"metrics:",
		"# counter - Total requests.",
		"requests_total: 7",
		"# gauge - In-flight requests.",
		"inflight:",
		"method=GET,path=/x: 2",
		"method=POST,path=/y: 5",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("metrics yaml missing %q:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"samples:", "- name:", "- value:"} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("metrics yaml still contains legacy %q:\n%s", unwanted, got)
		}
	}

	var roundTrip Snapshot
	if err := yaml.Unmarshal(out, &roundTrip); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	if len(roundTrip.Metrics) != 1 || len(roundTrip.Metrics[0].Metrics) != 2 {
		t.Fatalf("round-tripped metrics: %+v", roundTrip.Metrics)
	}
	byName := map[string]metricSnapshot{}
	for _, m := range roundTrip.Metrics[0].Metrics {
		byName[m.Name] = m
	}
	if rt := byName["requests_total"]; rt.Type != PrometheusCounter || len(rt.Samples) != 1 || rt.Samples[0].Value != 7 {
		t.Fatalf("counter round-trip: %+v", rt)
	}
	if inflight := byName["inflight"]; inflight.Type != PrometheusGauge || len(inflight.Samples) != 2 {
		t.Fatalf("gauge round-trip: %+v", inflight)
	}
}

func TestMetricsYAMLHistogramShape(t *testing.T) {
	hist := collectorSnapshot{
		Metrics: []metricSnapshot{{
			Name: "request_duration_seconds",
			Help: "Request duration.",
			Type: PrometheusHistogram,
			Samples: []metricSample{
				{Labels: map[string]string{"le": "0.1"}, Value: 1},
				{Labels: map[string]string{"le": "0.5"}, Value: 3},
				{Labels: map[string]string{"le": "+Inf"}, Value: 4},
				{Value: 4},
			},
		}},
	}
	out, err := yaml.Marshal(hist)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	if strings.Contains(got, "metrics:") {
		t.Fatalf("standalone histogram yaml should not have metrics wrapper:\n%s", got)
	}
	for _, want := range []string{
		"# histogram - Request duration.",
		"request_duration_seconds:",
		"count: 4",
		"buckets:",
		"0.1: 1",
		"+Inf: 4",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("histogram yaml missing %q:\n%s", want, got)
		}
	}

	var back collectorSnapshot
	if err := yaml.Unmarshal(out, &back); err != nil {
		t.Fatalf("histogram unmarshal: %v", err)
	}
	if len(back.Metrics) != 1 {
		t.Fatalf("decoded metrics len = %d", len(back.Metrics))
	}
	m := back.Metrics[0]
	if m.Type != PrometheusHistogram || m.Name != "request_duration_seconds" {
		t.Fatalf("decoded metric: %+v", m)
	}
	if len(m.Samples) != 4 {
		t.Fatalf("decoded samples len = %d, want 4", len(m.Samples))
	}
}

func TestLocalFormattingHandlersRejectBadFormat(t *testing.T) {
	state := NewManualState("database")
	response := httptest.NewRecorder()
	StateHandlerFunc(state, "toml")(response, httptest.NewRequest(http.MethodGet, "/state", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
}

func TestManualStateAddMetricAddsCollectorSnapshot(t *testing.T) {
	state := NewManualState("database")
	state.Warn("slow", nil)
	gauge := NewGauge("database_latency_ms", "Current database latency.")
	gauge.Set(42)
	state.AddMetric(gauge)

	snap := state.Snapshot()
	if snap.Name != "database" || snap.Status != Warn {
		t.Fatalf("snapshot = %+v", snap)
	}
	if len(snap.Metrics) != 1 {
		t.Fatalf("metrics len = %d, want 1", len(snap.Metrics))
	}
	if snap.Metrics[0].Metrics[0].Name != "database_latency_ms" || snap.Metrics[0].Metrics[0].Samples[0].Value != 42 {
		t.Fatalf("metrics = %+v", snap.Metrics)
	}

	out, err := SnapshotJSON(state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"metrics"`) || !strings.Contains(string(out), `"database_latency_ms"`) {
		t.Fatalf("snapshot json missing metrics: %s", out)
	}
}

func TestAggregateStateAddMetricAddsCollectorSnapshot(t *testing.T) {
	state := NewStateAggregator("database")
	gauge := NewGauge("database_latency_ms", "Current database latency.")
	gauge.Set(42)
	state.AddMetric(gauge)

	snap := state.Snapshot()
	if len(snap.Metrics) != 1 {
		t.Fatalf("metrics len = %d, want 1", len(snap.Metrics))
	}
	if snap.Metrics[0].Metrics[0].Name != "database_latency_ms" || snap.Metrics[0].Metrics[0].Samples[0].Value != 42 {
		t.Fatalf("metrics = %+v", snap.Metrics)
	}
}

func TestRegistryMountIsolatesInstancesByPrefix(t *testing.T) {
	mux := http.NewServeMux()

	east := NewRegistry(WithLabel("component", "issuer-east"))
	eastState := NewManualState("database")
	eastState.Warn("slow", nil)
	if err := east.Register(eastState); err != nil {
		t.Fatal(err)
	}
	east.Mount(mux, "/east")

	west := NewRegistry(WithLabel("component", "issuer-west"))
	westState := NewManualState("database")
	westState.Fail("connection refused", nil)
	if err := west.Register(westState); err != nil {
		t.Fatal(err)
	}
	west.Mount(mux, "west")

	eastResponse := httptest.NewRecorder()
	mux.ServeHTTP(eastResponse, httptest.NewRequest("GET", "/east/state", nil))
	eastText := eastResponse.Body.String()
	if !strings.Contains(eastText, "value: issuer-east") || !strings.Contains(eastText, "status: warn") {
		t.Fatalf("east mounted state unexpected:\n%s", eastText)
	}
	if strings.Contains(eastText, "issuer-west") {
		t.Fatalf("east mounted state leaked west registry:\n%s", eastText)
	}

	westResponse := httptest.NewRecorder()
	mux.ServeHTTP(westResponse, httptest.NewRequest("GET", "/west/state?format=short", nil))
	westText := westResponse.Body.String()
	if !strings.Contains(westText, "value: issuer-west") || !strings.Contains(westText, "status: fail") {
		t.Fatalf("west mounted state unexpected:\n%s", westText)
	}
	if strings.Contains(westText, "history:") {
		t.Fatalf("short mounted state should omit history:\n%s", westText)
	}
}

func TestRegistryPrometheusGroupsDescriptorsWithSamples(t *testing.T) {
	reg := NewRegistry(WithLabel("component", "issuer"))
	state := NewManualState("database")
	if err := reg.Register(state); err != nil {
		t.Fatal(err)
	}

	counter := NewCounter("queries_total", "Total queries.")
	counter.Add(3)
	if err := reg.RegisterCollectors(counter); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()

	queriesType := strings.Index(text, "# TYPE queries_total counter\n")
	queriesSample := strings.Index(text, "queries_total{component=\"issuer\"} 3\n")
	stateHelp := strings.Index(text, "# HELP state_level ")
	if queriesType == -1 || queriesSample == -1 || stateHelp == -1 {
		t.Fatalf("prometheus output missing expected sections:\n%s", text)
	}
	if !(queriesType < queriesSample && queriesSample < stateHelp) {
		t.Fatalf("prometheus descriptor and sample were not grouped:\n%s", text)
	}
}

func TestScalarMetricsAddConcurrently(t *testing.T) {
	const goroutines = 32
	const iterations = 1000

	counter := NewCounter("requests_total", "Total requests.")
	gauge := NewGauge("queue_depth", "Queue depth.")

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				counter.Inc()
				gauge.Add(1)
			}
		}()
	}
	wg.Wait()

	want := uint64(goroutines * iterations)
	if got := counter.Get(); got != want {
		t.Fatalf("counter = %v, want %v", got, want)
	}
	if got, want := gauge.Get(), int64(goroutines*iterations); got != want {
		t.Fatalf("gauge = %v, want %v", got, want)
	}
}

func TestScalarMetricsCanBeInitializedInPlace(t *testing.T) {
	var counter Counter
	var gauge Gauge

	counter.Init("requests_total", "Total requests.").Inc()
	gauge.Init("queue_depth", "Queue depth.").Set(5)

	if got := counter.Get(); got != 1 {
		t.Fatalf("counter = %v, want 1", got)
	}
	if got := gauge.Get(); got != 5 {
		t.Fatalf("gauge = %v, want 5", got)
	}
}

func TestVecMetricsEmitLabeledSamples(t *testing.T) {
	reg := NewRegistry(WithLabel("component", "issuer"))
	requests := NewCounterVec("requests_total", "Total requests.", "route", "status")
	queue := NewGaugeVec("queue_depth", "Queue depth.", "queue")

	requests.WithLabelValues("/checkout", "200").Add(12)
	requests.WithLabelValues("/checkout", "500").Inc()
	queue.WithLabelValues("default").Set(7)
	queue.WithLabelValues("priority").Set(3)

	if err := reg.RegisterCollectors(requests); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterCollectors(queue); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := reg.Prometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"# HELP queue_depth Queue depth.",
		"# TYPE queue_depth gauge",
		`queue_depth{component="issuer",queue="default"} 7`,
		`queue_depth{component="issuer",queue="priority"} 3`,
		"# HELP requests_total Total requests.",
		"# TYPE requests_total counter",
		`requests_total{component="issuer",route="/checkout",status="200"} 12`,
		`requests_total{component="issuer",route="/checkout",status="500"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

func TestVecMetricLabelCardinality(t *testing.T) {
	counter := NewCounterVec("requests_total", "Total requests.", "route", "status")
	if _, err := counter.GetMetricWithLabelValues("/checkout"); err == nil {
		t.Fatal("counter vec accepted the wrong number of label values")
	}

	gauge := NewGaugeVec("queue_depth", "Queue depth.", "queue")
	if _, err := gauge.GetMetricWithLabelValues("default", "extra"); err == nil {
		t.Fatal("gauge vec accepted the wrong number of label values")
	}
}

func TestVecMetricsAddConcurrently(t *testing.T) {
	const goroutines = 32
	const iterations = 1000

	counter := NewCounterVec("requests_total", "Total requests.", "route", "status")
	gauge := NewGaugeVec("queue_depth", "Queue depth.", "queue")

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				counter.WithLabelValues("/checkout", "200").Inc()
				gauge.WithLabelValues("default").Add(1)
			}
		}()
	}
	wg.Wait()

	wantCounter := uint64(goroutines * iterations)
	wantGauge := int64(goroutines * iterations)
	if got := counter.WithLabelValues("/checkout", "200").Get(); got != wantCounter {
		t.Fatalf("counter vec child = %v, want %v", got, wantCounter)
	}
	if got := gauge.WithLabelValues("default").Get(); got != wantGauge {
		t.Fatalf("gauge vec child = %v, want %v", got, wantGauge)
	}
}

func TestRegistryRejectsConflictingDescriptorLabels(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterCollectors(NewCounterVec("requests_total", "Total requests.", "route")); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterCollectors(NewCounterVec("requests_total", "Total requests.", "status")); err == nil {
		t.Fatal("registry accepted conflicting descriptor label names")
	}
}

func TestFailRatioAllFailedPolicy(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	fr := NewFailRatio("upstream", time.Minute, AllFailed(2, Fail), WithClock(func() time.Time { return now }))

	fr.Fail()
	if snap := fr.Snapshot(); snap.Status != Pass {
		t.Fatalf("one failure level = %v, want pass due min samples", snap.Status)
	}

	fr.Fail()
	snap := fr.Snapshot()
	if snap.Status != Fail {
		t.Fatalf("two failures level = %v, want fail", snap.Status)
	}

	fr.Pass()
	if snap := fr.Snapshot(); snap.Status != Pass {
		t.Fatalf("mixed outcomes level = %v, want pass", snap.Status)
	}
}

func TestFailRatioThresholdPolicyAndWindow(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	fr := NewFailRatio("upstream", time.Minute, RatioPolicy{MinSamples: 3, WarnAt: 0.5, FailAt: 0.75}, WithClock(func() time.Time { return now }))

	fr.Fail()
	fr.Pass()
	fr.Fail()
	if snap := fr.Snapshot(); snap.Status != Warn {
		t.Fatalf("2/3 failures level = %v, want warn", snap.Status)
	}

	fr.Fail()
	if snap := fr.Snapshot(); snap.Status != Fail {
		t.Fatalf("3/4 failures level = %v, want fail", snap.Status)
	}

	now = now.Add(2 * time.Minute)
	if snap := fr.Snapshot(); snap.Status != Pass {
		t.Fatalf("expired window level = %v, want pass", snap.Status)
	}
	if rs := fr.RatioSnapshot(); rs.Total != 0 {
		t.Fatalf("expired window total = %d, want 0", rs.Total)
	}
}

func TestFailMapTracksFailingItems(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	fm := NewFailMap("collection", WithClock(func() time.Time { return now }))

	if snap := fm.Snapshot(); snap.Status != Pass {
		t.Fatalf("empty map status = %v, want pass", snap.Status)
	}

	fm.Fail("row-1", "checksum mismatch", map[string]any{"hash": "abc"})
	firstFailAt := now
	now = now.Add(30 * time.Second)
	fm.Fail("row-2", "missing field", nil)

	snap := fm.Snapshot()
	if snap.Status != Fail {
		t.Fatalf("two failures status = %v, want fail", snap.Status)
	}
	if snap.Reason != "2 failing" {
		t.Fatalf("reason = %q, want %q", snap.Reason, "2 failing")
	}
	if count, _ := snap.Data["count"].(int); count != 2 {
		t.Fatalf("data count = %v, want 2", snap.Data["count"])
	}
	items, _ := snap.Data["items"].(map[string]any)
	if items == nil || items["row-1"] == nil || items["row-2"] == nil {
		t.Fatalf("data items = %#v, want both rows present", snap.Data["items"])
	}

	now = now.Add(time.Minute)
	fm.Fail("row-1", "still bad", nil)
	snap = fm.Snapshot()
	items, _ = snap.Data["items"].(map[string]any)
	row1, _ := items["row-1"].(map[string]any)
	if since, _ := row1["since"].(time.Time); !since.Equal(firstFailAt) {
		t.Fatalf("row-1 since = %v, want preserved %v", since, firstFailAt)
	}
	if secs, _ := row1["secs_in_failure"].(int64); secs != 90 {
		t.Fatalf("row-1 secs_in_failure = %d, want 90", secs)
	}

	fm.Pass("row-1")
	if snap := fm.Snapshot(); snap.Status != Fail {
		t.Fatalf("one remaining failure status = %v, want fail", snap.Status)
	}

	fm.Pass("row-2")
	snap = fm.Snapshot()
	if snap.Status != Pass {
		t.Fatalf("cleared status = %v, want pass", snap.Status)
	}
	if snap.Reason != "" {
		t.Fatalf("cleared reason = %q, want empty", snap.Reason)
	}
	if snap.Data != nil {
		t.Fatalf("cleared data = %#v, want nil", snap.Data)
	}
}

func TestRegistryVersionOnStateDisplay(t *testing.T) {
	reg := NewRegistry(WithVersion("1.2.3"))
	doc := reg.StateDisplay()
	if doc.Version != "1.2.3" {
		t.Fatalf("doc version = %q, want %q", doc.Version, "1.2.3")
	}

	plain := NewRegistry()
	if got := plain.StateDisplay().Version; got != "" {
		t.Fatalf("default version = %q, want empty", got)
	}
}

func TestRegistryHealthIsWorstOfAllAndDisplayedFirst(t *testing.T) {
	reg := NewRegistry()

	passing := NewManualState("alpha")
	warning := NewManualState("zulu")
	warning.Warn("slow", nil)
	failing := NewManualState("delta")
	failing.Fail("boom", nil)
	informational := NewManualState("optional", WithImportance(Informational))
	informational.Down("offline", nil)

	for _, s := range []State{passing, warning, failing, informational} {
		if err := reg.Register(s); err != nil {
			t.Fatal(err)
		}
	}

	snaps := reg.Snapshot()
	if len(snaps) != 5 {
		t.Fatalf("snapshots len = %d, want 5 (health + 4 registered)", len(snaps))
	}
	health := snaps[0]
	if health.Name != "health" {
		t.Fatalf("first state = %q, want health", health.Name)
	}
	if health.Status != Fail {
		t.Fatalf("health status = %v, want fail (informational down capped at warn, delta is fail)", health.Status)
	}
	if got := health.Reason; got != "fail:delta warn:optional,zulu" {
		t.Fatalf("health reason = %q, want %q", got, "fail:delta warn:optional,zulu")
	}
	if got, _ := health.Data["pass"].(int); got != 1 {
		t.Fatalf("pass count = %v, want 1", health.Data["pass"])
	}
	if got, _ := health.Data["warn"].(int); got != 2 {
		t.Fatalf("warn count = %v, want 2 (zulu + capped optional)", health.Data["warn"])
	}
	if got, _ := health.Data["fail"].(int); got != 1 {
		t.Fatalf("fail count = %v, want 1", health.Data["fail"])
	}
	if _, present := health.Data["down"]; present {
		t.Fatalf("down key should be omitted when count is 0, got %v", health.Data["down"])
	}
}

func TestRegistryHealthAllPassEmptyReason(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewManualState("alpha")); err != nil {
		t.Fatal(err)
	}
	snap := reg.Snapshot()[0]
	if snap.Status != Pass {
		t.Fatalf("health status = %v, want pass", snap.Status)
	}
	if snap.Reason != "" {
		t.Fatalf("health reason = %q, want empty", snap.Reason)
	}
}
