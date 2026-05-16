package statekit

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	if len(snap.History) != 2 {
		t.Fatalf("history len = %d, want 2", len(snap.History))
	}
	if snap.History[0].Status != Pass || snap.History[1].Status != Fail {
		t.Fatalf("history states = %+v", snap.History)
	}
	if snap.History[0].SecsAgo != 0 || snap.History[1].SecsAgo != 0 {
		t.Fatalf("history secs ago = %+v, want zero at fixed clock", snap.History)
	}
}

func TestManualStateHistoryReportsSecondsAgo(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s := NewManualState("database", WithClock(func() time.Time { return now }))

	now = now.Add(2 * time.Second)
	s.Warn("slow", nil)
	now = now.Add(3 * time.Second)
	snap := s.Snapshot()

	if len(snap.History) != 2 {
		t.Fatalf("history len = %d, want 2", len(snap.History))
	}
	if got := snap.History[0].SecsAgo; got != 5 {
		t.Fatalf("initial history secs ago = %v, want 5", got)
	}
	if got := snap.History[1].SecsAgo; got != 3 {
		t.Fatalf("warn history secs ago = %v, want 3", got)
	}
}

func TestManualStateClearsReasonForPass(t *testing.T) {
	s := NewManualState("database")
	s.Pass("connected", nil)

	snap := s.Snapshot()
	if snap.Reason != "" {
		t.Fatalf("pass reason = %q, want empty", snap.Reason)
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
	if len(doc.States) != 1 || doc.States[0].Status != Fail {
		t.Fatalf("states = %+v", doc.States)
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
