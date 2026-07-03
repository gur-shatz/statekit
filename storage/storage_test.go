package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

func TestMemoryStoreIngestsLabelsAndDetails(t *testing.T) {
	store := NewMemoryStore()
	observedAt := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	if err := store.IngestDocument(context.Background(), testDocument(), observedAt); err != nil {
		t.Fatal(err)
	}

	targets, err := store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets len = %d, want 1", len(targets))
	}
	detail, err := store.TargetDetail(context.Background(), targets[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Details) != 2 {
		t.Fatalf("details len = %d, want 2", len(detail.Details))
	}
	for _, item := range detail.Details {
		if item.Labels["service"] != "checkout" || item.Labels["region"] != "us-east-1" {
			t.Fatalf("labels = %+v", item.Labels)
		}
		if item.ScrapedFrom != "issuer-east" || item.ScrapePath != "scraper > issuer-east" {
			t.Fatalf("%s scrape metadata = %q %q", item.Name, item.ScrapedFrom, item.ScrapePath)
		}
		if item.GroupName != "payments" {
			t.Fatalf("group_name = %q", item.GroupName)
		}
		if item.ObservedAt != observedAt {
			t.Fatalf("observed_at = %v", item.ObservedAt)
		}
		if item.UpdatedAt.IsZero() {
			t.Fatalf("updated_at missing from detail: %+v", item)
		}
		if item.TargetKey != targets[0].Key {
			t.Fatalf("target_key = %q, want %q", item.TargetKey, targets[0].Key)
		}
	}
}

func TestMemoryStorePrunesVanishedStatesAndTargets(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	if err := store.IngestDocument(ctx, testDocument(), t0); err != nil {
		t.Fatal(err)
	}
	before, err := store.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stateCount(before) != 2 || before.Targets.Total != 1 {
		t.Fatalf("initial summary = %+v, want 2 states in 1 target", before)
	}
	identity := identityByName(t, store, "checkout-api")

	empty := statekit.StateDisplayDocument{
		Kind:      statekit.StateDisplayKind,
		LabelPath: testDocument().LabelPath,
	}
	if err := store.IngestDocument(ctx, empty, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	after, err := store.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stateCount(after) != 0 || after.Targets.Total != 0 {
		t.Fatalf("summary after empty ingest = %+v, want everything pruned", after)
	}
	if _, err := store.StateTimeline(ctx, identity); !errors.Is(err, ErrNotFound) {
		t.Fatalf("timeline after vanishing = %v, want ErrNotFound (rings pruned with their identities)", err)
	}
}

func stateCount(summary FleetSummary) int {
	total := 0
	for _, count := range summary.StatusCounts {
		total += count
	}
	return total
}

func TestMemoryStoreScopesPruneToDocumentKey(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	docA := testDocument()
	docB := testDocument()
	docB.LabelPath = []statekit.StateDisplayLabel{{Name: "service", Value: "billing"}}
	docB.States[0].Name = "billing-api"
	docB.States[0].ScrapedFrom = "issuer-west"
	docB.States[0].ScrapePath = "scraper > issuer-west"
	docB.States[0].Checks[0].Name = "billing-db"

	if err := store.IngestDocument(ctx, docA, t0); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestDocument(ctx, docB, t0); err != nil {
		t.Fatal(err)
	}

	emptyA := statekit.StateDisplayDocument{Kind: statekit.StateDisplayKind, LabelPath: docA.LabelPath}
	if err := store.IngestDocument(ctx, emptyA, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	survivors := allDetails(t, store)
	if len(survivors) != 2 {
		t.Fatalf("details len = %d, want 2 (only docA pruned, docB intact): %+v", len(survivors), survivors)
	}
	for _, item := range survivors {
		if item.Labels["service"] != "billing" {
			t.Fatalf("survived state from wrong scope: %+v", item.Labels)
		}
	}
}

// The leak fixed by the redesign: repeated scrapes of a state whose status
// has not changed must not grow L3, no matter how updated_at, reason, or
// data churn between scrapes.
func TestMemoryStoreTransitionsIgnoreObservationChurn(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	doc := testDocument()
	for i := 0; i < 10; i++ {
		doc.States[0].UpdatedAt = t0.Add(time.Duration(i) * time.Second)
		doc.States[0].Reason = fmt.Sprintf("database: slow (attempt %d)", i)
		doc.States[0].Data["latency_ms"] = 400 + i
		doc.States[0].Checks[0].UpdatedAt = doc.States[0].UpdatedAt
		if err := store.IngestDocument(ctx, doc, t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	total := totalTransitions(t, store)
	if total != 2 {
		t.Fatalf("transitions = %d, want one per state regardless of churn", total)
	}
}

// allDetails lists every stored state detail via the layer reads.
func allDetails(t *testing.T, store *MemoryStore) []StateDetail {
	t.Helper()
	targets, err := store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []StateDetail
	for _, target := range targets {
		detail, err := store.TargetDetail(context.Background(), target.Key)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, detail.Details...)
	}
	return out
}

func totalTransitions(t *testing.T, store *MemoryStore) int {
	t.Helper()
	total := 0
	for _, detail := range allDetails(t, store) {
		timeline, err := store.StateTimeline(context.Background(), detail.Identity)
		if err != nil {
			t.Fatal(err)
		}
		total += len(timeline.Transitions)
	}
	return total
}

func TestMemoryStoreConsumesSnapshotHistory(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	changedAt := time.Date(2026, 5, 16, 11, 59, 0, 0, time.UTC)

	doc := testDocument()
	doc.States[0].History = []statekit.HistoryEntry{
		{Timestamp: changedAt, Status: statekit.Warn, Reason: "database: slow"},
		{Timestamp: changedAt.Add(-time.Minute), Status: statekit.Fail, Reason: "db down"},
		{Timestamp: changedAt.Add(-2 * time.Minute), Status: statekit.Pass},
	}
	if err := store.IngestDocument(ctx, doc, changedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	identity := identityByName(t, store, "checkout-api")
	timeline, err := store.StateTimeline(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Transitions) != 3 {
		t.Fatalf("transitions len = %d, want 3 (history + head deduped): %+v", len(timeline.Transitions), timeline.Transitions)
	}
	if timeline.Transitions[0].Status != "warn" || timeline.Transitions[1].Status != "fail" || timeline.Transitions[2].Status != "pass" {
		t.Fatalf("transitions not newest-first: %+v", timeline.Transitions)
	}
	if timeline.Transitions[1].Reason != "db down" {
		t.Fatalf("transition reason = %q", timeline.Transitions[1].Reason)
	}
}

func TestMemoryStoreEnforcesTransitionRingCap(t *testing.T) {
	store := NewMemoryStore(WithTransitionRing(2, 1000))
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	doc := testDocument()
	statuses := []statekit.Status{statekit.Warn, statekit.Fail, statekit.Warn, statekit.Down}
	for i, status := range statuses {
		doc.States[0].Status = status
		doc.States[0].ChangedAt = t0.Add(time.Duration(i) * time.Minute)
		if err := store.IngestDocument(ctx, doc, t0.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}

	identity := identityByName(t, store, "checkout-api")
	timeline, err := store.StateTimeline(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Transitions) != 2 {
		t.Fatalf("ring len = %d, want cap 2: %+v", len(timeline.Transitions), timeline.Transitions)
	}
	if timeline.Transitions[0].Status != "down" || timeline.Transitions[1].Status != "warn" {
		t.Fatalf("ring should keep the newest transitions: %+v", timeline.Transitions)
	}
}

func TestMemoryStoreEnforcesTransitionBackstop(t *testing.T) {
	store := NewMemoryStore(WithTransitionRing(32, 2))
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	doc := testDocument()
	if err := store.IngestDocument(ctx, doc, t0); err != nil {
		t.Fatal(err)
	}
	doc.States[0].Status = statekit.Fail
	doc.States[0].ChangedAt = t0.Add(time.Minute)
	doc.States[0].Checks[0].Status = statekit.Warn
	doc.States[0].Checks[0].ChangedAt = t0.Add(time.Minute)
	if err := store.IngestDocument(ctx, doc, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	if total := totalTransitions(t, store); total != 2 {
		t.Fatalf("total transitions = %d, want global backstop 2", total)
	}
	for _, detail := range allDetails(t, store) {
		timeline, err := store.StateTimeline(ctx, detail.Identity)
		if err != nil {
			t.Fatal(err)
		}
		for _, transition := range timeline.Transitions {
			if !transition.ChangedAt.Equal(t0.Add(time.Minute)) {
				t.Fatalf("backstop should evict the oldest transitions first: %+v", timeline.Transitions)
			}
		}
	}
}

func TestMemoryStoreBuildsTargetSummaries(t *testing.T) {
	store := NewMemoryStore()
	doc := testDocument()
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	targets, err := store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets len = %d, want 1: %+v", len(targets), targets)
	}
	target := targets[0]
	if target.Name != "issuer-east" || target.ScrapePath != "scraper > issuer-east" {
		t.Fatalf("target identity = %q %q", target.Name, target.ScrapePath)
	}
	if target.WorstStatus != "warn" || target.StatusCounts["warn"] != 1 {
		t.Fatalf("summary = worst %q counts %+v", target.WorstStatus, target.StatusCounts)
	}
	if len(target.AffectedStates) != 1 || target.AffectedStates[0].Name != "checkout-api" {
		t.Fatalf("affected = %+v", target.AffectedStates)
	}
	if len(target.States) != 2 {
		t.Fatalf("headers should be flat (state + check) = %+v", target.States)
	}
	if target.States[0].Name != "checkout-api" || target.States[1].Name != "database" {
		t.Fatalf("headers = %+v", target.States)
	}
	if target.States[1].ParentIdentity != target.States[0].Identity {
		t.Fatalf("check header should point at its parent: %+v", target.States)
	}

	detail, err := store.TargetDetail(context.Background(), target.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Details) != 2 {
		t.Fatalf("details len = %d, want 2", len(detail.Details))
	}
	if detail.Details[0].Data["latency_ms"] != 482 {
		t.Fatalf("detail data = %+v", detail.Details[0].Data)
	}

	doc.States[0].ChangedSecsAgo += 10
	doc.States[0].UpdatedAt = doc.States[0].UpdatedAt.Add(10 * time.Second)
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	targets, err = store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if targets[0].MaterialHash != target.MaterialHash {
		t.Fatalf("material hash changed for volatile fields: %q != %q", targets[0].MaterialHash, target.MaterialHash)
	}
}

func TestMemoryStoreSummaryAndFleetHash(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	doc := testDocument()
	if err := store.IngestDocument(ctx, doc, t0); err != nil {
		t.Fatal(err)
	}
	summary, err := store.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.WorstStatus != "warn" {
		t.Fatalf("worst = %q", summary.WorstStatus)
	}
	if summary.StatusCounts["warn"] != 1 || summary.StatusCounts["pass"] != 1 {
		t.Fatalf("status counts = %+v", summary.StatusCounts)
	}
	if summary.Targets.Total != 1 || summary.Targets.ByStatus["warn"] != 1 {
		t.Fatalf("target counts = %+v", summary.Targets)
	}
	if summary.FleetHash == "" || summary.ObservedAt != t0 {
		t.Fatalf("rollup = hash %q observed %v", summary.FleetHash, summary.ObservedAt)
	}

	// A scrape that changes nothing material keeps the fleet hash stable.
	doc.States[0].UpdatedAt = doc.States[0].UpdatedAt.Add(time.Second)
	doc.States[0].ChangedSecsAgo += 1
	if err := store.IngestDocument(ctx, doc, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	unchanged, err := store.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.FleetHash != summary.FleetHash {
		t.Fatal("fleet hash changed without material change")
	}

	doc.States[0].Status = statekit.Fail
	doc.States[0].ChangedAt = t0.Add(time.Minute)
	if err := store.IngestDocument(ctx, doc, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	changed, err := store.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changed.FleetHash == summary.FleetHash {
		t.Fatal("fleet hash did not change on status change")
	}
	if changed.WorstStatus != "fail" {
		t.Fatalf("worst = %q", changed.WorstStatus)
	}
}

func TestMemoryStoreStateDetailAndChildren(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.IngestDocument(ctx, testDocument(), time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	parent := identityByName(t, store, "checkout-api")
	child := identityByName(t, store, "database")

	detail, err := store.StateDetail(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Data["latency_ms"] != 482 || detail.DataHash == "" {
		t.Fatalf("detail data = %+v hash %q", detail.Data, detail.DataHash)
	}
	if len(detail.Children) != 1 || detail.Children[0] != child {
		t.Fatalf("children = %+v, want [%s]", detail.Children, child)
	}

	if _, err := store.StateDetail(ctx, "no-such-identity"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if _, err := store.TargetDetail(ctx, "no-such-target"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if _, err := store.StateTimeline(ctx, "no-such-identity"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// A source that dies silently must not leave its states behind forever: its
// scope is evicted once it has missed ten of its own ingest intervals.
func TestMemoryStoreEvictsSilentSources(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	docA := testDocument()
	docB := testDocument()
	docB.LabelPath = []statekit.StateDisplayLabel{{Name: "service", Value: "billing"}}
	docB.States[0].Name = "billing-api"
	docB.States[0].ScrapedFrom = "issuer-west"
	docB.States[0].Checks = nil

	// docA ingests twice 15s apart, so its liveness TTL is 150s.
	if err := store.IngestDocument(ctx, docA, t0); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestDocument(ctx, docA, t0.Add(15*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestDocument(ctx, docB, t0.Add(15*time.Second)); err != nil {
		t.Fatal(err)
	}

	// docA goes silent; docB keeps ingesting past docA's TTL.
	if err := store.IngestDocument(ctx, docB, t0.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	survivors := allDetails(t, store)
	if len(survivors) != 1 || survivors[0].Name != "billing-api" {
		t.Fatalf("details after silent-source eviction = %+v, want only billing-api", survivors)
	}
	targets, err := store.Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Name != "issuer-west" {
		t.Fatalf("targets after silent-source eviction = %+v", targets)
	}
}

func TestMemoryStoreCachesZstdYAMLDocument(t *testing.T) {
	cache := NewFreecacheDocumentCache[statekit.StateDisplayDocument](1 << 20)
	store := NewMemoryStore(WithDocumentCache(cache, time.Minute))
	doc := testDocument()
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	data, ok, err := store.CachedDocumentYAML(context.Background(), StateDisplayDocumentKey(doc))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cached document missing")
	}
	text := string(data)
	if !strings.Contains(text, "kind: statekit.state.v1") || !strings.Contains(text, "checkout-api") {
		t.Fatalf("cached yaml = %s", text)
	}
	cachedDoc, ok, err := cache.Get(context.Background(), StateDisplayDocumentKey(doc))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || cachedDoc.States[0].Name != "checkout-api" {
		t.Fatalf("cached doc = %+v, ok=%v", cachedDoc, ok)
	}
}

func TestMemoryStoreIngestsEscalationsAndDedupesEvents(t *testing.T) {
	store := NewMemoryStore()
	observedAt := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	doc := statekit.EscalationDisplayDocument{
		Kind:      statekit.EscalationDisplayKind,
		LabelPath: []statekit.StateDisplayLabel{{Name: "component", Value: "issuer"}},
		Watermark: "2",
		Incidents: []statekit.EscalationIncident{{
			ID:            "ID-0000000001",
			ScrapedFrom:   "issuer-origin",
			ScrapePath:    "regional-east > issuer-origin",
			Type:          statekit.EscalationTypeRollback,
			Title:         "checkout failed",
			Status:        statekit.EscalationOpen,
			CreatedAt:     observedAt,
			ExpiresAt:     observedAt.Add(time.Hour),
			LastUpdatedAt: observedAt,
			Severity:      statekit.Fail,
			Labels:        map[string]string{"region": "use1", "subsystem": "support"},
			Topics:        map[string]any{"request_id": "req-1"},
			Events: []statekit.EscalationEvent{
				{Seq: "1", Timestamp: observedAt, Topic: "incident", Message: "created"},
				{Seq: "2", Timestamp: observedAt, Topic: "http", Message: "500"},
			},
		}},
	}

	if err := store.IngestEscalations(context.Background(), "issuer-east", doc, observedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestEscalations(context.Background(), "issuer-east", doc, observedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	incidents, err := store.Incidents(context.Background(), IncidentFilter{Source: "issuer-origin"})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incidents len = %d, want 1: %+v", len(incidents), incidents)
	}
	incident := incidents[0]
	if incident.Source != "issuer-origin" || incident.ScrapedFrom != "issuer-origin" || incident.ScrapePath != "regional-east > issuer-origin" {
		t.Fatalf("incident provenance = source %q scraped_from %q scrape_path %q", incident.Source, incident.ScrapedFrom, incident.ScrapePath)
	}
	if incident.Labels["component"] != "issuer" || incident.Labels["region"] != "use1" ||
		incident.Labels["subsystem"] != "support" || incident.Topics["request_id"] != "req-1" {
		t.Fatalf("incident metadata = labels %+v topics %+v", incident.Labels, incident.Topics)
	}
	if incident.Type != statekit.EscalationTypeRollback {
		t.Fatalf("incident type = %q, want %q", incident.Type, statekit.EscalationTypeRollback)
	}
	if byType, err := store.Incidents(context.Background(), IncidentFilter{Type: statekit.EscalationTypeBuild}); err != nil || len(byType) != 0 {
		t.Fatalf("type filter matched unexpectedly: %+v, err %v", byType, err)
	}
	if len(incident.Events) != 2 {
		t.Fatalf("events len = %d, want 2: %+v", len(incident.Events), incident.Events)
	}
	if incident.Events[0].Seq != "1" || incident.Events[1].Seq != "2" {
		t.Fatalf("events order = %+v", incident.Events)
	}

	if err := store.AcknowledgeIncident(context.Background(), "issuer-origin", "ID-0000000001", observedAt); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := store.Incidents(context.Background(), IncidentFilter{Status: statekit.EscalationAcknowledged})
	if err != nil {
		t.Fatal(err)
	}
	if len(acknowledged) != 1 {
		t.Fatalf("acknowledged incidents = %+v", acknowledged)
	}
}

func TestMemoryStoreKeepsReusedIncidentIDWithDifferentStartTime(t *testing.T) {
	store := NewMemoryStore()
	createdAt := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	doc := func(title string, t time.Time) statekit.EscalationDisplayDocument {
		return statekit.EscalationDisplayDocument{
			Kind: statekit.EscalationDisplayKind,
			Incidents: []statekit.EscalationIncident{{
				ID:            "ID-0000000001",
				Title:         title,
				Status:        statekit.EscalationOpen,
				CreatedAt:     t,
				ExpiresAt:     t.Add(time.Hour),
				LastUpdatedAt: t,
				Events: []statekit.EscalationEvent{
					{Seq: "epoch:1", Timestamp: t, Topic: "incident", Message: "created"},
				},
			}},
		}
	}

	if err := store.IngestEscalations(context.Background(), "issuer-east", doc("before restart", createdAt), createdAt); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestEscalations(context.Background(), "issuer-east", doc("after restart", createdAt.Add(time.Second)), createdAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	incidents, err := store.Incidents(context.Background(), IncidentFilter{Source: "issuer-east", ID: "ID-0000000001"})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 2 {
		t.Fatalf("incidents len = %d, want 2: %+v", len(incidents), incidents)
	}
	if incidents[0].Title != "after restart" || incidents[1].Title != "before restart" {
		t.Fatalf("incidents were merged incorrectly: %+v", incidents)
	}
}

func TestMemoryStoreIncidentRetention(t *testing.T) {
	store := NewMemoryStore(WithIncidentRetention(time.Hour, 2))
	ctx := context.Background()
	t0 := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)

	closed := statekit.EscalationDisplayDocument{
		Kind: statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{{
			ID:            "closed-1",
			Title:         "old deploy",
			Status:        statekit.EscalationClosed,
			CreatedAt:     t0.Add(-time.Hour),
			LastUpdatedAt: t0,
			Events: []statekit.EscalationEvent{
				{Seq: "1", Timestamp: t0, Topic: "deploy", Message: "started"},
				{Seq: "2", Timestamp: t0, Topic: "deploy", Message: "finished"},
				{Seq: "3", Timestamp: t0, Topic: "deploy", Message: "closed"},
			},
		}},
	}
	if err := store.IngestEscalations(ctx, "deployer", closed, t0); err != nil {
		t.Fatal(err)
	}

	incidents, err := store.Incidents(ctx, IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incidents = %+v", incidents)
	}
	if len(incidents[0].Events) != 2 || incidents[0].Events[0].Seq != "2" || incidents[0].Events[1].Seq != "3" {
		t.Fatalf("event ring should keep the newest 2: %+v", incidents[0].Events)
	}

	// Any later ingest past the TTL sweeps the closed incident away.
	open := statekit.EscalationDisplayDocument{
		Kind: statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{{
			ID:            "open-1",
			Title:         "new incident",
			Status:        statekit.EscalationOpen,
			CreatedAt:     t0.Add(2 * time.Hour),
			LastUpdatedAt: t0.Add(2 * time.Hour),
		}},
	}
	if err := store.IngestEscalations(ctx, "deployer", open, t0.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	incidents, err = store.Incidents(ctx, IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 || incidents[0].ID != "open-1" {
		t.Fatalf("closed incident should be swept after TTL: %+v", incidents)
	}
	if _, err := store.Incidents(ctx, IncidentFilter{Source: "deployer", ID: "closed-1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeIncident(ctx, "deployer", "closed-1", t0.Add(3*time.Hour)); err == nil {
		t.Fatal("acknowledging a swept incident should fail")
	}
}

func TestAPIRetiresPreLayerEndpointsAndServesOpenAPI(t *testing.T) {
	store := NewMemoryStore()
	if err := store.IngestDocument(context.Background(), testDocument(), time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	for _, retired := range []string{"/state/current", "/state/events", "/state/groups"} {
		resp, err := http.Get(server.URL + retired)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404 (retired)", retired, resp.StatusCode)
		}
	}

	openAPIResp, err := http.Get(server.URL + "/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer openAPIResp.Body.Close()
	if got := openAPIResp.Header.Get("Content-Type"); got != "text/yaml; charset=utf-8" {
		t.Fatalf("openapi content type = %q", got)
	}
}

func TestAPILayerEndpointsAndETagRoundTrips(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	doc := testDocument()
	if err := store.IngestDocument(ctx, doc, t0); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	get := func(path, ifNoneMatch string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Summary round-trip: 200 with ETag, then 304, then 200 after a change.
	resp := get("/state/summary", "")
	var summary FleetSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	etag := resp.Header.Get("ETag")
	if resp.StatusCode != http.StatusOK || etag == "" {
		t.Fatalf("summary status %d etag %q", resp.StatusCode, etag)
	}
	if summary.WorstStatus != "warn" {
		t.Fatalf("summary = %+v", summary)
	}

	resp = get("/state/summary", etag)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional summary status = %d, want 304", resp.StatusCode)
	}

	doc.States[0].Status = statekit.Fail
	doc.States[0].ChangedAt = t0.Add(time.Minute)
	if err := store.IngestDocument(ctx, doc, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	resp = get("/state/summary", etag)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") == etag {
		t.Fatalf("summary after change: status %d etag %q", resp.StatusCode, resp.Header.Get("ETag"))
	}

	// Target drill-down: the L1 material hash is the L2 ETag.
	targets, err := store.Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := "/state/targets/" + url.PathEscape(targets[0].Key)
	resp = get(targetPath, "")
	var detail TargetDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") != `"`+targets[0].MaterialHash+`"` {
		t.Fatalf("target detail status %d etag %q, want material hash", resp.StatusCode, resp.Header.Get("ETag"))
	}
	if len(detail.Details) != 2 || detail.Details[0].Data["latency_ms"] == nil {
		t.Fatalf("target detail = %+v", detail)
	}
	resp = get(targetPath, resp.Header.Get("ETag"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional target detail status = %d, want 304", resp.StatusCode)
	}

	// State detail and its timeline.
	identity := identityByName(t, store, "checkout-api")
	resp = get("/state/states/"+identity, "")
	var state StateDetail
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(state.Children) != 1 {
		t.Fatalf("state detail status %d body %+v", resp.StatusCode, state)
	}

	resp = get("/state/states/"+identity+"/timeline", "")
	var timeline StateTimeline
	if err := json.NewDecoder(resp.Body).Decode(&timeline); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(timeline.Transitions) != 2 {
		t.Fatalf("timeline status %d body %+v", resp.StatusCode, timeline)
	}

	resp = get("/state/states/no-such-identity", "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing state status = %d, want 404", resp.StatusCode)
	}
}

func TestAPIChartTimelineEndpoints(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	now := time.Now()
	doc := testDocument()
	doc.States[0].ChangedAt = now.Add(-time.Minute)
	if err := store.IngestDocument(ctx, doc, now); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/state/timeline?scope=fleet&window=30m&buckets=6")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buckets []BucketCounts
	if err := json.NewDecoder(resp.Body).Decode(&buckets); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") == "" {
		t.Fatalf("timeline status %d etag %q", resp.StatusCode, resp.Header.Get("ETag"))
	}
	if len(buckets) != 6 {
		t.Fatalf("buckets len = %d, want 6", len(buckets))
	}
	total := 0
	for _, bucket := range buckets {
		total += bucket.Counts["warn"]
	}
	if total != 1 {
		t.Fatalf("expected one warn bucket across the window: %+v", buckets)
	}

	bucketResp, err := http.Get(server.URL + "/state/timeline/bucket?t=" + url.QueryEscape(now.Format(time.RFC3339Nano)))
	if err != nil {
		t.Fatal(err)
	}
	defer bucketResp.Body.Close()
	var triggering []TriggeringState
	if err := json.NewDecoder(bucketResp.Body).Decode(&triggering); err != nil {
		t.Fatal(err)
	}
	if len(triggering) != 1 || triggering[0].Label != "issuer-east:checkout-api" || triggering[0].Status != "warn" {
		t.Fatalf("bucket contributors = %+v", triggering)
	}

	badResp, err := http.Get(server.URL + "/state/timeline?scope=bogus")
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad scope status = %d, want 400", badResp.StatusCode)
	}
}

func TestAPIExposesIncidentsAsYAML(t *testing.T) {
	store := NewMemoryStore()
	createdAt := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	if err := store.IngestEscalations(context.Background(), "issuer-east", statekit.EscalationDisplayDocument{
		Kind: statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{{
			ID:            "ID-0000000001",
			Title:         "checkout failed",
			Status:        statekit.EscalationOpen,
			CreatedAt:     createdAt,
			ExpiresAt:     createdAt.Add(time.Hour),
			LastUpdatedAt: createdAt,
			Events: []statekit.EscalationEvent{
				{Seq: "1", Timestamp: createdAt, Topic: "incident", Message: "created"},
			},
		}},
	}, createdAt); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/escalations/incidents")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "text/yaml; charset=utf-8" {
		t.Fatalf("content type = %q, want yaml", got)
	}
	var incidents []Incident
	if err := yaml.NewDecoder(resp.Body).Decode(&incidents); err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 || incidents[0].ID != "ID-0000000001" {
		t.Fatalf("incidents = %+v", incidents)
	}
	if len(incidents[0].Events) != 0 {
		t.Fatalf("incident list should be a summary without events: %+v", incidents[0].Events)
	}

	detailResp, err := http.Get(server.URL + "/escalations/incidents/issuer-east/ID-0000000001")
	if err != nil {
		t.Fatal(err)
	}
	defer detailResp.Body.Close()
	var incident Incident
	if err := json.NewDecoder(detailResp.Body).Decode(&incident); err != nil {
		t.Fatal(err)
	}
	if detailResp.StatusCode != http.StatusOK || incident.ID != "ID-0000000001" || len(incident.Events) != 1 {
		t.Fatalf("incident detail status %d = %+v", detailResp.StatusCode, incident)
	}

	missingResp, err := http.Get(server.URL + "/escalations/incidents/issuer-east/missing")
	if err != nil {
		t.Fatal(err)
	}
	missingResp.Body.Close()
	if missingResp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing incident status = %d, want 404", missingResp.StatusCode)
	}
}

func TestAPIIngestsYAMLDocument(t *testing.T) {
	store := NewMemoryStore()
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	resp, err := http.Post(server.URL+"/state/doc", "text/yaml", strings.NewReader(`
kind: statekit.state.v1
label_path:
  - name: service
    value: checkout
states:
  - name: database
    status: warn
    importance: important
    reason: slow
    changed_at: 2026-05-16T12:00:00Z
    changed_secs_ago: 3
`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	summary, err := store.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.StatusCounts["warn"] != 1 || summary.WorstStatus != "warn" {
		t.Fatalf("summary after ingest = %+v", summary)
	}
}

func TestAPIExposesTargets(t *testing.T) {
	store := NewMemoryStore()
	if err := store.IngestDocument(context.Background(), testDocument(), time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/state/targets")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var targets []TargetSummary
	if err := json.NewDecoder(resp.Body).Decode(&targets); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Name != "issuer-east" {
		t.Fatalf("targets = %+v", targets)
	}
	if resp.Header.Get("ETag") == "" {
		t.Fatal("targets response missing ETag")
	}
	body, err := json.Marshal(targets[0].States)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "latency_ms") {
		t.Fatalf("L1 headers must not carry data payloads: %s", body)
	}
}

func identityByName(t *testing.T, store *MemoryStore, name string) string {
	t.Helper()
	targets, err := store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		for _, header := range target.States {
			if header.Name == name {
				return header.Identity
			}
		}
	}
	t.Fatalf("state %q not found", name)
	return ""
}

func testDocument() statekit.StateDisplayDocument {
	changedAt := time.Date(2026, 5, 16, 11, 59, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 5, 16, 11, 59, 30, 0, time.UTC)
	return statekit.StateDisplayDocument{
		Kind: statekit.StateDisplayKind,
		LabelPath: []statekit.StateDisplayLabel{
			{Name: "service", Value: "checkout"},
		},
		States: []statekit.Snapshot{{
			Name:           "checkout-api",
			ScrapedFrom:    "issuer-east",
			ScrapePath:     "scraper > issuer-east",
			Status:         statekit.Warn,
			Importance:     statekit.Important,
			Reason:         "database: slow",
			UpdatedAt:      updatedAt,
			UpdatedSecsAgo: 30,
			ChangedAt:      changedAt,
			ChangedSecsAgo: 60,
			Data: map[string]any{
				"latency_ms": 482,
				"labels": map[string]any{
					"group_name": "payments",
					"region":     "us-east-1",
				},
			},
			Checks: []statekit.Snapshot{{
				Name:           "database",
				Status:         statekit.Pass,
				Importance:     statekit.Important,
				UpdatedAt:      updatedAt,
				UpdatedSecsAgo: 30,
				ChangedAt:      changedAt,
				ChangedSecsAgo: 60,
				Data: map[string]any{
					"labels": map[string]string{
						"group_name": "payments",
						"region":     "us-east-1",
					},
				},
			}},
		}},
	}
}

func TestAPIGlobalIncidentLifecycle(t *testing.T) {
	store := NewMemoryStore()
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	post := func(body map[string]any) (int, Incident) {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(server.URL+"/escalations/global", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var incident Incident
		if resp.StatusCode < 300 {
			if err := json.NewDecoder(resp.Body).Decode(&incident); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, incident
	}

	code, created := post(map[string]any{
		"type":    statekit.EscalationTypeDeployment,
		"title":   "deploy v1.2.3",
		"message": "rollout started",
		"labels":  map[string]string{"version": "v1.2.3"},
	})
	if code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", code, http.StatusCreated)
	}
	if created.Type != statekit.EscalationTypeDeployment || created.Status != statekit.EscalationOpen ||
		created.Source != "global" || created.Title != "deploy v1.2.3" || created.Labels["version"] != "v1.2.3" {
		t.Fatalf("created incident = %+v", created)
	}
	if len(created.Events) != 1 || created.Events[0].Message != "rollout started" {
		t.Fatalf("created events = %+v", created.Events)
	}

	code, annotated := post(map[string]any{"id": created.ID, "message": "50% rolled out"})
	if code != http.StatusOK || annotated.Status != statekit.EscalationOpen || len(annotated.Events) != 2 {
		t.Fatalf("annotate status = %d, incident = %+v", code, annotated)
	}

	code, closed := post(map[string]any{"id": created.ID, "status": statekit.EscalationClosed})
	if code != http.StatusOK || closed.Status != statekit.EscalationClosed || len(closed.Events) != 3 {
		t.Fatalf("close status = %d, incident = %+v", code, closed)
	}
	if closed.Events[2].Message != "closed" || closed.CreatedAt != created.CreatedAt {
		t.Fatalf("closed incident = %+v", closed)
	}

	if code, _ := post(map[string]any{"title": "missing type"}); code != http.StatusBadRequest {
		t.Fatalf("missing type status = %d, want %d", code, http.StatusBadRequest)
	}
	if code, _ := post(map[string]any{"id": "missing", "status": statekit.EscalationClosed}); code != http.StatusNotFound {
		t.Fatalf("unknown id status = %d, want %d", code, http.StatusNotFound)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/escalations/incidents?type=deployment", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("incidents content-type = %q, want application/json", got)
	}
	var listed []Incident
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed incidents = %+v", listed)
	}
}
