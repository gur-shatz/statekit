package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

func TestMemoryStoreIngestsCurrentLabelsAndGroups(t *testing.T) {
	store := NewMemoryStore()
	observedAt := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	if err := store.IngestDocument(context.Background(), testDocument(), observedAt); err != nil {
		t.Fatal(err)
	}

	current, err := store.Current(context.Background(), CurrentFilter{
		Labels: map[string]string{"region": "us-east-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 {
		t.Fatalf("current len = %d, want 2", len(current))
	}
	for _, item := range current {
		if item.Labels["service"] != "checkout" {
			t.Fatalf("labels = %+v", item.Labels)
		}
		if item.ScrapedFrom != "issuer-east" || item.ScrapePath != "scraper > issuer-east" {
			t.Fatalf("%s scrape metadata = %q %q", item.Name, item.ScrapedFrom, item.ScrapePath)
		}
		if item.GroupName != "payments" {
			t.Fatalf("group_name = %q", item.GroupName)
		}
		if item.Observation.ObservedAt != observedAt {
			t.Fatalf("observed_at = %v", item.Observation.ObservedAt)
		}
		if item.Observation.UpdatedAt.IsZero() {
			t.Fatalf("updated_at missing from observation: %+v", item.Observation)
		}
	}

	groups, err := store.Groups(context.Background(), GroupQuery{
		By: []string{"group_name", "label:region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups len = %d, want 1: %+v", len(groups), groups)
	}
	if groups[0].Values["group_name"] != "payments" || groups[0].Values["label:region"] != "us-east-1" {
		t.Fatalf("group values = %+v", groups[0].Values)
	}
	if groups[0].StatusCounts["warn"] != 1 || groups[0].StatusCounts["pass"] != 1 {
		t.Fatalf("status counts = %+v", groups[0].StatusCounts)
	}
	if groups[0].WorstStatus != "warn" {
		t.Fatalf("worst status = %q", groups[0].WorstStatus)
	}
}

func TestMemoryStorePrunesVanishedStatesAndTargets(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	if err := store.IngestDocument(ctx, testDocument(), t0); err != nil {
		t.Fatal(err)
	}
	before, err := store.Current(ctx, CurrentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 2 {
		t.Fatalf("initial current len = %d, want 2", len(before))
	}
	beforeTargets, err := store.Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeTargets) != 1 {
		t.Fatalf("initial targets len = %d, want 1", len(beforeTargets))
	}

	empty := statekit.StateDisplayDocument{
		Kind:      statekit.StateDisplayKind,
		LabelPath: testDocument().LabelPath,
	}
	if err := store.IngestDocument(ctx, empty, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	after, err := store.Current(ctx, CurrentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("current after empty ingest = %d, want 0 (states vanished): %+v", len(after), after)
	}
	afterTargets, err := store.Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterTargets) != 0 {
		t.Fatalf("targets after empty ingest = %d, want 0: %+v", len(afterTargets), afterTargets)
	}

	events, err := store.Events(ctx, EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events after vanishing = %d, want 2 (history retained)", len(events))
	}
}

func TestMemoryStoreScopesPruneToDocumentKey(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	docA := testDocument()
	docB := testDocument()
	docB.LabelPath = []statekit.StateDisplayLabel{{Name: "service", Value: "billing"}}
	docB.States[0].Name = "billing-api"
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

	current, err := store.Current(ctx, CurrentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 {
		t.Fatalf("current len = %d, want 2 (only docA pruned, docB intact)", len(current))
	}
	for _, item := range current {
		if item.Labels["service"] != "billing" {
			t.Fatalf("survived state from wrong scope: %+v", item.Labels)
		}
	}
}

func TestMemoryStoreEventsAreDedupedByTransition(t *testing.T) {
	store := NewMemoryStore()
	doc := testDocument()
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	events, err := store.Events(context.Background(), EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events len = %d, want one per state transition", len(events))
	}
}

func TestMemoryStoreBuildsTargetDocuments(t *testing.T) {
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
	if len(target.States) != 1 || target.States[0].Name != "checkout-api" {
		t.Fatalf("states = %+v", target.States)
	}
	if target.States[0].Data["latency_ms"] != 482 {
		t.Fatalf("state data = %+v", target.States[0].Data)
	}
	if len(target.States[0].Checks) != 1 || target.States[0].Checks[0].Name != "database" {
		t.Fatalf("checks = %+v", target.States[0].Checks)
	}

	doc.States[0].ChangedSecsAgo += 10
	if err := store.IngestDocument(context.Background(), doc, time.Date(2026, 5, 16, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	targets, err = store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if targets[0].MaterialHash != target.MaterialHash {
		t.Fatalf("material hash changed for volatile changed_secs_ago: %q != %q", targets[0].MaterialHash, target.MaterialHash)
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

func TestAPIExposesCurrentGroupsEventsAndOpenAPI(t *testing.T) {
	store := NewMemoryStore()
	if err := store.IngestDocument(context.Background(), testDocument(), time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewAPI(store).Handler())
	defer server.Close()

	currentResp, err := http.Get(server.URL + "/state/current?label.region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer currentResp.Body.Close()
	var current []CurrentState
	if err := json.NewDecoder(currentResp.Body).Decode(&current); err != nil {
		t.Fatal(err)
	}
	if len(current) != 2 {
		t.Fatalf("api current len = %d", len(current))
	}

	groupResp, err := http.Get(server.URL + "/state/groups?by=label:region")
	if err != nil {
		t.Fatal(err)
	}
	defer groupResp.Body.Close()
	var groups []GroupBucket
	if err := json.NewDecoder(groupResp.Body).Decode(&groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Values["label:region"] != "us-east-1" {
		t.Fatalf("api groups = %+v", groups)
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
	current, err := store.Current(context.Background(), CurrentFilter{Status: "warn"})
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || current[0].Name != "database" {
		t.Fatalf("current = %+v", current)
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
	var targets []TargetDocument
	if err := json.NewDecoder(resp.Body).Decode(&targets); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Name != "issuer-east" {
		t.Fatalf("targets = %+v", targets)
	}
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
