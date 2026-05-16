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

func testDocument() statekit.StateDisplayDocument {
	changedAt := time.Date(2026, 5, 16, 11, 59, 0, 0, time.UTC)
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
			ChangedAt:      changedAt,
			ChangedSecsAgo: 60,
			Data: map[string]any{
				"labels": map[string]any{
					"group_name": "payments",
					"region":     "us-east-1",
				},
			},
			Checks: []statekit.Snapshot{{
				Name:           "database",
				Status:         statekit.Pass,
				Importance:     statekit.Important,
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
