package infopages

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/storage"
)

func TestHandlerRendersRegistryConfig(t *testing.T) {
	reg := statekit.NewRegistry(
		statekit.WithVersion("v1.2.3"),
		statekit.WithLabel("component", "issuer"),
	)
	state := statekit.NewManualState("database")
	state.Warn("slow", nil)
	if err := reg.Register(state); err != nil {
		t.Fatal(err)
	}
	requests := statekit.NewCounter("requests_total", "Total requests.")
	if err := reg.RegisterCollectors(requests); err != nil {
		t.Fatal(err)
	}

	handler := Handler(Options{
		Title:       "issuer",
		Registry:    reg,
		RegistryURL: "/statekit",
		GeneratedAt: fixedTime,
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/config", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := response.Body.String()
	for _, want := range []string{"issuer", "v1.2.3", "database", "warn", "requests_total", "/statekit/state"} {
		if !strings.Contains(body, want) {
			t.Fatalf("config page missing %q:\n%s", want, body)
		}
	}
}

func TestHandlerRendersStorageCounts(t *testing.T) {
	store := storage.NewMemoryStore()
	doc := statekit.StateDisplayDocument{
		Kind: statekit.StateDisplayKind,
		LabelPath: []statekit.StateDisplayLabel{
			{Name: "component", Value: "issuer"},
		},
		States: []statekit.Snapshot{
			{
				Name:       "database",
				Status:     statekit.Fail,
				Importance: statekit.Important,
				ChangedAt:  time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC),
			},
		},
	}
	if err := store.IngestDocument(nil, doc, time.Date(2026, 6, 9, 10, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	handler := Handler(Options{
		Storage:     store,
		APIURL:      "/api",
		GeneratedAt: fixedTime,
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/storage", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := response.Body.String()
	for _, want := range []string{"*storage.MemoryStore", "Current states", "Targets", "2026-06-09T10:01:00Z", "/api/state/current"} {
		if !strings.Contains(body, want) {
			t.Fatalf("storage page missing %q:\n%s", want, body)
		}
	}
}

func fixedTime() time.Time {
	return time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
}
