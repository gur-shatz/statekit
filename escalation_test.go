package statekit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestEscalationsBudgetAndAckClearsBufferedEvents(t *testing.T) {
	now := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	escalations := NewEscalations(
		WithEscalationClock(func() time.Time { return now }),
		WithEscalationPolicy(EscalationPolicy{MaxUnacknowledged: 1, TTL: time.Hour}),
	)

	first, ok := escalations.Start(context.Background(), EscalationSpec{Title: "checkout failed", Severity: Fail})
	if !ok {
		t.Fatal("first escalation rejected")
	}
	first.AddLog(context.Background(), now, "http", "500", map[string]any{"path": "/checkout"})
	if _, ok := escalations.Start(context.Background(), EscalationSpec{Title: "second"}); ok {
		t.Fatal("second unacknowledged escalation accepted over budget")
	}

	doc := escalations.EscalationDisplay("", "")
	if !strings.HasSuffix(doc.Watermark, ":2") {
		t.Fatalf("watermark = %q, want suffix :2", doc.Watermark)
	}
	if len(doc.Incidents) != 1 || len(doc.Incidents[0].Events) != 2 {
		t.Fatalf("incident doc = %+v", doc.Incidents)
	}

	escalations.EscalationDisplay("", doc.Watermark)
	afterAck := escalations.EscalationDisplay("", "")
	if len(afterAck.Incidents) != 0 {
		t.Fatalf("ack did not clear incident from local export: %+v", afterAck.Incidents)
	}
	if _, ok := escalations.Start(context.Background(), EscalationSpec{Title: "second"}); !ok {
		t.Fatal("second escalation rejected after ack")
	}
}

func TestEscalationsIgnoreOldCursorAfterRestartEpochChanges(t *testing.T) {
	firstTime := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	first := NewEscalations(
		WithEscalationClock(func() time.Time { return firstTime }),
		WithEscalationPolicy(EscalationPolicy{TTL: time.Hour}),
	)
	if _, ok := first.Start(context.Background(), EscalationSpec{Title: "before restart"}); !ok {
		t.Fatal("first escalation rejected")
	}
	oldWatermark := first.EscalationDisplay("", "").Watermark

	secondTime := firstTime.Add(time.Second)
	second := NewEscalations(
		WithEscalationClock(func() time.Time { return secondTime }),
		WithEscalationPolicy(EscalationPolicy{TTL: time.Hour}),
	)
	if _, ok := second.Start(context.Background(), EscalationSpec{Title: "after restart"}); !ok {
		t.Fatal("second escalation rejected")
	}

	doc := second.EscalationDisplay(oldWatermark, oldWatermark)
	if len(doc.Incidents) != 1 {
		t.Fatalf("new epoch incident hidden by old cursor: %+v", doc)
	}
	if doc.Incidents[0].ID != "ID-0000000001" || doc.Incidents[0].Title != "after restart" {
		t.Fatalf("incident = %+v", doc.Incidents[0])
	}
}

func TestRegistryEscalationHandlerUsesAckQuery(t *testing.T) {
	escalations := NewEscalations(WithEscalationPolicy(EscalationPolicy{TTL: time.Hour}))
	incident, ok := escalations.Start(context.Background(), EscalationSpec{Title: "support case"})
	if !ok {
		t.Fatal("escalation rejected")
	}
	incident.AddData(context.Background(), "metadata", map[string]any{"request_id": "req-1"})

	reg := NewRegistry(WithLabel("component", "issuer"))
	reg.RegisterEscalations(escalations)
	req := httptest.NewRequest(http.MethodGet, "/escalations?ack=2", nil)
	rec := httptest.NewRecorder()
	reg.EscalationHandler().ServeHTTP(rec, req)

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("cache-control = %q, want no-store", got)
	}
	var doc EscalationDisplayDocument
	if err := yaml.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if doc.Kind != EscalationDisplayKind || len(doc.LabelPath) != 1 || doc.LabelPath[0].Value != "issuer" {
		t.Fatalf("handler doc = %+v", doc)
	}
	if len(doc.Incidents) != 0 {
		t.Fatalf("ack query did not clear incident from local export: %+v", doc.Incidents)
	}
}
