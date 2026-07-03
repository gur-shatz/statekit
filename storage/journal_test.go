package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
)

// recentDocument is testDocument with timestamps near now, so journal
// retention (relative to wall clock) keeps its identities on replay.
func recentDocument(now time.Time) statekit.StateDisplayDocument {
	doc := testDocument()
	doc.States[0].ChangedAt = now.Add(-time.Minute)
	doc.States[0].UpdatedAt = now.Add(-30 * time.Second)
	doc.States[0].Checks[0].ChangedAt = now.Add(-time.Minute)
	doc.States[0].Checks[0].UpdatedAt = now.Add(-30 * time.Second)
	return doc
}

func TestJournalRestoresHistoryAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.ndjson")
	ctx := context.Background()
	now := time.Now()

	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore(WithJournal(journal))
	doc := recentDocument(now)
	if err := store.IngestDocument(ctx, doc, now); err != nil {
		t.Fatal(err)
	}
	doc.States[0].Status = statekit.Fail
	doc.States[0].ChangedAt = now.Add(time.Minute)
	if err := store.IngestDocument(ctx, doc, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestEscalations(ctx, "issuer-east", statekit.EscalationDisplayDocument{
		Kind: statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{{
			ID:            "INC-1",
			Title:         "checkout failed",
			Status:        statekit.EscalationOpen,
			CreatedAt:     now,
			LastUpdatedAt: now,
			Events: []statekit.EscalationEvent{
				{Seq: "1", Timestamp: now, Topic: "incident", Message: "created"},
			},
		}},
	}, now); err != nil {
		t.Fatal(err)
	}
	identity := identityByName(t, store, "checkout-api")
	if err := journal.Err(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh store on the same journal: the current layers are empty until
	// the next scrape, but the history is back.
	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewMemoryStore(WithJournal(reopened))
	timeline, err := restarted.StateTimeline(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Transitions) != 2 {
		t.Fatalf("replayed transitions = %+v, want warn+fail", timeline.Transitions)
	}
	if timeline.Transitions[0].Status != "fail" || timeline.Transitions[1].Status != "warn" {
		t.Fatalf("replayed order = %+v", timeline.Transitions)
	}
	incidents, err := restarted.Incidents(ctx, IncidentFilter{Source: "issuer-east", ID: "INC-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 || len(incidents[0].Events) != 1 {
		t.Fatalf("replayed incidents = %+v", incidents)
	}
	if err := restarted.AcknowledgeIncident(ctx, "issuer-east", "INC-1", now.Add(time.Hour)); err != nil {
		t.Fatalf("replayed incident index broken: %v", err)
	}
}

func TestJournalReplayKeepsBoundsAndDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.ndjson")
	ctx := context.Background()
	now := time.Now()

	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore(WithJournal(journal))
	doc := recentDocument(now)
	// Observation churn must not grow the journal's replayed state either.
	for i := 0; i < 5; i++ {
		doc.States[0].Reason = "attempt"
		doc.States[0].UpdatedAt = now.Add(time.Duration(i) * time.Second)
		if err := store.IngestDocument(ctx, doc, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	journal.Close()

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewMemoryStore(WithJournal(reopened), WithTransitionRing(1, 100))
	identity := identityByName(t, store, "checkout-api")
	timeline, err := restarted.StateTimeline(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Transitions) != 1 {
		t.Fatalf("replay must respect the restarted store's ring cap: %+v", timeline.Transitions)
	}
}

func TestJournalRetentionDropsStaleIdentities(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.ndjson")
	ctx := context.Background()
	now := time.Now()

	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore(WithJournal(journal))
	doc := recentDocument(now)
	doc.States[0].ChangedAt = now.Add(-48 * time.Hour)
	doc.States[0].Checks[0].ChangedAt = now.Add(-time.Minute)
	if err := store.IngestDocument(ctx, doc, now); err != nil {
		t.Fatal(err)
	}
	stale := identityByName(t, store, "checkout-api")
	fresh := identityByName(t, store, "database")
	journal.Close()

	reopened, err := OpenJournal(path, WithJournalRetention(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewMemoryStore(WithJournal(reopened))
	if _, err := restarted.StateTimeline(ctx, stale); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale identity should be dropped on replay, err = %v", err)
	}
	if _, err := restarted.StateTimeline(ctx, fresh); err != nil {
		t.Fatalf("fresh identity should replay: %v", err)
	}
}

func TestJournalCompactsWhenOversized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.ndjson")
	ctx := context.Background()
	now := time.Now()

	journal, err := OpenJournal(path, WithJournalMaxSize(1))
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore(WithJournal(journal))
	doc := recentDocument(now)
	for i := 0; i < 4; i++ {
		doc.States[0].Status = []statekit.Status{statekit.Warn, statekit.Fail}[i%2]
		doc.States[0].ChangedAt = now.Add(time.Duration(i) * time.Minute)
		if err := store.IngestDocument(ctx, doc, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Err(); err != nil {
		t.Fatal(err)
	}
	journal.Close()

	// The compacted file must still fully rehydrate a fresh store.
	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewMemoryStore(WithJournal(reopened))
	identity := identityByName(t, store, "checkout-api")
	timeline, err := restarted.StateTimeline(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Transitions) != 4 {
		t.Fatalf("compacted journal lost transitions: %+v", timeline.Transitions)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("journal empty after compaction")
	}
}
