package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileChartStoreSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Truncate(time.Minute)

	chart, err := NewFileChartStore(dir, time.Minute, 60)
	if err != nil {
		t.Fatal(err)
	}
	chart.Record(now.Add(-2*time.Minute), []TriggeringState{
		{Identity: "a", TargetKey: "east", Label: "east:api", Status: "warn"},
	})
	chart.Record(now, []TriggeringState{
		{Identity: "b", TargetKey: "west", Label: "west:db", Status: "fail"},
	})
	if err := chart.Err(); err != nil {
		t.Fatal(err)
	}
	if err := chart.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileChartStore(dir, time.Minute, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	old, err := reopened.Bucket("fleet", now.Add(-2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 1 || old[0].Label != "east:api" {
		t.Fatalf("replayed bucket = %+v", old)
	}
	buckets, err := reopened.Range("fleet", now.Add(-5*time.Minute), now.Add(time.Minute), 6)
	if err != nil {
		t.Fatal(err)
	}
	warns, fails := 0, 0
	for _, bucket := range buckets {
		warns += bucket.Counts["warn"]
		fails += bucket.Counts["fail"]
	}
	if warns != 1 || fails != 1 {
		t.Fatalf("replayed range counts: %d warn %d fail, want 1/1: %+v", warns, fails, buckets)
	}
}

func TestFileChartStoreDeduplicatesAndSkipsEmpty(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Truncate(time.Minute)

	chart, err := NewFileChartStore(dir, time.Minute, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer chart.Close()

	triggering := []TriggeringState{{Identity: "a", Label: "east:api", Status: "warn"}}
	// A healthy fleet appends nothing; an unchanged degraded bucket appends once.
	chart.Record(now, nil)
	chart.Record(now.Add(10*time.Second), triggering)
	chart.Record(now.Add(20*time.Second), triggering)
	chart.Record(now.Add(30*time.Second), append(triggering, TriggeringState{Identity: "b", Label: "west:db", Status: "fail"}))
	if err := chart.Err(); err != nil {
		t.Fatal(err)
	}

	lines := 0
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		lines += strings.Count(string(data), "\n")
	}
	if lines != 2 {
		t.Fatalf("segment lines = %d, want 2 (one per distinct bucket content)", lines)
	}
}

func TestFileChartStoreSweepsExpiredSegments(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "chart-2020-01-01.ndjson")
	if err := os.WriteFile(stale, []byte(`{"t":"2020-01-01T00:00:00Z","states":[{"identity":"x","label":"x","status":"down"}]}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	chart, err := NewFileChartStore(dir, time.Minute, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer chart.Close()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("expired segment should be deleted, stat err = %v", err)
	}
	contributors, err := chart.Bucket("fleet", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(contributors) != 0 {
		t.Fatalf("expired data must not replay: %+v", contributors)
	}
}
