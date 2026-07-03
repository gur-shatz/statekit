package storage

import (
	"testing"
	"time"
)

func TestMemoryChartStoreRangeAndBucket(t *testing.T) {
	chart := NewMemoryChartStore(time.Minute, 5)
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	chart.Record(t0.Add(10*time.Second), []TriggeringState{
		{Identity: "a", TargetKey: "issuer-east", Label: "issuer-east:api", Status: "warn"},
		{Identity: "b", TargetKey: "issuer-west", Label: "issuer-west:db", Status: "fail"},
	})
	chart.Record(t0.Add(30*time.Second), []TriggeringState{
		// Same identity, same bucket: merged, not double counted.
		{Identity: "a", TargetKey: "issuer-east", Label: "issuer-east:api", Status: "warn"},
	})
	chart.Record(t0.Add(time.Minute), []TriggeringState{
		{Identity: "a", TargetKey: "issuer-east", Label: "issuer-east:api", Status: "fail"},
	})

	buckets, err := chart.Range("fleet", t0, t0.Add(2*time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 {
		t.Fatalf("buckets len = %d, want 2", len(buckets))
	}
	if buckets[0].Counts["warn"] != 1 || buckets[0].Counts["fail"] != 1 {
		t.Fatalf("bucket 0 counts = %+v", buckets[0].Counts)
	}
	if buckets[1].Counts["fail"] != 1 || buckets[1].Counts["warn"] != 0 {
		t.Fatalf("bucket 1 counts = %+v", buckets[1].Counts)
	}

	scoped, err := chart.Range("target:issuer-west", t0, t0.Add(2*time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if scoped[0].Counts["fail"] != 1 || scoped[0].Counts["warn"] != 0 || scoped[1].Counts["fail"] != 0 {
		t.Fatalf("scoped counts = %+v", scoped)
	}

	contributors, err := chart.Bucket("fleet", t0.Add(45*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(contributors) != 2 || contributors[0].Label != "issuer-east:api" || contributors[1].Label != "issuer-west:db" {
		t.Fatalf("contributors = %+v", contributors)
	}

	if _, err := chart.Range("bogus", t0, t0.Add(time.Minute), 1); err == nil {
		t.Fatal("bad scope should error")
	}
	if _, err := chart.Range("fleet", t0, t0.Add(time.Minute), 0); err == nil {
		t.Fatal("zero buckets should error")
	}
	if _, err := chart.Range("fleet", t0, t0, 1); err == nil {
		t.Fatal("empty range should error")
	}
}

// The window is a hard bound: writing a bucket one full window later reuses
// the slot round-robin and the old bucket is gone.
func TestMemoryChartStoreRoundRobinOverwrite(t *testing.T) {
	const window = 3
	chart := NewMemoryChartStore(time.Minute, window)
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	chart.Record(t0, []TriggeringState{{Identity: "old", Label: "old", Status: "down"}})
	before, err := chart.Bucket("fleet", t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 {
		t.Fatalf("bucket before overwrite = %+v", before)
	}

	chart.Record(t0.Add(window*time.Minute), []TriggeringState{{Identity: "new", Label: "new", Status: "warn"}})

	after, err := chart.Bucket("fleet", t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("old bucket should be overwritten round-robin: %+v", after)
	}
	replaced, err := chart.Bucket("fleet", t0.Add(window*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(replaced) != 1 || replaced[0].Identity != "new" {
		t.Fatalf("new bucket = %+v", replaced)
	}
}

// An idle slot is also reset when its time passes with nothing triggering, so
// a recovered fleet stops reporting stale contributors.
func TestMemoryChartStoreEmptyRecordClearsBucket(t *testing.T) {
	chart := NewMemoryChartStore(time.Minute, 3)
	t0 := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	chart.Record(t0, []TriggeringState{{Identity: "a", Label: "a", Status: "warn"}})
	chart.Record(t0.Add(3*time.Minute), nil)

	stale, err := chart.Bucket("fleet", t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("stale bucket should be cleared by the empty record: %+v", stale)
	}
}
