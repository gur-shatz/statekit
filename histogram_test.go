package statekit

import (
	"math"
	"testing"
)

func TestHistogramSnapshotPercentagesAndFiltering(t *testing.T) {
	h := NewHistogram()
	h.Add("ok", 80)
	h.Add("not_found", 15)
	h.Add("error", 5)
	h.Add("ok", 20)

	snap := h.Snapshot()
	if snap.Total != 120 {
		t.Fatalf("total = %f, want 120", snap.Total)
	}
	if snap.Count != 4 {
		t.Fatalf("count = %d, want 4", snap.Count)
	}

	top := snap.Top(1)
	if len(top) != 1 || top[0].Key != "ok" || top[0].Value != 100 {
		t.Fatalf("top = %+v, want ok bucket", top)
	}
	if math.Abs(top[0].Percentage-100.0/120.0*100) > 0.000001 {
		t.Fatalf("top percentage = %f", top[0].Percentage)
	}

	topPercent := snap.TopPercent(90)
	if len(topPercent) != 2 || topPercent[0].Key != "ok" || topPercent[1].Key != "not_found" {
		t.Fatalf("top percent = %+v, want ok and not_found", topPercent)
	}

	bottom := snap.Bottom(1)
	if len(bottom) != 1 || bottom[0].Key != "error" {
		t.Fatalf("bottom = %+v, want error bucket", bottom)
	}
}

func TestHistogramPercentile(t *testing.T) {
	h := NewHistogram()
	for _, value := range []float64{10, 20, 30, 40, 50} {
		h.Add("latency", value)
	}

	if got := h.Percentile(0.5); got != 30 {
		t.Fatalf("p50 = %f, want 30", got)
	}
	if got := h.Percentile(90); got != 46 {
		t.Fatalf("p90 = %f, want 46", got)
	}
}
