package counters

import (
	"slices"
	"testing"
)

func TestNewEpochWeightedCountersInitializesEpochAlignedStartAndWidth(t *testing.T) {
	c := NewEpochWeightedCounters(60, 123456789, 3)

	if got, want := c.start, uint32(123456780); got != want {
		t.Fatalf("start = %d, want %d", got, want)
	}
	if got, want := c.Len(), 3; got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
}

func TestNewEpochWeightedCountersPanicsForZeroSpan(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewEpochWeightedCounters did not panic")
		}
	}()

	NewEpochWeightedCounters(0, 0, 1)
}

func TestNewEpochWeightedCountersPanicsForNegativeWidth(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewEpochWeightedCounters did not panic")
		}
	}()

	NewEpochWeightedCounters(60, 0, -1)
}

func TestEpochWeightedCountersCurrentValueInterpolatesCurrentBucket(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 3)
	c.Add(0, []int64{120, 60, 30})

	tests := []struct {
		name      string
		timestamp uint32
		want      []int64
	}{
		{name: "start", timestamp: 0, want: []int64{0, 0, 0}},
		{name: "quarter", timestamp: 15, want: []int64{30, 15, 7}},
		{name: "half", timestamp: 30, want: []int64{60, 30, 15}},
		{name: "near end", timestamp: 59, want: []int64{118, 59, 29}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := make([]int64, c.Len())
			c.CurrentValue(tt.timestamp, got)

			if !slices.Equal(got, tt.want) {
				t.Fatalf("CurrentValue(%d) = %v, want %v", tt.timestamp, got, tt.want)
			}
		})
	}
}

func TestEpochWeightedCountersCurrentWindowValueCountsCurrentBucketImmediately(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)
	c.Add(0, []int64{120, 60})

	got := make([]int64, c.Len())
	c.CurrentWindowValue(0, got)
	if want := []int64{120, 60}; !slices.Equal(got, want) {
		t.Fatalf("CurrentWindowValue at bucket start = %v, want %v", got, want)
	}

	c.CurrentWindowValue(30, got)
	if want := []int64{120, 60}; !slices.Equal(got, want) {
		t.Fatalf("CurrentWindowValue halfway through current bucket = %v, want %v", got, want)
	}

	c.CurrentWindowValue(90, got)
	if want := []int64{60, 30}; !slices.Equal(got, want) {
		t.Fatalf("CurrentWindowValue halfway through next bucket = %v, want %v", got, want)
	}
}

func TestEpochWeightedCountersRotatesOneBucket(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)
	c.Add(10, []int64{120, 40})

	got := make([]int64, c.Len())
	c.CurrentValue(60, got)
	if want := []int64{120, 40}; !slices.Equal(got, want) {
		t.Fatalf("CurrentValue at next bucket start = %v, want %v", got, want)
	}

	c.Add(90, []int64{60, 20})

	assertValues(t, "prev", c.prev, []int64{120, 40})
	assertValues(t, "curr", c.curr, []int64{60, 20})

	c.CurrentValue(90, got)
	if want := []int64{90, 30}; !slices.Equal(got, want) {
		t.Fatalf("CurrentValue halfway through next bucket = %v, want %v", got, want)
	}
}

func TestEpochWeightedCountersIgnoresTimestampsOlderThanCurrentBucket(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)
	c.Add(10, []int64{100, 50})
	c.Add(70, []int64{20, 10})

	c.Add(59, []int64{1000, 1000})

	assertValues(t, "prev", c.prev, []int64{100, 50})
	assertValues(t, "curr", c.curr, []int64{20, 10})

	got := []int64{-1, -1}
	c.CurrentValue(59, got)
	if want := []int64{0, 0}; !slices.Equal(got, want) {
		t.Fatalf("CurrentValue for stale timestamp = %v, want %v", got, want)
	}
}

func TestEpochWeightedCountersResetsAfterSkippingBuckets(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)
	c.Add(10, []int64{100, 50})

	c.Add(180, []int64{25, 5})

	if got, want := c.start, uint32(180); got != want {
		t.Fatalf("start = %d, want %d", got, want)
	}
	assertValues(t, "prev", c.prev, []int64{0, 0})
	assertValues(t, "curr", c.curr, []int64{25, 5})
}

func TestEpochWeightedCountersIgnoresExtraInputAndOutputSlots(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)
	c.Add(0, []int64{120, 60, 999})

	got := []int64{-1, -1, 777}
	c.CurrentValue(30, got)

	if want := []int64{60, 30, 777}; !slices.Equal(got, want) {
		t.Fatalf("CurrentValue = %v, want %v", got, want)
	}
}

func TestEpochWeightedCountersPanicsForShortInput(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)

	defer func() {
		if recover() == nil {
			t.Fatal("Add did not panic")
		}
	}()

	c.Add(0, []int64{1})
}

func TestEpochWeightedCountersPanicsForShortOutput(t *testing.T) {
	c := NewEpochWeightedCounters(60, 0, 2)

	defer func() {
		if recover() == nil {
			t.Fatal("CurrentValue did not panic")
		}
	}()

	c.CurrentValue(0, []int64{0})
}

func TestBucketStart(t *testing.T) {
	tests := []struct {
		timestamp uint32
		span      uint32
		want      uint32
	}{
		{timestamp: 0, span: 60, want: 0},
		{timestamp: 59, span: 60, want: 0},
		{timestamp: 60, span: 60, want: 60},
		{timestamp: 119, span: 60, want: 60},
		{timestamp: 123456789, span: 60, want: 123456780},
		{timestamp: 123456789, span: 300, want: 123456600},
	}

	for _, tt := range tests {
		if got := bucketStart(tt.timestamp, tt.span); got != tt.want {
			t.Fatalf("bucketStart(%d, %d) = %d, want %d", tt.timestamp, tt.span, got, tt.want)
		}
	}
}

func assertValues(t *testing.T, name string, got []int64, want []int64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s len = %d, want %d", name, len(got), len(want))
	}

	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %d, want %d", name, i, got[i], want[i])
		}
	}
}
