package promhelpers

import (
	"sort"
	"testing"

	"github.com/gur-shatz/statekit"
)

func TestSamplesFromMapBuildsOneSamplePerEntry(t *testing.T) {
	published := map[string]uint64{
		"orders":   3,
		"payments": 7,
	}

	got := SamplesFromMap("published_by_table_total", "table", published)
	sort.Slice(got, func(i, j int) bool {
		return got[i].Labels["table"] < got[j].Labels["table"]
	})

	want := []statekit.PrometheusSample{
		{Name: "published_by_table_total", Labels: map[string]string{"table": "orders"}, Value: 3},
		{Name: "published_by_table_total", Labels: map[string]string{"table": "payments"}, Value: 7},
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%+v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Name != want[i].Name {
			t.Errorf("sample[%d].Name = %q, want %q", i, got[i].Name, want[i].Name)
		}
		if got[i].Value != want[i].Value {
			t.Errorf("sample[%d].Value = %v, want %v", i, got[i].Value, want[i].Value)
		}
		if got[i].Labels["table"] != want[i].Labels["table"] {
			t.Errorf("sample[%d].Labels[table] = %q, want %q", i, got[i].Labels["table"], want[i].Labels["table"])
		}
	}
}

func TestSamplesFromMapEmptyReturnsNil(t *testing.T) {
	if got := SamplesFromMap[uint64]("x", "k", nil); got != nil {
		t.Fatalf("nil map: got %+v, want nil", got)
	}
	if got := SamplesFromMap("x", "k", map[string]uint64{}); got != nil {
		t.Fatalf("empty map: got %+v, want nil", got)
	}
}

func TestSamplesFromMapWithTotalAppendsSum(t *testing.T) {
	published := map[string]uint64{
		"orders":   3,
		"payments": 7,
	}

	got := SamplesFromMapWithTotal("published_by_table", "table", "published_total", published)

	var total *statekit.PrometheusSample
	perTable := 0
	for i := range got {
		s := &got[i]
		switch s.Name {
		case "published_total":
			total = s
		case "published_by_table":
			perTable++
		}
	}
	if perTable != 2 {
		t.Errorf("per-table samples = %d, want 2", perTable)
	}
	if total == nil {
		t.Fatalf("missing total sample, got %+v", got)
	}
	if total.Value != 10 {
		t.Errorf("total.Value = %v, want 10", total.Value)
	}
	if len(total.Labels) != 0 {
		t.Errorf("total.Labels = %+v, want empty", total.Labels)
	}
}

func TestSamplesFromMapWithTotalEmitsZeroTotalOnEmpty(t *testing.T) {
	got := SamplesFromMapWithTotal[uint64]("m", "k", "m_total", nil)
	if len(got) != 1 || got[0].Name != "m_total" || got[0].Value != 0 {
		t.Fatalf("empty map: got %+v, want one zero-valued m_total sample", got)
	}
}

func TestSamplesFromMapWithTotalOmitsTotalWhenNameEmpty(t *testing.T) {
	got := SamplesFromMapWithTotal("m", "k", "", map[string]uint64{"a": 1})
	if len(got) != 1 || got[0].Name != "m" {
		t.Fatalf("got %+v, want only the per-key sample", got)
	}
}

func TestSamplesFromMapAcceptsFloatAndInt(t *testing.T) {
	intMap := map[string]int{"a": -1, "b": 2}
	if got := SamplesFromMap("m", "k", intMap); len(got) != 2 {
		t.Fatalf("int map: len = %d, want 2", len(got))
	}

	floatMap := map[string]float64{"a": 1.5}
	got := SamplesFromMap("m", "k", floatMap)
	if len(got) != 1 || got[0].Value != 1.5 {
		t.Fatalf("float map: got %+v, want one sample value 1.5", got)
	}
}
