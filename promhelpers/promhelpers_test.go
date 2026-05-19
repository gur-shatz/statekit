package promhelpers

import (
	"sort"
	"testing"

	"github.com/gur-shatz/statekit"
)

func TestSamplesFromMapBuildsOneSamplePerEntryPlusTotal(t *testing.T) {
	published := map[string]uint64{
		"orders":   3,
		"payments": 7,
	}

	got := SamplesFromMap("published_by_table", "table", published)

	var perTable []statekit.PrometheusSample
	var total *statekit.PrometheusSample
	for i := range got {
		s := &got[i]
		switch s.Name {
		case "published_by_table":
			perTable = append(perTable, *s)
		case "published_by_table_total":
			total = s
		default:
			t.Errorf("unexpected sample name %q", s.Name)
		}
	}

	sort.Slice(perTable, func(i, j int) bool {
		return perTable[i].Labels["table"] < perTable[j].Labels["table"]
	})
	want := []statekit.PrometheusSample{
		{Name: "published_by_table", Labels: map[string]string{"table": "orders"}, Value: 3},
		{Name: "published_by_table", Labels: map[string]string{"table": "payments"}, Value: 7},
	}
	if len(perTable) != len(want) {
		t.Fatalf("per-table len = %d, want %d (%+v)", len(perTable), len(want), perTable)
	}
	for i := range want {
		if perTable[i].Value != want[i].Value {
			t.Errorf("sample[%d].Value = %v, want %v", i, perTable[i].Value, want[i].Value)
		}
		if perTable[i].Labels["table"] != want[i].Labels["table"] {
			t.Errorf("sample[%d].Labels[table] = %q, want %q", i, perTable[i].Labels["table"], want[i].Labels["table"])
		}
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

func TestSamplesFromMapEmptyReturnsNil(t *testing.T) {
	if got := SamplesFromMap[uint64]("x", "k", nil); got != nil {
		t.Fatalf("nil map: got %+v, want nil", got)
	}
	if got := SamplesFromMap("x", "k", map[string]uint64{}); got != nil {
		t.Fatalf("empty map: got %+v, want nil", got)
	}
}

func TestSamplesFromMapAcceptsFloatAndInt(t *testing.T) {
	intMap := map[string]int{"a": -1, "b": 2}
	got := SamplesFromMap("m", "k", intMap)
	if len(got) != 3 {
		t.Fatalf("int map: len = %d, want 3 (2 per-key + total)", len(got))
	}
	var intTotal float64
	for _, s := range got {
		if s.Name == "m_total" {
			intTotal = s.Value
		}
	}
	if intTotal != 1 {
		t.Errorf("int total = %v, want 1", intTotal)
	}

	floatMap := map[string]float64{"a": 1.5}
	got = SamplesFromMap("m", "k", floatMap)
	if len(got) != 2 {
		t.Fatalf("float map: len = %d, want 2 (1 per-key + total)", len(got))
	}
	for _, s := range got {
		if s.Name == "m_total" && s.Value != 1.5 {
			t.Errorf("float total = %v, want 1.5", s.Value)
		}
	}
}
