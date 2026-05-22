package statekit

import (
	"fmt"
	"net/http"
	"sort"

	"gopkg.in/yaml.v3"
)

const StateDisplayKind = "statekit.state.v1"

type StateDisplayDocument struct {
	Kind      string              `json:"kind" yaml:"kind"`
	Version   string              `json:"version,omitempty" yaml:"version,omitempty"`
	LabelPath []StateDisplayLabel `json:"label_path" yaml:"label_path"`
	States    []Snapshot          `json:"states" yaml:"states"`
}

type StateDisplayLabel struct {
	Name  string `json:"name" yaml:"name"`
	Value string `json:"value" yaml:"value"`
}

func (r *Registry) StateDisplay() StateDisplayDocument {
	return StateDisplayDocument{
		Kind:      StateDisplayKind,
		Version:   r.Version(),
		LabelPath: r.stateDisplayLabelPath(),
		States:    r.Snapshot(),
	}
}

func (r *Registry) StateDisplayYAMLHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		doc, err := displayDocumentWithFormat(r.StateDisplay(), req.URL.Query().Get("format"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		if err := yaml.NewEncoder(w).Encode(doc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// defaultHistoryLimit caps history entries in the default display format.
// History is exposed most-recent-first by the tracker, so the top N is the
// freshest N. Use ?format=verbose to see the full retained chain.
const defaultHistoryLimit = 4

// ApplyDisplayFormat applies the named format ("", "verbose", "short") to a
// StateDisplayDocument. Callers assembling a filtered document (e.g. a
// per-target view in a fleet aggregator) use this to keep the same
// history-limit semantics as the registry's built-in /state handler.
func ApplyDisplayFormat(doc StateDisplayDocument, format string) (StateDisplayDocument, error) {
	return displayDocumentWithFormat(doc, format)
}

func displayDocumentWithFormat(doc StateDisplayDocument, format string) (StateDisplayDocument, error) {
	switch format {
	case "":
		doc.States = snapshotsWithHistoryLimit(doc.States, defaultHistoryLimit)
		return doc, nil
	case "verbose":
		return doc, nil
	case "short":
		doc.States = snapshotsWithoutHistory(doc.States)
		return doc, nil
	default:
		return StateDisplayDocument{}, fmt.Errorf("unsupported state display format %q", format)
	}
}

func snapshotsWithoutHistory(in []Snapshot) []Snapshot {
	out := make([]Snapshot, len(in))
	for i, snap := range in {
		snap.History = nil
		snap.Checks = snapshotsWithoutHistory(snap.Checks)
		out[i] = snap
	}
	return out
}

// snapshotsWithHistoryLimit keeps at most limit entries from the start of
// each Snapshot's History. The tracker emits history newest-first, so the
// prefix is the most recent N.
func snapshotsWithHistoryLimit(in []Snapshot, limit int) []Snapshot {
	out := make([]Snapshot, len(in))
	for i, snap := range in {
		if len(snap.History) > limit {
			snap.History = append([]HistoryEntry(nil), snap.History[:limit]...)
		}
		snap.Checks = snapshotsWithHistoryLimit(snap.Checks, limit)
		out[i] = snap
	}
	return out
}

func (r *Registry) stateDisplayLabelPath() []StateDisplayLabel {
	r.mu.RLock()
	labels := make(map[string]string, len(r.labels))
	for name, value := range r.labels {
		labels[name] = value
	}
	order := append([]string(nil), r.labelOrder...)
	r.mu.RUnlock()

	seen := make(map[string]struct{}, len(order))
	path := make([]StateDisplayLabel, 0, len(labels))
	for _, name := range order {
		value, ok := labels[name]
		if !ok {
			continue
		}
		path = append(path, StateDisplayLabel{Name: name, Value: value})
		seen[name] = struct{}{}
	}

	var rest []string
	for name := range labels {
		if _, ok := seen[name]; !ok {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	for _, name := range rest {
		path = append(path, StateDisplayLabel{Name: name, Value: labels[name]})
	}
	return path
}
