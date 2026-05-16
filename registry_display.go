package statekit

import (
	"net/http"
	"sort"

	"gopkg.in/yaml.v3"
)

const StateDisplayKind = "statekit.state.v1"

type StateDisplayDocument struct {
	Kind      string              `json:"kind" yaml:"kind"`
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
		LabelPath: r.stateDisplayLabelPath(),
		States:    r.Snapshot(),
	}
}

func (r *Registry) StateDisplayYAMLHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		if err := yaml.NewEncoder(w).Encode(r.StateDisplay()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
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
