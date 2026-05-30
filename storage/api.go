package storage

import (
	"embed"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

//go:embed openapi.yaml
var openAPIFS embed.FS

type API struct {
	store Store
}

func NewAPI(store Store) *API {
	return &API{store: store}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.yaml", a.handleOpenAPI)
	mux.HandleFunc("GET /state/current", a.handleCurrent)
	mux.HandleFunc("GET /state/targets", a.handleTargets)
	mux.HandleFunc("GET /state/groups", a.handleGroups)
	mux.HandleFunc("GET /state/events", a.handleEvents)
	mux.HandleFunc("POST /state/doc", a.handleIngestDocument)
	mux.HandleFunc("GET /escalations/incidents", a.handleIncidents)
	mux.HandleFunc("POST /escalations/doc", a.handleIngestEscalations)
	mux.HandleFunc("POST /escalations/ack", a.handleAcknowledgeIncident)
	return mux
}

func (a *API) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	data, err := openAPIFS.ReadFile("openapi.yaml")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	_, _ = w.Write(data)
}

func (a *API) handleCurrent(w http.ResponseWriter, r *http.Request) {
	items, err := a.store.Current(r.Context(), filterFromRequest(r))
	writeJSON(w, items, err)
}

func (a *API) handleTargets(w http.ResponseWriter, r *http.Request) {
	items, err := a.store.Targets(r.Context())
	writeJSON(w, items, err)
}

func (a *API) handleGroups(w http.ResponseWriter, r *http.Request) {
	query := GroupQuery{
		By:     groupByFromRequest(r),
		Filter: filterFromRequest(r),
	}
	items, err := a.store.Groups(r.Context(), query)
	writeJSON(w, items, err)
}

func (a *API) handleEvents(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := a.store.Events(r.Context(), EventFilter{
		Identity: r.URL.Query().Get("identity"),
		Limit:    limit,
	})
	writeJSON(w, items, err)
}

func (a *API) handleIngestDocument(w http.ResponseWriter, r *http.Request) {
	var doc statekit.StateDisplayDocument
	if err := yaml.NewDecoder(r.Body).Decode(&doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	observedAt := time.Now()
	if value := r.URL.Query().Get("observed_at"); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		observedAt = parsed
	}
	if err := a.store.IngestDocument(r.Context(), doc, observedAt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleIncidents(w http.ResponseWriter, r *http.Request) {
	items, err := a.store.Incidents(r.Context(), IncidentFilter{
		Source: r.URL.Query().Get("source"),
		Status: r.URL.Query().Get("status"),
		ID:     r.URL.Query().Get("id"),
	})
	writeYAML(w, items, err)
}

func (a *API) handleIngestEscalations(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		http.Error(w, "missing source", http.StatusBadRequest)
		return
	}
	var doc statekit.EscalationDisplayDocument
	if err := yaml.NewDecoder(r.Body).Decode(&doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	observedAt := time.Now()
	if value := r.URL.Query().Get("observed_at"); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		observedAt = parsed
	}
	if err := a.store.IngestEscalations(r.Context(), source, doc, observedAt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleAcknowledgeIncident(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	id := r.URL.Query().Get("id")
	if source == "" || id == "" {
		http.Error(w, "missing source or id", http.StatusBadRequest)
		return
	}
	if err := a.store.AcknowledgeIncident(r.Context(), source, id, time.Now()); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func filterFromRequest(r *http.Request) CurrentFilter {
	q := r.URL.Query()
	filter := CurrentFilter{
		Status:    q.Get("status"),
		GroupName: q.Get("group_name"),
		Labels:    map[string]string{},
	}
	for key, values := range q {
		if !strings.HasPrefix(key, "label.") || len(values) == 0 {
			continue
		}
		filter.Labels[strings.TrimPrefix(key, "label.")] = values[len(values)-1]
	}
	if len(filter.Labels) == 0 {
		filter.Labels = nil
	}
	return filter
}

func groupByFromRequest(r *http.Request) []string {
	q := r.URL.Query()
	var out []string
	for _, value := range q["by"] {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, value any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeYAML(w http.ResponseWriter, value any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	if err := yaml.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
