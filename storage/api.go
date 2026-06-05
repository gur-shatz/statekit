package storage

import (
	"embed"
	"encoding/json"
	"fmt"
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
	mux.HandleFunc("POST /escalations/global", a.handleGlobalIncident)
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
		Type:   r.URL.Query().Get("type"),
	})
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSON(w, items, err)
		return
	}
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

// GlobalIncidentRequest reports a fleet-wide incident (build, deployment,
// rollback, ...) directly to the store, without a component owning it. An
// empty ID creates a new incident; a request with an ID annotates the
// existing incident and can close it by setting status to "closed".
type GlobalIncidentRequest struct {
	Source   string            `json:"source,omitempty"`
	ID       string            `json:"id,omitempty"`
	Type     string            `json:"type,omitempty"`
	Title    string            `json:"title,omitempty"`
	Severity *statekit.Status  `json:"severity,omitempty"`
	Status   string            `json:"status,omitempty"`
	Message  string            `json:"message,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	Data     map[string]any    `json:"data,omitempty"`
}

const defaultGlobalIncidentSource = "global"

func (a *API) handleGlobalIncident(w http.ResponseWriter, r *http.Request) {
	var req GlobalIncidentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	source := req.Source
	if source == "" {
		source = defaultGlobalIncidentSource
	}
	now := time.Now()
	var incident statekit.EscalationIncident
	created := req.ID == ""
	if created {
		if req.Type == "" {
			http.Error(w, "missing type", http.StatusBadRequest)
			return
		}
		incident = newGlobalIncident(req, now)
	} else {
		existing, err := a.findIncident(r, source, req.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		source = existing.Source
		incident, err = updatedGlobalIncident(existing, req, now)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	doc := statekit.EscalationDisplayDocument{
		Kind:      statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{incident},
	}
	if err := a.store.IngestEscalations(r.Context(), source, doc, now); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stored, err := a.findIncident(r, source, incident.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if created {
		w.WriteHeader(http.StatusCreated)
	}
	writeJSON(w, stored, nil)
}

func newGlobalIncident(req GlobalIncidentRequest, now time.Time) statekit.EscalationIncident {
	title := req.Title
	if title == "" {
		title = req.Type
	}
	var severity statekit.Status
	if req.Severity != nil {
		severity = *req.Severity
	}
	message := req.Message
	if message == "" {
		message = "started"
	}
	return statekit.EscalationIncident{
		ID:            req.Type + "-" + strconv.FormatInt(now.UnixNano(), 10),
		Type:          req.Type,
		Title:         title,
		Status:        statekit.EscalationOpen,
		CreatedAt:     now,
		LastUpdatedAt: now,
		Severity:      severity,
		Labels:        req.Labels,
		Events:        []statekit.EscalationEvent{globalIncidentEvent(req.Type, message, req.Data, now)},
	}
}

func updatedGlobalIncident(existing Incident, req GlobalIncidentRequest, now time.Time) (statekit.EscalationIncident, error) {
	severity, err := statekit.ParseStatus(firstNonEmpty(existing.Severity, statekit.Pass.String()))
	if err != nil {
		return statekit.EscalationIncident{}, err
	}
	if req.Severity != nil {
		severity = *req.Severity
	}
	incident := statekit.EscalationIncident{
		ID:            existing.ID,
		Type:          existing.Type,
		Title:         firstNonEmpty(req.Title, existing.Title),
		Status:        existing.Status,
		CreatedAt:     existing.CreatedAt,
		ExpiresAt:     existing.ExpiresAt,
		LastUpdatedAt: now,
		Severity:      severity,
		Labels:        mergeStringLabels(existing.Labels, req.Labels),
		Topics:        existing.Topics,
	}
	message := req.Message
	switch req.Status {
	case "":
	case statekit.EscalationClosed:
		incident.Status = statekit.EscalationClosed
		if message == "" {
			message = "closed"
		}
	default:
		return statekit.EscalationIncident{}, fmt.Errorf("unsupported status %q", req.Status)
	}
	if message != "" || len(req.Data) > 0 {
		incident.Events = []statekit.EscalationEvent{globalIncidentEvent(existing.Type, message, req.Data, now)}
	}
	return incident, nil
}

func globalIncidentEvent(topic, message string, data map[string]any, now time.Time) statekit.EscalationEvent {
	return statekit.EscalationEvent{
		Seq:       strconv.FormatInt(now.UnixNano(), 10),
		Timestamp: now,
		Topic:     topic,
		Message:   message,
		Data:      data,
	}
}

func (a *API) findIncident(r *http.Request, source, id string) (Incident, error) {
	items, err := a.store.Incidents(r.Context(), IncidentFilter{Source: source, ID: id})
	if err != nil {
		return Incident{}, err
	}
	if len(items) == 0 {
		return Incident{}, fmt.Errorf("incident %q from source %q not found", id, source)
	}
	latest := items[0]
	for _, item := range items[1:] {
		if item.CreatedAt.After(latest.CreatedAt) {
			latest = item
		}
	}
	return latest, nil
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
