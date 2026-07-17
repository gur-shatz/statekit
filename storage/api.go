package storage

import (
	"embed"
	"encoding/json"
	"errors"
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

const (
	defaultTimelineWindow  = 24 * time.Hour
	defaultTimelineBuckets = 64
)

type API struct {
	store   Store
	chart   ChartStore
	metrics MetricsStore
}

type APIOption func(*API)

// WithChart mounts the charting endpoints on a specific chart store. Without
// it, NewAPI uses the store's own chart store when the store exposes one
// (MemoryStore does).
func WithChart(chart ChartStore) APIOption {
	return func(a *API) {
		a.chart = chart
	}
}

// WithMetrics mounts the metrics endpoint on a specific metrics store.
func WithMetrics(metrics MetricsStore) APIOption {
	return func(a *API) {
		a.metrics = metrics
	}
}

// ChartProvider is implemented by stores that own a charting store.
type ChartProvider interface {
	Chart() ChartStore
}

type MetricsProvider interface {
	MetricsStore() MetricsStore
}

func NewAPI(store Store, opts ...APIOption) *API {
	api := &API{store: store}
	for _, opt := range opts {
		opt(api)
	}
	if api.chart == nil {
		if provider, ok := store.(ChartProvider); ok {
			api.chart = provider.Chart()
		}
	}
	if api.metrics == nil {
		if provider, ok := store.(MetricsProvider); ok {
			api.metrics = provider.MetricsStore()
		}
	}
	return api
}

type route struct {
	Method  string
	Pattern string
	Handler http.HandlerFunc
}

// routes is the single registration table; the OpenAPI contract test checks
// it against the embedded spec in both directions.
func (a *API) routes() []route {
	return []route{
		{"GET", "/openapi.yaml", a.handleOpenAPI},
		{"GET", "/state/summary", a.handleSummary},
		{"GET", "/state/targets", a.handleTargets},
		{"GET", "/state/targets/{key...}", a.handleTargetDetail},
		{"GET", "/state/states/{identity}", a.handleStateDetail},
		{"GET", "/state/states/{identity}/timeline", a.handleStateTimeline},
		{"GET", "/state/timeline", a.handleChartTimeline},
		{"GET", "/state/timeline/bucket", a.handleChartBucket},
		{"GET", "/metrics/status", a.handleMetricsStatus},
		{"GET", "/metrics/timeseries", a.handleMetricsTimeseries},
		{"POST", "/state/doc", a.handleIngestDocument},
		{"GET", "/state/mutes", a.handleMutes},
		{"POST", "/state/mutes", a.handleUpsertMute},
		{"DELETE", "/state/mutes/{identity}", a.handleDeleteMute},
		{"GET", "/escalations/incidents", a.handleIncidents},
		{"GET", "/escalations/incidents/{source}/{id}", a.handleIncidentDetail},
		{"POST", "/escalations/doc", a.handleIngestEscalations},
		{"POST", "/escalations/ack", a.handleAcknowledgeIncident},
		{"POST", "/escalations/global", a.handleGlobalIncident},
	}
}

type MetricsStatus struct {
	Enabled bool `json:"enabled"`
}

func (a *API) handleMetricsStatus(w http.ResponseWriter, r *http.Request) {
	out := MetricsStatus{Enabled: a.metrics != nil}
	writeJSONETag(w, r, hashJSON(out), out)
}

func (a *API) handleMetricsTimeseries(w http.ResponseWriter, r *http.Request) {
	if a.metrics == nil {
		http.Error(w, "metrics store not configured", http.StatusNotFound)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing metrics key", http.StatusBadRequest)
		return
	}
	window := defaultMetricsRetention
	if value := r.URL.Query().Get("window"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			http.Error(w, fmt.Sprintf("invalid window %q", value), http.StatusBadRequest)
			return
		}
		window = parsed
	}
	to := time.Now()
	out, err := a.metrics.Metrics(key, to.Add(-window), to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONETag(w, r, hashJSON(out), out)
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, r := range a.routes() {
		mux.HandleFunc(r.Method+" "+r.Pattern, r.Handler)
	}
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

func (a *API) handleSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := a.store.Summary(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONETag(w, r, summary.FleetHash, summary)
}

func (a *API) handleTargets(w http.ResponseWriter, r *http.Request) {
	summary, err := a.store.Summary(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items, err := a.store.Targets(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONETag(w, r, summary.FleetHash, items)
}

func (a *API) handleTargetDetail(w http.ResponseWriter, r *http.Request) {
	detail, err := a.store.TargetDetail(r.Context(), r.PathValue("key"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSONETag(w, r, detail.MaterialHash, detail)
}

func (a *API) handleStateDetail(w http.ResponseWriter, r *http.Request) {
	detail, err := a.store.StateDetail(r.Context(), r.PathValue("identity"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSONETag(w, r, stateDetailETag(detail), detail)
}

func (a *API) handleStateTimeline(w http.ResponseWriter, r *http.Request) {
	timeline, err := a.store.StateTimeline(r.Context(), r.PathValue("identity"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSONETag(w, r, hashJSON(timeline.Transitions), timeline)
}

func (a *API) handleChartTimeline(w http.ResponseWriter, r *http.Request) {
	if a.chart == nil {
		http.Error(w, "charting store not configured", http.StatusNotFound)
		return
	}
	q := r.URL.Query()
	window := defaultTimelineWindow
	if value := q.Get("window"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			http.Error(w, fmt.Sprintf("invalid window %q", value), http.StatusBadRequest)
			return
		}
		window = parsed
	}
	buckets := defaultTimelineBuckets
	if value := q.Get("buckets"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			http.Error(w, fmt.Sprintf("invalid buckets %q", value), http.StatusBadRequest)
			return
		}
		buckets = parsed
	}
	to := time.Now()
	out, err := a.chart.Range(q.Get("scope"), to.Add(-window), to, buckets)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONETag(w, r, hashJSON(out), out)
}

func (a *API) handleChartBucket(w http.ResponseWriter, r *http.Request) {
	if a.chart == nil {
		http.Error(w, "charting store not configured", http.StatusNotFound)
		return
	}
	q := r.URL.Query()
	t, err := time.Parse(time.RFC3339Nano, q.Get("t"))
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid t: %v", err), http.StatusBadRequest)
		return
	}
	out, err := a.chart.Bucket(q.Get("scope"), t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONETag(w, r, hashJSON(out), out)
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

type MuteRequest struct {
	Identity  string `json:"identity"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	Duration  string `json:"duration,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

func (a *API) handleMutes(w http.ResponseWriter, r *http.Request) {
	items, err := a.store.Mutes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONETag(w, r, hashJSON(items), items)
}

func (a *API) handleUpsertMute(w http.ResponseWriter, r *http.Request) {
	var req MuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Identity == "" {
		http.Error(w, "missing identity", http.StatusBadRequest)
		return
	}
	if req.Status == "" {
		http.Error(w, "missing status", http.StatusBadRequest)
		return
	}
	now := time.Now()
	expiresAt, err := parseMuteExpiry(req, now)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mute, err := a.store.UpsertMute(r.Context(), StateMute{
		Identity:  req.Identity,
		Status:    strings.ToLower(req.Status),
		Reason:    req.Reason,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, mute, nil)
}

func parseMuteExpiry(req MuteRequest, now time.Time) (time.Time, error) {
	if req.ExpiresAt != "" {
		expiresAt, err := time.Parse(time.RFC3339Nano, req.ExpiresAt)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid expires_at: %w", err)
		}
		if !expiresAt.After(now) {
			return time.Time{}, fmt.Errorf("expires_at must be in the future")
		}
		return expiresAt, nil
	}
	if req.Duration == "" {
		return time.Time{}, fmt.Errorf("missing duration or expires_at")
	}
	duration, err := parseMuteDuration(req.Duration)
	if err != nil || duration <= 0 {
		return time.Time{}, fmt.Errorf("invalid duration %q", req.Duration)
	}
	return now.Add(duration), nil
}

// parseMuteDuration accepts Go durations plus a day suffix ("7d", "30d",
// "365d"), which time.ParseDuration does not support.
func parseMuteDuration(raw string) (time.Duration, error) {
	if trimmed, ok := strings.CutSuffix(raw, "d"); ok {
		days, err := strconv.Atoi(trimmed)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", raw)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(raw)
}

func (a *API) handleDeleteMute(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteMute(r.Context(), r.PathValue("identity")); err != nil {
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
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The list is a summary: events ride only on the per-incident detail
	// endpoint.
	for i := range items {
		items[i].Events = nil
	}
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSONETag(w, r, incidentsETag(items), items)
		return
	}
	writeYAML(w, items, nil)
}

func (a *API) handleIncidentDetail(w http.ResponseWriter, r *http.Request) {
	incident, err := a.findIncident(r, r.PathValue("source"), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSONETag(w, r, incidentsETag([]Incident{incident}), incident)
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

// stateDetailETag hashes the material content of a state detail, excluding
// volatile timestamps that move on every scrape, so an unchanged state polls
// as 304.
func stateDetailETag(detail StateDetail) string {
	return hashJSON(map[string]any{
		"identity":        detail.Identity,
		"parent_identity": detail.ParentIdentity,
		"name":            detail.Name,
		"status":          detail.Status,
		"reason":          detail.Reason,
		"importance":      detail.Importance,
		"help":            detail.Help,
		"group_name":      detail.GroupName,
		"labels":          detail.Labels,
		"changed_at":      detail.ChangedAt,
		"data_hash":       detail.DataHash,
		"children":        detail.Children,
		"mute":            detail.Mute,
	})
}

// incidentsETag hashes incident content excluding observed_at, which bumps on
// every scrape without the incidents changing.
func incidentsETag(items []Incident) string {
	fields := make([]map[string]any, 0, len(items))
	for _, incident := range items {
		fields = append(fields, map[string]any{
			"identity":        incident.Identity,
			"status":          incident.Status,
			"title":           incident.Title,
			"severity":        incident.Severity,
			"last_updated_at": incident.LastUpdatedAt,
			"events":          len(incident.Events),
		})
	}
	return hashJSON(fields)
}

// writeJSONETag writes value as JSON with a strong ETag, answering 304 when
// If-None-Match matches.
func writeJSONETag(w http.ResponseWriter, r *http.Request, etag string, value any) {
	tag := `"` + etag + `"`
	w.Header().Set("ETag", tag)
	if ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), tag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, value, nil)
}

func ifNoneMatchSatisfied(header, tag string) bool {
	if header == "" {
		return false
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "W/")
		if candidate == "*" || candidate == tag {
			return true
		}
	}
	return false
}

func writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
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
