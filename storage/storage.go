// Package storage stores statekit display documents in a query-friendly shape.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

// Store is the storage boundary. Backends should treat IngestDocument as the
// entry point for every state document received from a component or scraper.
type Store interface {
	IngestDocument(ctx context.Context, doc statekit.StateDisplayDocument, observedAt time.Time) error
	IngestEscalations(ctx context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error
	Current(ctx context.Context, filter CurrentFilter) ([]CurrentState, error)
	Groups(ctx context.Context, query GroupQuery) ([]GroupBucket, error)
	Events(ctx context.Context, filter EventFilter) ([]StateEvent, error)
	Targets(ctx context.Context) ([]TargetDocument, error)
	Incidents(ctx context.Context, filter IncidentFilter) ([]Incident, error)
	AcknowledgeIncident(ctx context.Context, source, id string, at time.Time) error
}

type StateNode struct {
	Identity       string                       `json:"identity"`
	ParentIdentity string                       `json:"parent_identity,omitempty"`
	Name           string                       `json:"name"`
	ScrapedFrom    string                       `json:"scraped_from,omitempty"`
	ScrapePath     string                       `json:"scrape_path,omitempty"`
	Importance     string                       `json:"importance"`
	Help           string                       `json:"help,omitempty"`
	GroupName      string                       `json:"group_name,omitempty"`
	LabelPath      []statekit.StateDisplayLabel `json:"label_path,omitempty"`
	Labels         map[string]string            `json:"labels,omitempty"`
	FirstSeenAt    time.Time                    `json:"first_seen_at"`
	LastSeenAt     time.Time                    `json:"last_seen_at"`
	MetadataHash   string                       `json:"metadata_hash"`
}

type CurrentObservation struct {
	Identity       string         `json:"identity"`
	Status         string         `json:"status"`
	Reason         string         `json:"reason,omitempty"`
	ChangedAt      time.Time      `json:"changed_at"`
	ChangedSecsAgo int64          `json:"changed_secs_ago"`
	UpdatedAt      time.Time      `json:"updated_at,omitempty"`
	UpdatedSecsAgo int64          `json:"updated_secs_ago,omitempty"`
	ObservedAt     time.Time      `json:"observed_at"`
	Data           map[string]any `json:"data,omitempty"`
	DataHash       string         `json:"data_hash,omitempty"`
}

type CurrentState struct {
	StateNode
	Observation CurrentObservation `json:"observation"`
}

type StateEvent struct {
	EventKey   string         `json:"event_key"`
	Identity   string         `json:"identity"`
	Status     string         `json:"status"`
	Reason     string         `json:"reason,omitempty"`
	ChangedAt  time.Time      `json:"changed_at"`
	UpdatedAt  time.Time      `json:"updated_at,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
	Data       map[string]any `json:"data,omitempty"`
}

type TargetDocument struct {
	Key            string            `json:"key"`
	Name           string            `json:"name"`
	ScrapePath     string            `json:"scrape_path"`
	Labels         map[string]string `json:"labels,omitempty"`
	WorstStatus    string            `json:"worst_status"`
	StatusCounts   map[string]int    `json:"status_counts"`
	AffectedStates []AffectedState   `json:"affected_states,omitempty"`
	MaterialHash   string            `json:"material_hash"`
	ObservedAt     time.Time         `json:"observed_at"`
	States         []TargetState     `json:"states"`
}

type TargetState struct {
	Identity   string            `json:"identity"`
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	Reason     string            `json:"reason,omitempty"`
	Importance string            `json:"importance"`
	Help       string            `json:"help,omitempty"`
	GroupName  string            `json:"group_name,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Data       map[string]any    `json:"data,omitempty"`
	ChangedAt  time.Time         `json:"changed_at"`
	UpdatedAt  time.Time         `json:"updated_at,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
	Checks     []TargetCheck     `json:"checks,omitempty"`
}

type TargetCheck struct {
	Identity   string            `json:"identity"`
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	Reason     string            `json:"reason,omitempty"`
	Importance string            `json:"importance"`
	Help       string            `json:"help,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	ChangedAt  time.Time         `json:"changed_at"`
	UpdatedAt  time.Time         `json:"updated_at,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
}

type AffectedState struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Reason    string    `json:"reason,omitempty"`
	ChangedAt time.Time `json:"changed_at"`
}

type CurrentFilter struct {
	Status    string            `json:"status,omitempty"`
	GroupName string            `json:"group_name,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type GroupQuery struct {
	By     []string      `json:"by,omitempty"`
	Filter CurrentFilter `json:"filter,omitempty"`
}

type GroupBucket struct {
	Values       map[string]string `json:"values"`
	StatusCounts map[string]int    `json:"status_counts"`
	Total        int               `json:"total"`
	WorstStatus  string            `json:"worst_status"`
}

type EventFilter struct {
	Identity string `json:"identity,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

type Incident struct {
	Identity      string                       `json:"identity"`
	Source        string                       `json:"source"`
	ScrapedFrom   string                       `json:"scraped_from,omitempty"`
	ScrapePath    string                       `json:"scrape_path,omitempty"`
	ID            string                       `json:"id"`
	Title         string                       `json:"title"`
	Status        string                       `json:"status"`
	CreatedAt     time.Time                    `json:"created_at"`
	ExpiresAt     time.Time                    `json:"expires_at"`
	LastUpdatedAt time.Time                    `json:"last_updated_at"`
	Severity      string                       `json:"severity,omitempty"`
	LabelPath     []statekit.StateDisplayLabel `json:"label_path,omitempty"`
	Labels        map[string]string            `json:"labels,omitempty"`
	Topics        map[string]any               `json:"topics,omitempty"`
	Events        []IncidentEvent              `json:"events,omitempty"`
	ObservedAt    time.Time                    `json:"observed_at"`
}

type IncidentEvent struct {
	EventKey   string         `json:"event_key"`
	Identity   string         `json:"identity"`
	Seq        string         `json:"seq"`
	Timestamp  time.Time      `json:"timestamp"`
	Topic      string         `json:"topic"`
	Message    string         `json:"message,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
}

type IncidentFilter struct {
	Source string `json:"source,omitempty"`
	Status string `json:"status,omitempty"`
	ID     string `json:"id,omitempty"`
}

type MemoryStore struct {
	mu                 sync.RWMutex
	nodes              map[string]StateNode
	current            map[string]CurrentObservation
	events             map[string]StateEvent
	order              []string
	targets            map[string]TargetDocument
	incidents          map[string]Incident
	incidentEvents     map[string]map[string]IncidentEvent
	docScopeIdentities map[string]map[string]struct{}
	docScopeTargets    map[string]map[string]struct{}
	docCache           DocumentCache[statekit.StateDisplayDocument]
	docTTL             time.Duration
}

type MemoryStoreOption func(*MemoryStore)

func WithDocumentCache(cache DocumentCache[statekit.StateDisplayDocument], ttl time.Duration) MemoryStoreOption {
	return func(s *MemoryStore) {
		s.docCache = cache
		s.docTTL = ttl
	}
}

func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	store := &MemoryStore{
		nodes:              map[string]StateNode{},
		current:            map[string]CurrentObservation{},
		events:             map[string]StateEvent{},
		targets:            map[string]TargetDocument{},
		incidents:          map[string]Incident{},
		incidentEvents:     map[string]map[string]IncidentEvent{},
		docScopeIdentities: map[string]map[string]struct{}{},
		docScopeTargets:    map[string]map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

func (s *MemoryStore) IngestEscalations(_ context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	labels := labelsFromLabelPath(doc.LabelPath)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, in := range doc.Incidents {
		origin := firstNonEmpty(in.ScrapedFrom, source)
		scrapePath := firstNonEmpty(in.ScrapePath, source)
		incidentLabels := mergeStringLabels(labels, in.Labels)
		identity := incidentIdentity(origin, in.ID, in.CreatedAt)
		incident := s.incidents[identity]
		incident.Identity = identity
		incident.Source = origin
		incident.ScrapedFrom = origin
		incident.ScrapePath = scrapePath
		incident.ID = in.ID
		incident.Title = in.Title
		incident.Status = in.Status
		incident.CreatedAt = in.CreatedAt
		incident.ExpiresAt = in.ExpiresAt
		incident.LastUpdatedAt = in.LastUpdatedAt
		incident.Severity = in.Severity.String()
		incident.LabelPath = append([]statekit.StateDisplayLabel(nil), doc.LabelPath...)
		incident.Labels = incidentLabels
		incident.Topics = cloneData(in.Topics)
		incident.ObservedAt = observedAt
		events := s.incidentEvents[identity]
		if events == nil {
			events = map[string]IncidentEvent{}
			s.incidentEvents[identity] = events
		}
		for _, event := range in.Events {
			key := hashJSON(map[string]any{"scraped_from": origin, "incident_id": in.ID, "created_at": in.CreatedAt, "seq": event.Seq})
			events[key] = IncidentEvent{
				EventKey:   key,
				Identity:   identity,
				Seq:        event.Seq,
				Timestamp:  event.Timestamp,
				Topic:      event.Topic,
				Message:    event.Message,
				Data:       cloneData(event.Data),
				ObservedAt: observedAt,
			}
		}
		incident.Events = sortedIncidentEvents(events)
		s.incidents[identity] = incident
	}
	return nil
}

func (s *MemoryStore) IngestDocument(ctx context.Context, doc statekit.StateDisplayDocument, observedAt time.Time) error {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	if err := s.cacheDocument(ctx, doc); err != nil {
		return err
	}
	entries := flattenDocument(doc, observedAt)
	targets := buildTargetDocuments(entries)
	docKey := StateDisplayDocumentKey(doc)

	newIdentities := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		newIdentities[entry.Node.Identity] = struct{}{}
	}
	newTargets := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		newTargets[target.Key] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for identity := range s.docScopeIdentities[docKey] {
		if _, kept := newIdentities[identity]; kept {
			continue
		}
		delete(s.nodes, identity)
		delete(s.current, identity)
	}
	for key := range s.docScopeTargets[docKey] {
		if _, kept := newTargets[key]; kept {
			continue
		}
		delete(s.targets, key)
	}

	for _, entry := range entries {
		if existing, ok := s.nodes[entry.Node.Identity]; ok {
			entry.Node.FirstSeenAt = existing.FirstSeenAt
		}
		s.nodes[entry.Node.Identity] = entry.Node
		s.current[entry.Current.Identity] = entry.Current
		if _, ok := s.events[entry.Event.EventKey]; !ok {
			s.events[entry.Event.EventKey] = entry.Event
			s.order = append(s.order, entry.Event.EventKey)
		}
	}
	for _, target := range targets {
		s.targets[target.Key] = target
	}
	s.docScopeIdentities[docKey] = newIdentities
	s.docScopeTargets[docKey] = newTargets
	return nil
}

func (s *MemoryStore) CachedDocumentYAML(ctx context.Context, key string) ([]byte, bool, error) {
	if s.docCache == nil {
		return nil, false, nil
	}
	return s.docCache.GetYAML(ctx, key)
}

func (s *MemoryStore) cacheDocument(ctx context.Context, doc statekit.StateDisplayDocument) error {
	if s.docCache == nil {
		return nil
	}
	return s.docCache.Set(ctx, StateDisplayDocumentKey(doc), doc, s.docTTL)
}

func (s *MemoryStore) Current(_ context.Context, filter CurrentFilter) ([]CurrentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]CurrentState, 0, len(s.current))
	for identity, current := range s.current {
		node, ok := s.nodes[identity]
		if !ok {
			continue
		}
		item := CurrentState{StateNode: node, Observation: current}
		if !matchesFilter(item, filter) {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity < out[j].Identity
	})
	return out, nil
}

func (s *MemoryStore) Groups(ctx context.Context, query GroupQuery) ([]GroupBucket, error) {
	if len(query.By) == 0 {
		query.By = []string{"group_name"}
	}
	current, err := s.Current(ctx, query.Filter)
	if err != nil {
		return nil, err
	}
	by := append([]string(nil), query.By...)
	buckets := map[string]*GroupBucket{}
	for _, item := range current {
		values := groupValues(item, by)
		key := stableString(values)
		bucket := buckets[key]
		if bucket == nil {
			bucket = &GroupBucket{
				Values:       values,
				StatusCounts: map[string]int{},
				WorstStatus:  statekit.Pass.String(),
			}
			buckets[key] = bucket
		}
		bucket.Total++
		bucket.StatusCounts[item.Observation.Status]++
		if statusRank(item.Observation.Status) > statusRank(bucket.WorstStatus) {
			bucket.WorstStatus = item.Observation.Status
		}
	}

	out := make([]GroupBucket, 0, len(buckets))
	for _, bucket := range buckets {
		out = append(out, *bucket)
	}
	sort.Slice(out, func(i, j int) bool {
		return stableString(out[i].Values) < stableString(out[j].Values)
	})
	return out, nil
}

func (s *MemoryStore) Events(_ context.Context, filter EventFilter) ([]StateEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []StateEvent
	for _, key := range s.order {
		event := s.events[key]
		if filter.Identity != "" && event.Identity != filter.Identity {
			continue
		}
		out = append(out, event)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[len(out)-filter.Limit:]
	}
	return out, nil
}

func (s *MemoryStore) Targets(_ context.Context) ([]TargetDocument, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]TargetDocument, 0, len(s.targets))
	for _, target := range s.targets {
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool {
		if statusRank(out[i].WorstStatus) != statusRank(out[j].WorstStatus) {
			return statusRank(out[i].WorstStatus) > statusRank(out[j].WorstStatus)
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (s *MemoryStore) Incidents(_ context.Context, filter IncidentFilter) ([]Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Incident, 0, len(s.incidents))
	for _, incident := range s.incidents {
		if filter.Source != "" && incident.Source != filter.Source {
			continue
		}
		if filter.ID != "" && incident.ID != filter.ID {
			continue
		}
		if filter.Status != "" && incident.Status != filter.Status {
			continue
		}
		incident.Events = sortedIncidentEvents(s.incidentEvents[incident.Identity])
		out = append(out, incident)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastUpdatedAt.Equal(out[j].LastUpdatedAt) {
			return out[i].LastUpdatedAt.After(out[j].LastUpdatedAt)
		}
		return out[i].Identity < out[j].Identity
	})
	return out, nil
}

func (s *MemoryStore) AcknowledgeIncident(_ context.Context, source, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var identity string
	var incident Incident
	var ok bool
	for candidateIdentity, candidate := range s.incidents {
		if candidate.Source != source || candidate.ID != id {
			continue
		}
		if !ok || candidate.CreatedAt.After(incident.CreatedAt) {
			identity = candidateIdentity
			incident = candidate
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("incident %q from source %q not found", id, source)
	}
	incident.Status = statekit.EscalationAcknowledged
	if at.IsZero() {
		at = time.Now()
	}
	incident.ObservedAt = at
	s.incidents[identity] = incident
	return nil
}

type flattenedState struct {
	Node    StateNode
	Current CurrentObservation
	Event   StateEvent
}

func flattenDocument(doc statekit.StateDisplayDocument, observedAt time.Time) []flattenedState {
	var out []flattenedState
	for _, state := range doc.States {
		out = append(out, flattenSnapshot(doc.LabelPath, nil, "", nil, "", "", state, observedAt)...)
	}
	return out
}

func StateDisplayDocumentKey(doc statekit.StateDisplayDocument) string {
	return hashJSON(map[string]any{
		"kind":       doc.Kind,
		"label_path": doc.LabelPath,
	})
}

func flattenSnapshot(labelPath []statekit.StateDisplayLabel, path []string, parentIdentity string, inheritedLabels map[string]string, inheritedScrapedFrom, inheritedScrapePath string, snap statekit.Snapshot, observedAt time.Time) []flattenedState {
	statePath := append(append([]string(nil), path...), snap.Name)
	labels := labelsForSnapshot(labelPath, inheritedLabels, snap)
	scrapedFrom := firstNonEmpty(snap.ScrapedFrom, inheritedScrapedFrom)
	scrapePath := firstNonEmpty(snap.ScrapePath, inheritedScrapePath)
	identity := identityFor(labelPath, statePath, scrapedFrom, scrapePath, snap.Name)
	nowLabels := cloneLabels(labels)
	groupName := nowLabels["group_name"]
	data := cloneData(snap.Data)
	metadataHash := hashJSON(map[string]any{
		"parent_identity": parentIdentity,
		"name":            snap.Name,
		"scraped_from":    scrapedFrom,
		"scrape_path":     scrapePath,
		"importance":      snap.Importance.String(),
		"help":            snap.Help,
		"group_name":      groupName,
		"labels":          nowLabels,
	})
	eventKey := hashJSON(map[string]any{
		"identity":   identity,
		"changed_at": snap.ChangedAt,
		"updated_at": snap.UpdatedAt,
		"status":     snap.Status.String(),
		"reason":     snap.Reason,
	})
	entry := flattenedState{
		Node: StateNode{
			Identity:       identity,
			ParentIdentity: parentIdentity,
			Name:           snap.Name,
			ScrapedFrom:    scrapedFrom,
			ScrapePath:     scrapePath,
			Importance:     snap.Importance.String(),
			Help:           snap.Help,
			GroupName:      groupName,
			LabelPath:      append([]statekit.StateDisplayLabel(nil), labelPath...),
			Labels:         nowLabels,
			FirstSeenAt:    observedAt,
			LastSeenAt:     observedAt,
			MetadataHash:   metadataHash,
		},
		Current: CurrentObservation{
			Identity:       identity,
			Status:         snap.Status.String(),
			Reason:         snap.Reason,
			ChangedAt:      snap.ChangedAt,
			ChangedSecsAgo: snap.ChangedSecsAgo,
			UpdatedAt:      snap.UpdatedAt,
			UpdatedSecsAgo: snap.UpdatedSecsAgo,
			ObservedAt:     observedAt,
			Data:           data,
			DataHash:       hashJSON(data),
		},
		Event: StateEvent{
			EventKey:   eventKey,
			Identity:   identity,
			Status:     snap.Status.String(),
			Reason:     snap.Reason,
			ChangedAt:  snap.ChangedAt,
			UpdatedAt:  snap.UpdatedAt,
			ObservedAt: observedAt,
			Data:       data,
		},
	}
	out := []flattenedState{entry}
	for _, child := range snap.Checks {
		out = append(out, flattenSnapshot(labelPath, statePath, identity, nowLabels, scrapedFrom, scrapePath, child, observedAt)...)
	}
	return out
}

func labelsForSnapshot(labelPath []statekit.StateDisplayLabel, inherited map[string]string, snap statekit.Snapshot) map[string]string {
	labels := map[string]string{}
	for k, v := range inherited {
		labels[k] = v
	}
	for _, label := range labelPath {
		if _, ok := labels[label.Name]; !ok {
			labels[label.Name] = label.Value
		}
	}
	if snap.Data != nil {
		if dataLabels, ok := snap.Data["labels"]; ok {
			for k, v := range labelsFromAny(dataLabels) {
				labels[k] = v
			}
		}
	}
	return labels
}

func labelsFromAny(value any) map[string]string {
	out := map[string]string{}
	switch labels := value.(type) {
	case map[string]string:
		for k, v := range labels {
			out[k] = v
		}
	case map[string]any:
		for k, v := range labels {
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}

func labelsFromLabelPath(path []statekit.StateDisplayLabel) map[string]string {
	out := map[string]string{}
	for _, label := range path {
		out[label.Name] = label.Value
	}
	return out
}

func identityFor(labelPath []statekit.StateDisplayLabel, statePath []string, scrapedFrom, scrapePath, snapshotName string) string {
	return hashJSON(map[string]any{
		"label_path":    labelPath,
		"state_path":    statePath,
		"scraped_from":  scrapedFrom,
		"scrape_path":   scrapePath,
		"snapshot_name": snapshotName,
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func matchesFilter(item CurrentState, filter CurrentFilter) bool {
	if filter.Status != "" && item.Observation.Status != filter.Status {
		return false
	}
	if filter.GroupName != "" && item.GroupName != filter.GroupName {
		return false
	}
	for k, v := range filter.Labels {
		if item.Labels[k] != v {
			return false
		}
	}
	return true
}

func groupValues(item CurrentState, by []string) map[string]string {
	values := map[string]string{}
	for _, key := range by {
		switch {
		case key == "group_name":
			values[key] = item.GroupName
		case key == "status":
			values[key] = item.Observation.Status
		case key == "importance":
			values[key] = item.Importance
		case key == "scraped_from":
			values[key] = item.ScrapedFrom
		case strings.HasPrefix(key, "label:"):
			name := strings.TrimPrefix(key, "label:")
			values[key] = item.Labels[name]
		default:
			values[key] = item.Labels[key]
		}
	}
	return values
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeStringLabels(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneData(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func incidentIdentity(source, id string, createdAt time.Time) string {
	return hashJSON(map[string]any{"source": source, "incident_id": id, "created_at": createdAt})
}

func sortedIncidentEvents(events map[string]IncidentEvent) []IncidentEvent {
	out := make([]IncidentEvent, 0, len(events))
	for _, event := range events {
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool {
		left, leftErr := parseIncidentSeq(out[i].Seq)
		right, rightErr := parseIncidentSeq(out[j].Seq)
		if leftErr == nil && rightErr == nil && left != right {
			return left < right
		}
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].EventKey < out[j].EventKey
	})
	return out
}

func parseIncidentSeq(seq string) (uint64, error) {
	if _, after, ok := strings.Cut(seq, ":"); ok {
		seq = after
	}
	return strconv.ParseUint(seq, 10, 64)
}

func stableString(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func hashJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte(fmt.Sprint(value))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func statusRank(status string) int {
	switch status {
	case statekit.Down.String():
		return 4
	case statekit.Fail.String():
		return 3
	case statekit.Warn.String():
		return 2
	case statekit.Pass.String():
		return 1
	default:
		return 0
	}
}
