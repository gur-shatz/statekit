// Package storage stores statekit display documents in a query-friendly shape.
//
// Data is organized in three layers by access pattern:
//
//   - L1: current target summaries and per-state headers, no data payloads.
//     Polled constantly, replaced in place on ingest, O(fleet).
//   - L2: full current state detail including data. Fetched per target or per
//     state on drill-down, replaced in place on ingest, O(fleet x data size).
//   - L3: historical and bounded: per-identity transition rings, the charting
//     store (chart.go), and incidents with retention.
//
// L1 and L2 are replaced on every ingest, so their size is a function of
// fleet size, not of time. Only L3 grows with time and every L3 structure is
// explicitly bounded, so worst-case memory is computable from configuration.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

// ErrNotFound is wrapped by reads whose subject does not exist in the store.
var ErrNotFound = errors.New("not found")

// Store is the storage boundary. Backends should treat IngestDocument as the
// entry point for every state document received from a component or scraper.
type Store interface {
	IngestDocument(ctx context.Context, doc statekit.StateDisplayDocument, observedAt time.Time) error
	IngestEscalations(ctx context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error
	UpsertMute(ctx context.Context, mute StateMute) (StateMute, error)
	DeleteMute(ctx context.Context, identity string) error
	Mutes(ctx context.Context) ([]StateMute, error)

	// L1: polled constantly, no data payloads.
	Summary(ctx context.Context) (FleetSummary, error)
	Targets(ctx context.Context) ([]TargetSummary, error)
	// L2: fetched on drill-down, carries the heavy fields.
	TargetDetail(ctx context.Context, key string) (TargetDetail, error)
	StateDetail(ctx context.Context, identity string) (StateDetail, error)
	// L3: historical, bounded.
	StateTimeline(ctx context.Context, identity string) (StateTimeline, error)

	Incidents(ctx context.Context, filter IncidentFilter) ([]Incident, error)
	AcknowledgeIncident(ctx context.Context, source, id string, at time.Time) error
}

// FleetSummary is the L1 fleet rollup served by GET /state/summary. FleetHash
// is its ETag: the hash of the sorted (key, material_hash) pairs, recomputed
// at ingest.
type FleetSummary struct {
	WorstStatus  string            `json:"worst_status"`
	StatusCounts map[string]int    `json:"status_counts"` // by state
	Targets      FleetTargetCounts `json:"targets"`
	FleetHash    string            `json:"fleet_hash"`
	ObservedAt   time.Time         `json:"observed_at"`
}

type FleetTargetCounts struct {
	Total    int            `json:"total"`
	ByStatus map[string]int `json:"by_status"` // by target worst_status
}

// TargetSummary is one L1 rail entry. GET /state/targets returns the fleet's
// summaries, each carrying the flat StateHeaders for its states.
type TargetSummary struct {
	Key            string            `json:"key"`
	Name           string            `json:"name"`
	ScrapePath     string            `json:"scrape_path"`
	Labels         map[string]string `json:"labels,omitempty"`
	WorstStatus    string            `json:"worst_status"`
	StatusCounts   map[string]int    `json:"status_counts"`
	AffectedStates []AffectedState   `json:"affected_states,omitempty"`
	MaterialHash   string            `json:"material_hash"` // ETag for the L2 detail
	ObservedAt     time.Time         `json:"observed_at"`
	States         []StateHeader     `json:"states"`
}

// StateHeader is the light per-state row: enough for the states column,
// nothing heavy. State lists are flat with parent pointers, not nested trees.
type StateHeader struct {
	Identity       string     `json:"identity"`
	ParentIdentity string     `json:"parent_identity,omitempty"`
	TargetKey      string     `json:"target_key,omitempty"`
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	OriginalStatus string     `json:"original_status,omitempty"`
	Reason         string     `json:"reason,omitempty"`
	OriginalReason string     `json:"original_reason,omitempty"`
	Importance     string     `json:"importance"`
	ChangedAt      time.Time  `json:"changed_at"`
	Mute           *StateMute `json:"mute,omitempty"`
}

type AffectedState struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Reason    string    `json:"reason,omitempty"`
	ChangedAt time.Time `json:"changed_at"`
}

// TargetDetail is the L2 response of GET /state/targets/{key}: the summary
// plus full state details.
type TargetDetail struct {
	TargetSummary
	Details []StateDetail `json:"details"`
}

// StateDetail is the L2 view of one state, served by GET /state/states/{identity}.
type StateDetail struct {
	StateHeader
	ScrapedFrom string                       `json:"scraped_from,omitempty"`
	ScrapePath  string                       `json:"scrape_path,omitempty"`
	Help        string                       `json:"help,omitempty"`
	GroupName   string                       `json:"group_name,omitempty"`
	Labels      map[string]string            `json:"labels,omitempty"`
	LabelPath   []statekit.StateDisplayLabel `json:"label_path,omitempty"`
	Data        map[string]any               `json:"data,omitempty"`
	DataHash    string                       `json:"data_hash"` // ETag component
	UpdatedAt   time.Time                    `json:"updated_at,omitempty"`
	ObservedAt  time.Time                    `json:"observed_at"`
	FirstSeenAt time.Time                    `json:"first_seen_at"`
	LastSeenAt  time.Time                    `json:"last_seen_at"`
	Children    []string                     `json:"children,omitempty"` // child identities, filled on read
}

// Transition is one L3 ring entry. Its identity is
// identity + changed_at + status; updated_at and reason are deliberately not
// part of it, so scrapes of an unchanged state contribute nothing.
type Transition struct {
	ChangedAt time.Time `json:"changed_at"`
	Status    string    `json:"status"`
	Reason    string    `json:"reason,omitempty"`
}

// StateTimeline is the response of GET /state/states/{identity}/timeline.
type StateTimeline struct {
	Identity    string       `json:"identity"`
	Transitions []Transition `json:"transitions"` // newest first
}

// StateMute caps one state's effective status until ExpiresAt. Status is the
// maximum visible status while the mute is active; use "pass" to force-pass.
// Stored details are never mutated by mutes; the visible view is derived from
// raw detail + mute on materialization and reads. OriginalStatus reports the
// state's current raw status wherever a mute is returned.
type StateMute struct {
	Identity       string    `json:"identity"`
	TargetKey      string    `json:"target_key,omitempty"`
	Name           string    `json:"name,omitempty"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	OriginalStatus string    `json:"original_status,omitempty"`
}

type Incident struct {
	Identity      string                       `json:"identity"`
	Source        string                       `json:"source"`
	ScrapedFrom   string                       `json:"scraped_from,omitempty"`
	ScrapePath    string                       `json:"scrape_path,omitempty"`
	ID            string                       `json:"id"`
	Type          string                       `json:"type,omitempty"`
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
	Type   string `json:"type,omitempty"`
}

const (
	defaultTransitionRingCap  = 32
	defaultTransitionBackstop = 100_000
	defaultClosedIncidentTTL  = 24 * time.Hour
	defaultIncidentEventCap   = 100
	defaultEvictionFallback   = 10 * time.Minute
	// evictionIntervalFactor scales the observed ingest interval of a source
	// into its liveness TTL: a source is evicted after missing this many of
	// its own ingest cycles.
	evictionIntervalFactor = 10
)

var _ Store = (*MemoryStore)(nil)

type MemoryStore struct {
	mu sync.RWMutex

	// L1
	targets map[string]TargetSummary
	// L2
	details map[string]StateDetail
	// L3
	timelines       map[string]*transitionRing
	transitionTotal int
	chart           ChartStore
	journal         *Journal
	mutes           map[string]StateMute
	incidents       map[string]Incident
	incidentEvents  map[string]map[string]IncidentEvent
	incidentIndex   map[string]map[string]struct{} // source+"\n"+id -> incident identities

	docScopeIdentities map[string]map[string]struct{}
	docScopeTargets    map[string]map[string]struct{}
	docLastSeen        map[string]time.Time
	docIntervals       map[string]time.Duration

	fleetHash  string
	observedAt time.Time

	docCache DocumentCache[statekit.StateDisplayDocument]
	docTTL   time.Duration

	transitionCap      int
	transitionBackstop int
	closedIncidentTTL  time.Duration
	incidentEventCap   int
	evictionFallback   time.Duration
}

type MemoryStoreOption func(*MemoryStore)

func WithDocumentCache(cache DocumentCache[statekit.StateDisplayDocument], ttl time.Duration) MemoryStoreOption {
	return func(s *MemoryStore) {
		s.docCache = cache
		s.docTTL = ttl
	}
}

// WithTransitionRing bounds L3 transition storage: ringCap transitions per
// identity plus a global backstop across all identities.
func WithTransitionRing(ringCap, backstop int) MemoryStoreOption {
	return func(s *MemoryStore) {
		if ringCap > 0 {
			s.transitionCap = ringCap
		}
		if backstop > 0 {
			s.transitionBackstop = backstop
		}
	}
}

// WithIncidentRetention bounds incident storage: closed incidents are dropped
// closedTTL after their last update, and each incident keeps at most eventCap
// of its newest events.
func WithIncidentRetention(closedTTL time.Duration, eventCap int) MemoryStoreOption {
	return func(s *MemoryStore) {
		if closedTTL > 0 {
			s.closedIncidentTTL = closedTTL
		}
		if eventCap > 0 {
			s.incidentEventCap = eventCap
		}
	}
}

// WithChartStore replaces the charting store backend.
func WithChartStore(chart ChartStore) MemoryStoreOption {
	return func(s *MemoryStore) {
		if chart != nil {
			s.chart = chart
		}
	}
}

// WithJournal persists L3 history (transitions and incidents) to the given
// journal. NewMemoryStore replays it before serving, so history survives
// restarts; every ring cap, dedup rule, and incident TTL still applies.
func WithJournal(journal *Journal) MemoryStoreOption {
	return func(s *MemoryStore) {
		s.journal = journal
	}
}

// WithEvictionFallback sets the liveness TTL used for sources whose ingest
// interval has not been observed yet.
func WithEvictionFallback(ttl time.Duration) MemoryStoreOption {
	return func(s *MemoryStore) {
		if ttl > 0 {
			s.evictionFallback = ttl
		}
	}
}

func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	store := &MemoryStore{
		targets:            map[string]TargetSummary{},
		details:            map[string]StateDetail{},
		timelines:          map[string]*transitionRing{},
		chart:              NewMemoryChartStore(time.Minute, 24*60),
		mutes:              map[string]StateMute{},
		incidents:          map[string]Incident{},
		incidentEvents:     map[string]map[string]IncidentEvent{},
		incidentIndex:      map[string]map[string]struct{}{},
		docScopeIdentities: map[string]map[string]struct{}{},
		docScopeTargets:    map[string]map[string]struct{}{},
		docLastSeen:        map[string]time.Time{},
		docIntervals:       map[string]time.Duration{},
		transitionCap:      defaultTransitionRingCap,
		transitionBackstop: defaultTransitionBackstop,
		closedIncidentTTL:  defaultClosedIncidentTTL,
		incidentEventCap:   defaultIncidentEventCap,
		evictionFallback:   defaultEvictionFallback,
	}
	for _, opt := range opts {
		opt(store)
	}
	if store.journal != nil {
		store.replayJournal()
	}
	return store
}

// replayJournal rehydrates L3 from the journal, then compacts the file back
// to exactly the live state so stale and duplicate lines drop. Transitions
// replay through the normal ring path (caps and dedup apply); identities
// whose newest transition is older than the journal retention are skipped,
// so identity churn cannot accumulate orphaned rings across restarts.
func (this *MemoryStore) replayJournal() {
	journal := this.journal
	this.journal = nil // appends during replay would echo entries back
	entries, err := journal.read()
	if err != nil {
		journal.setErr(err)
		this.journal = journal
		return
	}
	now := time.Now()
	cutoff := now.Add(-journal.retention)

	transitions := map[string][]Transition{}
	for _, entry := range entries {
		switch {
		case entry.Kind == "transition" && entry.Transition != nil && entry.Identity != "":
			transitions[entry.Identity] = append(transitions[entry.Identity], *entry.Transition)
		case entry.Kind == "incident" && entry.Incident != nil:
			this.replayIncident(*entry.Incident)
		case entry.Kind == "mute" && entry.Mute != nil:
			this.replayMute(*entry.Mute, now)
		}
	}
	for identity, list := range transitions {
		newest := list[0].ChangedAt
		for _, transition := range list {
			if transition.ChangedAt.After(newest) {
				newest = transition.ChangedAt
			}
		}
		if newest.Before(cutoff) {
			continue
		}
		this.appendTransitions(identity, list)
	}
	this.sweepClosedIncidents(now)
	this.journal = journal
	this.compactJournal()
}

func (this *MemoryStore) replayMute(mute StateMute, now time.Time) {
	if mute.Identity == "" || !mute.ExpiresAt.After(now) {
		return
	}
	this.mutes[mute.Identity] = mute
}

// replayIncident restores one journaled incident upsert; later entries for
// the same incident win, matching ingest semantics.
func (this *MemoryStore) replayIncident(incident Incident) {
	events := map[string]IncidentEvent{}
	for _, event := range incident.Events {
		events[event.EventKey] = event
	}
	this.incidentEvents[incident.Identity] = events
	this.incidents[incident.Identity] = incident
	this.indexIncident(incident)
}

// compactJournal rewrites the journal from live state. Callers hold the
// write lock (or run before the store is shared).
func (this *MemoryStore) compactJournal() {
	var entries []journalEntry
	for identity, ring := range this.timelines {
		for _, transition := range ring.entries {
			t := transition
			entries = append(entries, journalEntry{Kind: "transition", Identity: identity, Transition: &t})
		}
	}
	for identity, incident := range this.incidents {
		incident.Events = sortedIncidentEvents(this.incidentEvents[identity])
		in := incident
		entries = append(entries, journalEntry{Kind: "incident", Incident: &in})
	}
	now := time.Now()
	for identity, mute := range this.mutes {
		if mute.ExpiresAt.After(now) {
			m := mute
			entries = append(entries, journalEntry{Kind: "mute", Identity: identity, Mute: &m})
		}
	}
	_ = this.journal.rewrite(entries)
}

// Chart exposes the charting store so its endpoints can be mounted beside the
// state API.
func (this *MemoryStore) Chart() ChartStore {
	return this.chart
}

func (this *MemoryStore) IngestDocument(ctx context.Context, doc statekit.StateDisplayDocument, observedAt time.Time) error {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	if err := this.cacheDocument(ctx, doc); err != nil {
		return err
	}
	entries := flattenDocument(doc, observedAt)
	docKey := StateDisplayDocumentKey(doc)

	newIdentities := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		newIdentities[entry.Detail.Identity] = struct{}{}
	}
	now := time.Now()
	this.mu.Lock()
	this.sweepExpiredMutes(now)
	visibleEntries := applyMutes(entries, this.mutes, now)
	summaries := buildTargetSummaries(visibleEntries)
	newTargets := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		newTargets[summary.Key] = struct{}{}
	}
	for identity := range this.docScopeIdentities[docKey] {
		if _, kept := newIdentities[identity]; kept {
			continue
		}
		this.dropIdentity(identity)
	}
	for key := range this.docScopeTargets[docKey] {
		if _, kept := newTargets[key]; kept {
			continue
		}
		delete(this.targets, key)
	}

	for i, entry := range entries {
		// Stored details stay raw; mutes are derived on read. TargetKey is
		// stamped by buildTargetSummaries on the visible copy, so carry it over.
		detail := entry.Detail
		detail.TargetKey = visibleEntries[i].Detail.TargetKey
		if existing, ok := this.details[detail.Identity]; ok {
			detail.FirstSeenAt = existing.FirstSeenAt
		}
		this.details[detail.Identity] = detail
		this.appendTransitions(detail.Identity, entry.Transitions)
	}
	for _, summary := range summaries {
		this.targets[summary.Key] = summary
	}
	this.docScopeIdentities[docKey] = newIdentities
	this.docScopeTargets[docKey] = newTargets
	if last, ok := this.docLastSeen[docKey]; ok {
		if delta := observedAt.Sub(last); delta > 0 {
			this.docIntervals[docKey] = delta
		}
	}
	this.docLastSeen[docKey] = observedAt
	this.sweepExpiredScopes(docKey, observedAt)
	this.sweepClosedIncidents(observedAt)
	this.refreshFleetRollup(observedAt)
	if this.journal != nil && this.journal.needsCompaction() {
		this.compactJournal()
	}
	this.mu.Unlock()

	this.chart.Record(observedAt, triggeringStates(visibleEntries))
	return nil
}

func (this *MemoryStore) UpsertMute(_ context.Context, mute StateMute) (StateMute, error) {
	if mute.Identity == "" {
		return StateMute{}, fmt.Errorf("missing identity")
	}
	if _, err := statekit.ParseStatus(mute.Status); err != nil {
		return StateMute{}, err
	}
	now := time.Now()
	if mute.CreatedAt.IsZero() {
		mute.CreatedAt = now
	}
	mute.UpdatedAt = now
	if !mute.ExpiresAt.After(now) {
		return StateMute{}, fmt.Errorf("expires_at must be in the future")
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	if detail, ok := this.details[mute.Identity]; ok {
		mute.TargetKey = firstNonEmpty(mute.TargetKey, detail.TargetKey)
		mute.Name = firstNonEmpty(mute.Name, detail.Name)
		mute.OriginalStatus = detail.Status
	}
	this.mutes[mute.Identity] = mute
	this.rebuildCurrentLayersLocked(now)
	if this.journal != nil {
		this.journal.appendMute(mute)
		if this.journal.needsCompaction() {
			this.compactJournal()
		}
	}
	return mute, nil
}

func (this *MemoryStore) DeleteMute(_ context.Context, identity string) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if _, ok := this.mutes[identity]; !ok {
		return nil
	}
	delete(this.mutes, identity)
	this.rebuildCurrentLayersLocked(time.Now())
	if this.journal != nil {
		this.compactJournal()
	}
	return nil
}

func (this *MemoryStore) Mutes(_ context.Context) ([]StateMute, error) {
	now := time.Now()
	this.refreshExpiredMutes(now)
	this.mu.RLock()
	defer this.mu.RUnlock()
	out := make([]StateMute, 0, len(this.mutes))
	for _, mute := range this.mutes {
		// OriginalStatus tracks the state's current raw status, not a
		// snapshot from mute time.
		if detail, ok := this.details[mute.Identity]; ok {
			mute.OriginalStatus = detail.Status
		}
		out = append(out, mute)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ExpiresAt.Equal(out[j].ExpiresAt) {
			return out[i].ExpiresAt.Before(out[j].ExpiresAt)
		}
		return out[i].Identity < out[j].Identity
	})
	return out, nil
}

func (this *MemoryStore) sweepExpiredMutes(now time.Time) bool {
	expired := false
	for identity, mute := range this.mutes {
		if !mute.ExpiresAt.After(now) {
			delete(this.mutes, identity)
			expired = true
		}
	}
	return expired
}

// refreshExpiredMutes drops expired mutes and rematerializes the L1 layers so
// the fleet hash moves when a mute lapses. The read-lock pre-check keeps the
// common polling path (no mutes, or none expired) off the write lock.
func (this *MemoryStore) refreshExpiredMutes(now time.Time) {
	this.mu.RLock()
	expired := false
	for _, mute := range this.mutes {
		if !mute.ExpiresAt.After(now) {
			expired = true
			break
		}
	}
	this.mu.RUnlock()
	if !expired {
		return
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.sweepExpiredMutes(now) {
		this.rebuildCurrentLayersLocked(now)
		if this.journal != nil {
			this.compactJournal()
		}
	}
}

// rebuildCurrentLayersLocked rematerializes the L1 layers (target summaries
// and the fleet hash) from raw details plus active mutes. Stored details are
// never touched; the muted view is derived here and on single-state reads.
// Callers hold the write lock.
func (this *MemoryStore) rebuildCurrentLayersLocked(now time.Time) {
	this.sweepExpiredMutes(now)
	if len(this.details) == 0 {
		return
	}
	entries := make([]flattenedState, 0, len(this.details))
	for _, detail := range this.details {
		entries = append(entries, flattenedState{Detail: detail})
	}
	entries = applyMutes(entries, this.mutes, now)
	this.targets = map[string]TargetSummary{}
	for _, summary := range buildTargetSummaries(entries) {
		this.targets[summary.Key] = summary
	}
	this.refreshFleetRollup(now)
}

// dropIdentity removes a state from every current layer and its transition
// ring. Callers hold the write lock.
func (this *MemoryStore) dropIdentity(identity string) {
	delete(this.details, identity)
	if ring := this.timelines[identity]; ring != nil {
		this.transitionTotal -= len(ring.entries)
		delete(this.timelines, identity)
	}
}

// appendTransitions merges new transitions into the identity's ring, keeping
// per-identity and global bounds. Callers hold the write lock.
func (this *MemoryStore) appendTransitions(identity string, transitions []Transition) {
	if len(transitions) == 0 {
		return
	}
	ring := this.timelines[identity]
	if ring == nil {
		ring = &transitionRing{}
		this.timelines[identity] = ring
	}
	for _, transition := range transitions {
		added, dropped := ring.add(transition, this.transitionCap)
		if added {
			this.transitionTotal++
			if this.journal != nil {
				this.journal.appendTransition(identity, transition)
			}
		}
		this.transitionTotal -= dropped
	}
	for this.transitionTotal > this.transitionBackstop {
		if !this.evictOldestTransition() {
			break
		}
	}
}

// evictOldestTransition drops the globally oldest transition across all
// rings. Callers hold the write lock.
func (this *MemoryStore) evictOldestTransition() bool {
	var oldestIdentity string
	var oldest time.Time
	for identity, ring := range this.timelines {
		if len(ring.entries) == 0 {
			continue
		}
		if oldestIdentity == "" || ring.entries[0].ChangedAt.Before(oldest) {
			oldestIdentity = identity
			oldest = ring.entries[0].ChangedAt
		}
	}
	if oldestIdentity == "" {
		return false
	}
	ring := this.timelines[oldestIdentity]
	ring.entries = ring.entries[1:]
	this.transitionTotal--
	if len(ring.entries) == 0 {
		delete(this.timelines, oldestIdentity)
	}
	return true
}

// sweepExpiredScopes evicts every document scope whose source has gone
// silent: not seen for evictionIntervalFactor times its observed ingest
// interval, or the fallback TTL when the interval is unknown. Callers hold
// the write lock.
func (this *MemoryStore) sweepExpiredScopes(currentDocKey string, now time.Time) {
	for docKey, lastSeen := range this.docLastSeen {
		if docKey == currentDocKey {
			continue
		}
		ttl := this.evictionFallback
		if interval, ok := this.docIntervals[docKey]; ok {
			ttl = interval * evictionIntervalFactor
		}
		if now.Sub(lastSeen) <= ttl {
			continue
		}
		for identity := range this.docScopeIdentities[docKey] {
			this.dropIdentity(identity)
		}
		for key := range this.docScopeTargets[docKey] {
			delete(this.targets, key)
		}
		delete(this.docScopeIdentities, docKey)
		delete(this.docScopeTargets, docKey)
		delete(this.docLastSeen, docKey)
		delete(this.docIntervals, docKey)
	}
}

// refreshFleetRollup recomputes the fleet hash from the sorted
// (key, material_hash) pairs. Callers hold the write lock.
func (this *MemoryStore) refreshFleetRollup(observedAt time.Time) {
	pairs := make([][2]string, 0, len(this.targets))
	for key, target := range this.targets {
		pairs = append(pairs, [2]string{key, target.MaterialHash})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	this.fleetHash = hashJSON(pairs)
	if observedAt.After(this.observedAt) {
		this.observedAt = observedAt
	}
}

func (this *MemoryStore) IngestEscalations(_ context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	labels := labelsFromLabelPath(doc.LabelPath)
	this.mu.Lock()
	defer this.mu.Unlock()
	for _, in := range doc.Incidents {
		origin := firstNonEmpty(in.ScrapedFrom, source)
		scrapePath := firstNonEmpty(in.ScrapePath, source)
		incidentLabels := mergeStringLabels(labels, in.Labels)
		identity := incidentIdentity(origin, in.ID, in.CreatedAt)
		incident := this.incidents[identity]
		incident.Identity = identity
		incident.Source = origin
		incident.ScrapedFrom = origin
		incident.ScrapePath = scrapePath
		incident.ID = in.ID
		incident.Type = in.Type
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
		events := this.incidentEvents[identity]
		if events == nil {
			events = map[string]IncidentEvent{}
			this.incidentEvents[identity] = events
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
		incident.Events = this.capIncidentEvents(events)
		this.incidents[identity] = incident
		this.indexIncident(incident)
		if this.journal != nil {
			this.journal.appendIncident(incident)
		}
	}
	this.sweepClosedIncidents(observedAt)
	if this.journal != nil && this.journal.needsCompaction() {
		this.compactJournal()
	}
	return nil
}

// capIncidentEvents trims an incident's event map to the newest
// incidentEventCap entries and returns them sorted. Callers hold the write
// lock.
func (this *MemoryStore) capIncidentEvents(events map[string]IncidentEvent) []IncidentEvent {
	sorted := sortedIncidentEvents(events)
	if len(sorted) <= this.incidentEventCap {
		return sorted
	}
	for _, dropped := range sorted[:len(sorted)-this.incidentEventCap] {
		delete(events, dropped.EventKey)
	}
	return sorted[len(sorted)-this.incidentEventCap:]
}

func (this *MemoryStore) indexIncident(incident Incident) {
	key := incidentIndexKey(incident.Source, incident.ID)
	identities := this.incidentIndex[key]
	if identities == nil {
		identities = map[string]struct{}{}
		this.incidentIndex[key] = identities
	}
	identities[incident.Identity] = struct{}{}
}

// sweepClosedIncidents drops closed incidents whose last update is older than
// the closed-incident TTL. Callers hold the write lock.
func (this *MemoryStore) sweepClosedIncidents(now time.Time) {
	for identity, incident := range this.incidents {
		if incident.Status != statekit.EscalationClosed {
			continue
		}
		lastActivity := incident.LastUpdatedAt
		if incident.ObservedAt.After(lastActivity) {
			lastActivity = incident.ObservedAt
		}
		if now.Sub(lastActivity) <= this.closedIncidentTTL {
			continue
		}
		delete(this.incidents, identity)
		delete(this.incidentEvents, identity)
		key := incidentIndexKey(incident.Source, incident.ID)
		if identities := this.incidentIndex[key]; identities != nil {
			delete(identities, identity)
			if len(identities) == 0 {
				delete(this.incidentIndex, key)
			}
		}
	}
}

func incidentIndexKey(source, id string) string {
	return source + "\n" + id
}

func (this *MemoryStore) CachedDocumentYAML(ctx context.Context, key string) ([]byte, bool, error) {
	if this.docCache == nil {
		return nil, false, nil
	}
	return this.docCache.GetYAML(ctx, key)
}

func (this *MemoryStore) cacheDocument(ctx context.Context, doc statekit.StateDisplayDocument) error {
	if this.docCache == nil {
		return nil
	}
	return this.docCache.Set(ctx, StateDisplayDocumentKey(doc), doc, this.docTTL)
}

func (this *MemoryStore) Summary(_ context.Context) (FleetSummary, error) {
	now := time.Now()
	this.refreshExpiredMutes(now)
	this.mu.RLock()
	defer this.mu.RUnlock()

	out := FleetSummary{
		WorstStatus:  statekit.Pass.String(),
		StatusCounts: map[string]int{},
		Targets:      FleetTargetCounts{ByStatus: map[string]int{}},
		FleetHash:    this.fleetHash,
		ObservedAt:   this.observedAt,
	}
	for _, detail := range this.details {
		status := detail.Status
		if mute, ok := this.mutes[detail.Identity]; ok && mute.ExpiresAt.After(now) {
			status = capStatus(status, mute.Status)
		}
		out.StatusCounts[status]++
		if statusRank(status) > statusRank(out.WorstStatus) {
			out.WorstStatus = status
		}
	}
	for _, target := range this.targets {
		out.Targets.Total++
		out.Targets.ByStatus[target.WorstStatus]++
	}
	return out, nil
}

func (this *MemoryStore) Targets(_ context.Context) ([]TargetSummary, error) {
	this.refreshExpiredMutes(time.Now())
	this.mu.RLock()
	defer this.mu.RUnlock()

	out := make([]TargetSummary, 0, len(this.targets))
	for _, target := range this.targets {
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

func (this *MemoryStore) TargetDetail(_ context.Context, key string) (TargetDetail, error) {
	now := time.Now()
	this.refreshExpiredMutes(now)
	this.mu.RLock()
	defer this.mu.RUnlock()

	summary, ok := this.targets[key]
	if !ok {
		return TargetDetail{}, fmt.Errorf("target %q: %w", key, ErrNotFound)
	}
	out := TargetDetail{TargetSummary: summary, Details: make([]StateDetail, 0, len(summary.States))}
	for _, header := range summary.States {
		if detail, ok := this.details[header.Identity]; ok {
			out.Details = append(out.Details, this.muteViewLocked(detail, now))
		}
	}
	return out, nil
}

func (this *MemoryStore) StateDetail(_ context.Context, identity string) (StateDetail, error) {
	now := time.Now()
	this.refreshExpiredMutes(now)
	this.mu.RLock()
	defer this.mu.RUnlock()

	detail, ok := this.details[identity]
	if !ok {
		return StateDetail{}, fmt.Errorf("state %q: %w", identity, ErrNotFound)
	}
	detail = this.muteViewLocked(detail, now)
	for childIdentity, child := range this.details {
		if child.ParentIdentity == identity {
			detail.Children = append(detail.Children, childIdentity)
		}
	}
	sort.Strings(detail.Children)
	return detail, nil
}

func (this *MemoryStore) StateTimeline(_ context.Context, identity string) (StateTimeline, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	out := StateTimeline{Identity: identity, Transitions: []Transition{}}
	ring := this.timelines[identity]
	if ring == nil {
		if _, known := this.details[identity]; !known {
			return StateTimeline{}, fmt.Errorf("state %q: %w", identity, ErrNotFound)
		}
		return out, nil
	}
	for i := len(ring.entries) - 1; i >= 0; i-- {
		out.Transitions = append(out.Transitions, ring.entries[i])
	}
	return out, nil
}

// muteView derives the visible view of one raw detail under an active mute.
// The mute is always attached so a preemptive mute (one that does not change
// the current status) is still visible and clearable; original_status is set
// only when the cap actually changed the status.
func muteView(detail StateDetail, mute StateMute, now time.Time) StateDetail {
	if !mute.ExpiresAt.After(now) {
		return detail
	}
	mute.OriginalStatus = detail.Status
	if capped := capStatus(detail.Status, mute.Status); capped != detail.Status {
		detail.OriginalStatus = detail.Status
		detail.OriginalReason = detail.Reason
		detail.Status = capped
		if mute.Reason != "" {
			detail.Reason = mute.Reason
		} else if capped == statekit.Pass.String() {
			detail.Reason = "muted: forced pass"
		} else {
			detail.Reason = "muted: capped at " + capped
		}
	}
	detail.Mute = &mute
	return detail
}

// muteViewLocked resolves the visible view of one stored detail. Callers hold
// at least the read lock.
func (this *MemoryStore) muteViewLocked(detail StateDetail, now time.Time) StateDetail {
	if mute, ok := this.mutes[detail.Identity]; ok {
		return muteView(detail, mute, now)
	}
	return detail
}

func applyMutes(entries []flattenedState, mutes map[string]StateMute, now time.Time) []flattenedState {
	if len(mutes) == 0 {
		return entries
	}
	out := make([]flattenedState, len(entries))
	copy(out, entries)
	for i := range out {
		if mute, ok := mutes[out[i].Detail.Identity]; ok {
			out[i].Detail = muteView(out[i].Detail, mute, now)
		}
	}
	return out
}

func capStatus(current, cap string) string {
	if statusRank(current) <= statusRank(cap) {
		return current
	}
	return cap
}

func (this *MemoryStore) Incidents(_ context.Context, filter IncidentFilter) ([]Incident, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	var candidates []Incident
	if filter.Source != "" && filter.ID != "" {
		for identity := range this.incidentIndex[incidentIndexKey(filter.Source, filter.ID)] {
			candidates = append(candidates, this.incidents[identity])
		}
	} else {
		candidates = make([]Incident, 0, len(this.incidents))
		for _, incident := range this.incidents {
			candidates = append(candidates, incident)
		}
	}

	out := make([]Incident, 0, len(candidates))
	for _, incident := range candidates {
		if filter.Source != "" && incident.Source != filter.Source {
			continue
		}
		if filter.ID != "" && incident.ID != filter.ID {
			continue
		}
		if filter.Status != "" && incident.Status != filter.Status {
			continue
		}
		if filter.Type != "" && incident.Type != filter.Type {
			continue
		}
		incident.Events = sortedIncidentEvents(this.incidentEvents[incident.Identity])
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

func (this *MemoryStore) AcknowledgeIncident(_ context.Context, source, id string, at time.Time) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	var identity string
	var incident Incident
	var ok bool
	for candidateIdentity := range this.incidentIndex[incidentIndexKey(source, id)] {
		candidate := this.incidents[candidateIdentity]
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
	this.incidents[identity] = incident
	if this.journal != nil {
		this.journal.appendIncident(incident)
	}
	return nil
}

// transitionRing holds one identity's transitions in ascending changed_at
// order, bounded by the per-identity cap.
type transitionRing struct {
	entries []Transition
}

// add merges a transition into the ring unless an entry with the same
// changed_at and status exists, then trims to ringCap. It reports whether the
// transition was inserted and how many old entries the trim dropped.
func (this *transitionRing) add(transition Transition, ringCap int) (added bool, dropped int) {
	for _, entry := range this.entries {
		if entry.ChangedAt.Equal(transition.ChangedAt) && entry.Status == transition.Status {
			return false, 0
		}
	}
	insertAt := len(this.entries)
	for insertAt > 0 && this.entries[insertAt-1].ChangedAt.After(transition.ChangedAt) {
		insertAt--
	}
	this.entries = append(this.entries, Transition{})
	copy(this.entries[insertAt+1:], this.entries[insertAt:])
	this.entries[insertAt] = transition
	if len(this.entries) > ringCap {
		dropped = len(this.entries) - ringCap
		this.entries = append([]Transition(nil), this.entries[dropped:]...)
	}
	return true, dropped
}

type flattenedState struct {
	Detail      StateDetail
	Transitions []Transition // ascending by changed_at
	Path        []string
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
	changedAt := snap.ChangedAt
	if changedAt.IsZero() {
		changedAt = observedAt
	}
	entry := flattenedState{
		Detail: StateDetail{
			StateHeader: StateHeader{
				Identity:       identity,
				ParentIdentity: parentIdentity,
				Name:           snap.Name,
				Status:         snap.Status.String(),
				Reason:         snap.Reason,
				Importance:     snap.Importance.String(),
				ChangedAt:      changedAt,
			},
			ScrapedFrom: scrapedFrom,
			ScrapePath:  scrapePath,
			Help:        snap.Help,
			GroupName:   groupName,
			Labels:      nowLabels,
			LabelPath:   append([]statekit.StateDisplayLabel(nil), labelPath...),
			Data:        data,
			DataHash:    hashJSON(data),
			UpdatedAt:   snap.UpdatedAt,
			ObservedAt:  observedAt,
			FirstSeenAt: observedAt,
			LastSeenAt:  observedAt,
		},
		Transitions: snapshotTransitions(snap, changedAt),
		Path:        statePath,
	}
	out := []flattenedState{entry}
	for _, child := range snap.Checks {
		out = append(out, flattenSnapshot(labelPath, statePath, identity, nowLabels, scrapedFrom, scrapePath, child, observedAt)...)
	}
	return out
}

// snapshotTransitions extracts real transitions from a snapshot: its bounded
// History plus the observed changed_at boundary. An unchanged state yields
// the same transitions on every scrape, so ingest dedup holds by
// construction.
func snapshotTransitions(snap statekit.Snapshot, changedAt time.Time) []Transition {
	out := make([]Transition, 0, len(snap.History)+1)
	// History is newest-first; walk backwards to build ascending order.
	for i := len(snap.History) - 1; i >= 0; i-- {
		entry := snap.History[i]
		if entry.Timestamp.IsZero() {
			continue
		}
		out = appendTransition(out, Transition{
			ChangedAt: entry.Timestamp,
			Status:    entry.Status.String(),
			Reason:    entry.Reason,
		})
	}
	return appendTransition(out, Transition{
		ChangedAt: changedAt,
		Status:    snap.Status.String(),
		Reason:    snap.Reason,
	})
}

func appendTransition(transitions []Transition, transition Transition) []Transition {
	for _, existing := range transitions {
		if existing.ChangedAt.Equal(transition.ChangedAt) && existing.Status == transition.Status {
			return transitions
		}
	}
	return append(transitions, transition)
}

// triggeringStates lists the non-pass states of an ingested document for the
// charting store. Label is the display name captured at write time so chart
// reads never join against the current layers.
func triggeringStates(entries []flattenedState) []TriggeringState {
	var out []TriggeringState
	for _, entry := range entries {
		if entry.Detail.Status == statekit.Pass.String() {
			continue
		}
		targetKey, targetName, _ := targetIdentity(entry.Detail)
		label := strings.Join(entry.Path, "/")
		if targetName != "" {
			label = targetName + ":" + label
		}
		out = append(out, TriggeringState{
			Identity:  entry.Detail.Identity,
			TargetKey: targetKey,
			Label:     label,
			Status:    entry.Detail.Status,
		})
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
