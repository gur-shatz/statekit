package statekit

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	EscalationDisplayKind = "statekit.escalation.v1"

	EscalationOpen         = "open"
	EscalationClosed       = "closed"
	EscalationAcknowledged = "acknowledged"
)

type EscalationSource interface {
	EscalationDisplay(after, ack string) EscalationDisplayDocument
}

type EscalationDisplayDocument struct {
	Kind      string               `json:"kind" yaml:"kind"`
	Version   string               `json:"version,omitempty" yaml:"version,omitempty"`
	LabelPath []StateDisplayLabel  `json:"label_path" yaml:"label_path"`
	Watermark string               `json:"watermark,omitempty" yaml:"watermark,omitempty"`
	Incidents []EscalationIncident `json:"incidents" yaml:"incidents"`
}

type EscalationIncident struct {
	ID            string            `json:"id" yaml:"id"`
	ScrapedFrom   string            `json:"scraped_from,omitempty" yaml:"scraped_from,omitempty"`
	ScrapePath    string            `json:"scrape_path,omitempty" yaml:"scrape_path,omitempty"`
	Title         string            `json:"title" yaml:"title"`
	Status        string            `json:"status" yaml:"status"`
	CreatedAt     time.Time         `json:"created_at" yaml:"created_at"`
	ExpiresAt     time.Time         `json:"expires_at" yaml:"expires_at"`
	LastUpdatedAt time.Time         `json:"last_updated_at" yaml:"last_updated_at"`
	Severity      Status            `json:"severity,omitempty" yaml:"severity,omitempty"`
	Labels        map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Topics        map[string]any    `json:"topics,omitempty" yaml:"topics,omitempty"`
	Events        []EscalationEvent `json:"events,omitempty" yaml:"events,omitempty"`
}

type EscalationEvent struct {
	Seq       string         `json:"seq" yaml:"seq"`
	Timestamp time.Time      `json:"timestamp" yaml:"timestamp"`
	Topic     string         `json:"topic" yaml:"topic"`
	Message   string         `json:"message,omitempty" yaml:"message,omitempty"`
	Data      map[string]any `json:"data,omitempty" yaml:"data,omitempty"`
}

type EscalationSpec struct {
	Title    string
	Severity Status
	TTL      time.Duration
	Topics   map[string]any
}

type EscalationPolicy struct {
	MaxUnacknowledged int
	TTL               time.Duration
}

type EscalationOption func(*Escalations)

func WithEscalationPolicy(policy EscalationPolicy) EscalationOption {
	return func(e *Escalations) {
		if policy.MaxUnacknowledged > 0 {
			e.policy.MaxUnacknowledged = policy.MaxUnacknowledged
		}
		if policy.TTL > 0 {
			e.policy.TTL = policy.TTL
		}
	}
}

func WithEscalationClock(now clock) EscalationOption {
	return func(e *Escalations) {
		if now != nil {
			e.now = now
		}
	}
}

type Escalations struct {
	mu        sync.Mutex
	now       clock
	policy    EscalationPolicy
	epoch     string
	nextID    uint64
	nextSeq   uint64
	incidents map[string]*localIncident
	order     []string
}

type localIncident struct {
	ID            string
	Title         string
	Status        string
	CreatedAt     time.Time
	ExpiresAt     time.Time
	LastUpdatedAt time.Time
	Severity      Status
	Topics        map[string]any
	Events        []EscalationEvent
	LastSeq       uint64
	AckedSeq      uint64
}

func NewEscalations(opts ...EscalationOption) *Escalations {
	e := &Escalations{
		now: defaultClock,
		policy: EscalationPolicy{
			MaxUnacknowledged: 10,
			TTL:               24 * time.Hour,
		},
		incidents: map[string]*localIncident{},
	}
	for _, opt := range opts {
		opt(e)
	}
	e.epoch = strconv.FormatInt(e.now().UnixNano(), 36)
	return e
}

func (e *Escalations) Start(_ context.Context, spec EscalationSpec) (*Escalation, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireLocked()
	if e.policy.MaxUnacknowledged > 0 && e.unacknowledgedLocked() >= e.policy.MaxUnacknowledged {
		return nil, false
	}
	now := e.now()
	ttl := spec.TTL
	if ttl <= 0 {
		ttl = e.policy.TTL
	}
	e.nextID++
	id := fmt.Sprintf("ID-%010d", e.nextID)
	inc := &localIncident{
		ID:            id,
		Title:         spec.Title,
		Status:        EscalationOpen,
		CreatedAt:     now,
		ExpiresAt:     now.Add(ttl),
		LastUpdatedAt: now,
		Severity:      spec.Severity,
		Topics:        cloneAnyMap(spec.Topics),
	}
	e.incidents[id] = inc
	e.order = append(e.order, id)
	e.appendEventLocked(inc, now, "incident", "created", nil)
	return &Escalation{id: id, owner: e}, true
}

func (e *Escalations) EscalationDisplay(after, ack string) EscalationDisplayDocument {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireLocked()
	ackSeq := e.parseCursor(ack)
	if ackSeq > 0 {
		e.ackLocked(ackSeq)
	}
	afterSeq := e.parseCursor(after)
	out := EscalationDisplayDocument{
		Kind:      EscalationDisplayKind,
		Watermark: e.formatCursor(e.nextSeq),
	}
	for _, id := range e.order {
		inc := e.incidents[id]
		if inc == nil || inc.LastSeq <= afterSeq {
			continue
		}
		if len(inc.Events) == 0 && inc.AckedSeq >= inc.LastSeq {
			continue
		}
		out.Incidents = append(out.Incidents, inc.snapshot(afterSeq))
	}
	return out
}

func (e *Escalations) appendEventLocked(inc *localIncident, now time.Time, topic, message string, data map[string]any) {
	e.nextSeq++
	seq := e.nextSeq
	inc.LastSeq = seq
	inc.LastUpdatedAt = now
	inc.Events = append(inc.Events, EscalationEvent{
		Seq:       e.formatCursor(seq),
		Timestamp: now,
		Topic:     topic,
		Message:   message,
		Data:      cloneAnyMap(data),
	})
}

func (e *Escalations) unacknowledgedLocked() int {
	count := 0
	for _, inc := range e.incidents {
		if inc.LastSeq > inc.AckedSeq {
			count++
		}
	}
	return count
}

func (e *Escalations) ackLocked(seq uint64) {
	for _, inc := range e.incidents {
		if seq > inc.AckedSeq {
			inc.AckedSeq = min(seq, inc.LastSeq)
		}
		kept := inc.Events[:0]
		for _, event := range inc.Events {
			if e.parseCursor(event.Seq) > seq {
				kept = append(kept, event)
			}
		}
		inc.Events = kept
		if inc.Status == EscalationClosed && inc.AckedSeq >= inc.LastSeq {
			inc.Status = EscalationAcknowledged
		}
	}
}

func (e *Escalations) expireLocked() {
	now := e.now()
	kept := e.order[:0]
	for _, id := range e.order {
		inc := e.incidents[id]
		if inc == nil || (!inc.ExpiresAt.IsZero() && now.After(inc.ExpiresAt)) {
			delete(e.incidents, id)
			continue
		}
		kept = append(kept, id)
	}
	e.order = kept
}

type Escalation struct {
	id    string
	owner *Escalations
}

func (e *Escalation) ID() string { return e.id }

func (e *Escalation) AddLog(_ context.Context, ts time.Time, topic, msg string, fields map[string]any) {
	e.add(ts, topic, msg, fields)
}

func (e *Escalation) AddData(_ context.Context, topic string, data map[string]any) {
	e.add(time.Time{}, topic, "", data)
}

func (e *Escalation) Close(_ context.Context, reason string) {
	if e == nil || e.owner == nil {
		return
	}
	e.owner.mu.Lock()
	defer e.owner.mu.Unlock()
	inc := e.owner.incidents[e.id]
	if inc == nil {
		return
	}
	now := e.owner.now()
	inc.Status = EscalationClosed
	e.owner.appendEventLocked(inc, now, "incident", reason, nil)
}

func (e *Escalation) add(ts time.Time, topic, msg string, fields map[string]any) {
	if e == nil || e.owner == nil {
		return
	}
	e.owner.mu.Lock()
	defer e.owner.mu.Unlock()
	inc := e.owner.incidents[e.id]
	if inc == nil {
		return
	}
	if ts.IsZero() {
		ts = e.owner.now()
	}
	e.owner.appendEventLocked(inc, ts, topic, msg, fields)
}

func (i *localIncident) snapshot(afterSeq uint64) EscalationIncident {
	events := make([]EscalationEvent, 0, len(i.Events))
	for _, event := range i.Events {
		if parseCursor(event.Seq, "") > afterSeq {
			events = append(events, event)
		}
	}
	return EscalationIncident{
		ID:            i.ID,
		Title:         i.Title,
		Status:        i.Status,
		CreatedAt:     i.CreatedAt,
		ExpiresAt:     i.ExpiresAt,
		LastUpdatedAt: i.LastUpdatedAt,
		Severity:      i.Severity,
		Topics:        cloneAnyMap(i.Topics),
		Events:        events,
	}
}

func (r *Registry) RegisterEscalations(source EscalationSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.escalations = source
}

func (r *Registry) EscalationDisplay(after, ack string) EscalationDisplayDocument {
	r.mu.RLock()
	source := r.escalations
	version := r.version
	r.mu.RUnlock()
	labels := r.stateDisplayLabelPath()
	if source == nil {
		return EscalationDisplayDocument{
			Kind:      EscalationDisplayKind,
			Version:   version,
			LabelPath: labels,
		}
	}
	doc := source.EscalationDisplay(after, ack)
	doc.Kind = EscalationDisplayKind
	doc.Version = version
	doc.LabelPath = labels
	return doc
}

func (r *Registry) EscalationHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		q := req.URL.Query()
		doc := r.EscalationDisplay(q.Get("after"), q.Get("ack"))
		if err := yaml.NewEncoder(w).Encode(doc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (e *Escalations) parseCursor(s string) uint64 {
	return parseCursor(s, e.epoch)
}

func (e *Escalations) formatCursor(seq uint64) string {
	if seq == 0 {
		return ""
	}
	if e.epoch == "" {
		return formatSeq(seq)
	}
	return e.epoch + ":" + formatSeq(seq)
}

func parseCursor(s, epoch string) uint64 {
	if s == "" {
		return 0
	}
	if before, after, ok := strings.Cut(s, ":"); ok {
		if epoch != "" && before != epoch {
			return 0
		}
		s = after
	}
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func formatSeq(seq uint64) string {
	if seq == 0 {
		return ""
	}
	return strconv.FormatUint(seq, 10)
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
