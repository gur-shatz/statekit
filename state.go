// Package statekit provides small interfaces and reusable implementations for
// component-owned runtime state.
package statekit

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Status is the condition reported by a state object.
type Status int

const (
	Pass Status = iota
	Warn
	Fail
	Down
)

var statusNames = map[Status]string{
	Pass: "pass",
	Warn: "warn",
	Fail: "fail",
	Down: "down",
}

func (s Status) String() string {
	if name, ok := statusNames[s]; ok {
		return name
	}
	return fmt.Sprintf("Status(%d)", int(s))
}

func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *Status) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		status, err := parseStatus(str)
		if err != nil {
			return err
		}
		*s = status
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*s = Status(n)
	return nil
}

func (s Status) MarshalYAML() (any, error) {
	return s.String(), nil
}

func (s *Status) UnmarshalYAML(node *yaml.Node) error {
	var str string
	if err := node.Decode(&str); err == nil {
		status, err := parseStatus(str)
		if err != nil {
			return err
		}
		*s = status
		return nil
	}
	var n int
	if err := node.Decode(&n); err != nil {
		return err
	}
	*s = Status(n)
	return nil
}

func parseStatus(s string) (Status, error) {
	switch strings.ToLower(s) {
	case "pass":
		return Pass, nil
	case "warn":
		return Warn, nil
	case "fail":
		return Fail, nil
	case "down":
		return Down, nil
	default:
		return Pass, fmt.Errorf("unknown status %q", s)
	}
}

// Importance controls how a child state contributes to an aggregate.
type Importance int

const (
	Informational Importance = iota
	Important
)

func (i Importance) String() string {
	switch i {
	case Informational:
		return "informational"
	case Important:
		return "important"
	default:
		return fmt.Sprintf("Importance(%d)", int(i))
	}
}

func (i Importance) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

func (i *Importance) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		importance, err := parseImportance(s)
		if err != nil {
			return err
		}
		*i = importance
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*i = Importance(n)
	return nil
}

func (i Importance) MarshalYAML() (any, error) {
	return i.String(), nil
}

func (i *Importance) UnmarshalYAML(node *yaml.Node) error {
	var str string
	if err := node.Decode(&str); err == nil {
		importance, err := parseImportance(str)
		if err != nil {
			return err
		}
		*i = importance
		return nil
	}
	var n int
	if err := node.Decode(&n); err != nil {
		return err
	}
	*i = Importance(n)
	return nil
}

func parseImportance(s string) (Importance, error) {
	switch strings.ToLower(s) {
	case "informational":
		return Informational, nil
	case "important":
		return Important, nil
	default:
		return Important, fmt.Errorf("unknown importance %q", s)
	}
}

// HistoryEntry records a state transition.
type HistoryEntry struct {
	Timestamp time.Time      `json:"timestamp" yaml:"timestamp"`
	Status    Status         `json:"status" yaml:"status"`
	SecsAgo   int64          `json:"secs_ago" yaml:"secs_ago"`
	Reason    string         `json:"reason,omitempty" yaml:"reason"`
	Data      map[string]any `json:"data,omitempty" yaml:"data,omitempty"`
}

// Snapshot is an immutable point-in-time view of a State.
//
// Data carries state-specific fields such as probe metadata, metric values, or
// scraped labels. It stays nested under "data" in JSON and YAML.
type Snapshot struct {
	Name           string             `json:"name" yaml:"name"`
	ScrapedFrom    string             `json:"scraped_from,omitempty" yaml:"scraped_from,omitempty"` // this is scraped data
	ScrapePath     string             `json:"scrape_path,omitempty" yaml:"scrape_path,omitempty"`   // this is scraped data
	Status         Status             `json:"status" yaml:"status"`
	Importance     Importance         `json:"importance" yaml:"importance"`
	Help           string             `json:"help,omitempty" yaml:"help,omitempty"`
	Reason         string             `json:"reason,omitempty" yaml:"reason,omitempty"`
	UpdatedAt      time.Time          `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
	UpdatedSecsAgo int64              `json:"updated_secs_ago,omitempty" yaml:"updated_secs_ago,omitempty"`
	Data           map[string]any     `json:"data,omitempty" yaml:"data,omitempty"`
	ChangedAt      time.Time          `json:"changed_at" yaml:"changed_at"`
	ChangedSecsAgo int64              `json:"changed_secs_ago" yaml:"changed_secs_ago"`
	Metrics        collectorSnapshots `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	History        []HistoryEntry     `json:"history,omitempty" yaml:"history,omitempty"`
	Checks         []Snapshot         `json:"checks,omitempty" yaml:"checks,omitempty"`
}

// State is the central interface. Implementations own their concurrency,
// evaluation rules, and storage, and return safe snapshots.
type State interface {
	Name() string
	Snapshot() Snapshot
}

// StateCollection can be registered when a producer contributes multiple
// top-level states. Snapshot is still available as a summary/fallback, while
// Snapshots is used by Registry display and Prometheus output.
//
// Scrapers use this to add scraped states flatly without wrapping them in
// checks or mutating the remote check hierarchy.
type StateCollection interface {
	State
	Snapshots() []Snapshot
}

type clock func() time.Time

var defaultClock clock = time.Now
