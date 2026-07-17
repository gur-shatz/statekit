// Package scraper pulls state and metrics from remote statekit
// components on an interval and exposes the aggregated result through
// the local statekit Registry.
package scraper

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

const (
	defaultInterval = 15 * time.Second
	defaultTimeout  = 5 * time.Second
)

// Scraper periodically polls a set of targets. Each target contributes
// one or more top-level states (per-check liveness + the remote top-level
// states from state_aggregation, if configured). The scraper does NOT wrap
// remote states in its own aggregate and does NOT rewrite their checks.
type Scraper struct {
	cfg    Config
	client *http.Client

	states          []statekit.State
	metrics         *scrapedMetricsCollector
	metricsIngestor MetricsIngestor
	incidents       EscalationIngestor
	tasks           []*taskRunner
}

type Option func(*Scraper)

func WithEscalationIngestor(ingestor EscalationIngestor) Option {
	return func(s *Scraper) {
		s.incidents = ingestor
	}
}

// MetricsIngestor receives bounded-timeseries observations keyed by
// scrape_path. storage.MemoryStore's MetricsStore implements this interface.
type MetricsIngestor interface {
	IngestMetrics(key string, descs []statekit.PrometheusDesc, samples []statekit.PrometheusSample, observedAt time.Time)
}

func WithMetricsIngestor(ingestor MetricsIngestor) Option {
	return func(s *Scraper) {
		s.metricsIngestor = ingestor
	}
}

// New builds a Scraper from a parsed Config.
func New(cfg Config, opts ...Option) (*Scraper, error) {
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	s := &Scraper{
		cfg:     cfg,
		client:  &http.Client{},
		metrics: newScrapedMetricsCollector(),
	}
	for _, opt := range opts {
		opt(s)
	}

	for _, target := range cfg.Targets {
		tid := targetIdentifier(target)
		for j, check := range target.Liveness {
			runner, state := buildLiveness(target, check, j, cfg, s.client)
			state.scrapedFrom = tid
			s.states = append(s.states, state)
			s.tasks = append(s.tasks, runner)
		}
		if target.StateAggregation != nil {
			runner, mirror := buildAggregation(target, cfg, s.client)
			s.states = append(s.states, mirror)
			s.tasks = append(s.tasks, runner)
		}
		if target.Metrics != nil {
			runner := buildMetrics(target, cfg, s.client, s.metrics, s.metricsIngestor)
			s.tasks = append(s.tasks, runner)
		}
		if target.Escalations != nil {
			if s.incidents == nil {
				return nil, fmt.Errorf("target %q has escalations configured without an escalation ingestor", target.Name)
			}
			runner := buildEscalations(target, cfg, s.client, s.incidents)
			s.tasks = append(s.tasks, runner)
		}
	}
	return s, nil
}

// States returns the top-level states produced by the scraper. Each
// should be registered individually with a statekit.Registry.
func (s *Scraper) States() []statekit.State {
	return append([]statekit.State(nil), s.states...)
}

// MetricsCollector returns the collector that exposes scraped metrics.
func (s *Scraper) MetricsCollector() statekit.PrometheusCollector { return s.metrics }

// Run starts all task loops and blocks until ctx is cancelled.
func (s *Scraper) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, task := range s.tasks {
		wg.Add(1)
		go func(t *taskRunner) {
			defer wg.Done()
			t.Run(ctx)
		}(task)
	}
	wg.Wait()
}

// taskRunner is a generic interval-driven scrape loop. Implementations
// supply tick; the runner handles ticker, cancellation, and the initial
// eager call.
type taskRunner struct {
	name     string
	interval time.Duration
	tick     func(ctx context.Context)
}

func (this *taskRunner) Run(ctx context.Context) {
	this.tick(ctx)
	t := time.NewTicker(this.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			this.tick(ctx)
		}
	}
}

func validate(cfg *Config) error {
	seen := map[string]struct{}{}
	for i := range cfg.Targets {
		t := &cfg.Targets[i]
		if t.Name == "" {
			return fmt.Errorf("target index %d has empty name", i)
		}
		key := targetKey(*t)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate target %q", key)
		}
		seen[key] = struct{}{}
		if t.BaseURL == "" {
			return fmt.Errorf("target %q has empty base_url", t.Name)
		}
		if t.Escalations != nil && t.Escalations.Path == "" {
			return fmt.Errorf("target %q escalations has empty path", t.Name)
		}
		checks := map[string]struct{}{}
		for j := range t.Liveness {
			check := &t.Liveness[j]
			if check.Path == "" {
				return fmt.Errorf("target %q liveness check index %d has empty path", t.Name, j)
			}
			if check.Type != "" && check.Type != "http" {
				return fmt.Errorf("target %q liveness check %q has unsupported type %q", t.Name, checkID(*check, j), check.Type)
			}
			if check.ExpectBodyRegex != "" {
				if _, err := regexp.Compile(check.ExpectBodyRegex); err != nil {
					return fmt.Errorf("target %q liveness check %q has invalid expect_body_regex: %w", t.Name, checkID(*check, j), err)
				}
			}
			for _, exp := range check.ExpectJSON {
				if err := validateJSONExpectation(exp); err != nil {
					return fmt.Errorf("target %q liveness check %q has invalid expect_json: %w", t.Name, checkID(*check, j), err)
				}
			}
			id := checkID(*check, j)
			if _, dup := checks[id]; dup {
				return fmt.Errorf("target %q has duplicate liveness check id %q", t.Name, id)
			}
			checks[id] = struct{}{}
		}
	}
	return nil
}

// targetKey returns the uniqueness key for a target. Prefers an
// explicit ID, falls back to the trimmed base_url.
func targetKey(t TargetConfig) string {
	if t.ID != "" {
		return t.ID
	}
	return strings.TrimRight(t.BaseURL, "/")
}

// targetIdentifier returns the user-friendly identifier used in state
// names. Prefers ID, falls back to Name.
func targetIdentifier(t TargetConfig) string {
	if t.ID != "" {
		return t.ID
	}
	return t.Name
}

// checkID returns a stable identifier for a liveness check. Prefers
// explicit ID/Name, falls back to a path-derived form, then to index.
func checkID(check LivenessTask, idx int) string {
	if check.ID != "" {
		return check.ID
	}
	if check.Name != "" {
		return check.Name
	}
	if check.Path != "" {
		trimmed := strings.Trim(check.Path, "/")
		if trimmed == "" {
			return "root"
		}
		return strings.ReplaceAll(trimmed, "/", "-")
	}
	return fmt.Sprintf("check-%d", idx)
}

func mergeLabels(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// targetLabels merges scraper, target, and task labels and adds the
// target's structural identifiers (group_name, target_id) as labels too.
func targetLabels(global map[string]string, target TargetConfig, task map[string]string) map[string]string {
	labels := mergeLabels(global, target.Labels, task)
	if target.GroupName != "" {
		labels["group_name"] = target.GroupName
	}
	if target.ID != "" {
		labels["target_id"] = target.ID
	}
	return labels
}

func resolveURL(baseURL, path string) string {
	base := strings.TrimRight(baseURL, "/")
	if path == "" {
		return base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}

func resolveInterval(values ...Duration) time.Duration {
	if d := firstNonZero(values...); d != 0 {
		return d
	}
	return defaultInterval
}

func resolveTimeout(values ...Duration) time.Duration {
	if d := firstNonZero(values...); d != 0 {
		return d
	}
	return defaultTimeout
}

func parseImportance(s string) statekit.Importance {
	switch strings.ToLower(s) {
	case "informational":
		return statekit.Informational
	case "important", "":
		return statekit.Important
	default:
		return statekit.Important
	}
}
