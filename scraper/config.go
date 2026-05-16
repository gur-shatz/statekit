package scraper

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level scraper configuration.
type Config struct {
	Labels   map[string]string `yaml:"labels,omitempty"`
	Defaults Defaults          `yaml:"defaults,omitempty"`
	Targets  []TargetConfig    `yaml:"targets"`
}

// Defaults apply to every target and task unless overridden.
type Defaults struct {
	Interval     Duration             `yaml:"interval,omitempty"`
	Timeout      Duration             `yaml:"timeout,omitempty"`
	Expiration   Duration             `yaml:"expiration,omitempty"`
	HTTPLiveness HTTPLivenessDefaults `yaml:"http_liveness,omitempty"`
}

type HTTPLivenessDefaults struct {
	ExpectStatus  []int         `yaml:"expect_status,omitempty"`
	MaxLatency    Duration      `yaml:"max_latency,omitempty"`
	FailurePolicy FailurePolicy `yaml:"failure_policy,omitempty"`
}

// TargetConfig describes one scrape target and the tasks to run against it.
type TargetConfig struct {
	ID               string                `yaml:"id,omitempty"`
	Name             string                `yaml:"name"`
	GroupName        string                `yaml:"group_name,omitempty"`
	BaseURL          string                `yaml:"base_url"`
	Labels           map[string]string     `yaml:"labels,omitempty"`
	Interval         Duration              `yaml:"interval,omitempty"`
	Timeout          Duration              `yaml:"timeout,omitempty"`
	Expiration       Duration              `yaml:"expiration,omitempty"`
	Liveness         []LivenessTask        `yaml:"liveness,omitempty"`
	StateAggregation *StateAggregationTask `yaml:"state_aggregation,omitempty"`
	Metrics          *MetricsTask          `yaml:"metrics,omitempty"`
}

// LivenessTask probes a URL and reports status based on HTTP expectations.
// All fields beyond Path are optional; sensible defaults apply.
type LivenessTask struct {
	ID              string            `yaml:"id,omitempty"`
	Name            string            `yaml:"name,omitempty"`
	Type            string            `yaml:"type,omitempty"`
	Path            string            `yaml:"path"`
	Method          string            `yaml:"method,omitempty"`
	Importance      string            `yaml:"importance,omitempty"`
	Labels          map[string]string `yaml:"labels,omitempty"`
	Interval        Duration          `yaml:"interval,omitempty"`
	Timeout         Duration          `yaml:"timeout,omitempty"`
	MaxLatency      Duration          `yaml:"max_latency,omitempty"`
	ExpectStatus    []int             `yaml:"expect_status,omitempty"`
	ExpectBodyRegex string            `yaml:"expect_body_regex,omitempty"`
	ExpectJSON      JSONExpectations  `yaml:"expect_json,omitempty"`
	ExpectJSONPath  string            `yaml:"expect_json_path,omitempty"`
	ExpectContents  string            `yaml:"expect_contents,omitempty"`
	FailurePolicy   FailurePolicy     `yaml:"failure_policy,omitempty"`
}

// JSONExpectation is a JSON body assertion. The compact YAML form is:
//
//	expect_json: "$.status equals ok"
type JSONExpectation struct {
	Path      string `yaml:"path,omitempty"`
	Predicate string `yaml:"predicate,omitempty"`
	Value     any    `yaml:"value,omitempty"`
}

type JSONExpectations []JSONExpectation

func (e *JSONExpectations) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		parsed, err := parseJSONExpectation(node.Value)
		if err != nil {
			return err
		}
		*e = JSONExpectations{parsed}
		return nil
	case yaml.SequenceNode:
		out := make([]JSONExpectation, 0, len(node.Content))
		for _, child := range node.Content {
			var parsed JSONExpectation
			if child.Kind == yaml.ScalarNode {
				var err error
				parsed, err = parseJSONExpectation(child.Value)
				if err != nil {
					return err
				}
			} else if err := child.Decode(&parsed); err != nil {
				return err
			}
			parsed.Value = normalizeConfigValue(parsed.Value)
			if err := validateJSONExpectation(parsed); err != nil {
				return err
			}
			out = append(out, parsed)
		}
		*e = out
		return nil
	case yaml.MappingNode:
		var parsed JSONExpectation
		if err := node.Decode(&parsed); err != nil {
			return err
		}
		parsed.Value = normalizeConfigValue(parsed.Value)
		if err := validateJSONExpectation(parsed); err != nil {
			return err
		}
		*e = JSONExpectations{parsed}
		return nil
	default:
		return fmt.Errorf("expect_json must be a string, mapping, or list")
	}
}

func parseJSONExpectation(expr string) (JSONExpectation, error) {
	parts := strings.Fields(expr)
	if len(parts) == 1 {
		exp := JSONExpectation{Path: parts[0], Predicate: "exists"}
		return exp, validateJSONExpectation(exp)
	}
	if len(parts) < 3 {
		return JSONExpectation{}, fmt.Errorf("expect_json %q must be '<jsonpath> <predicate> <value>'", expr)
	}
	valueText := strings.Join(parts[2:], " ")
	var value any
	if err := yaml.Unmarshal([]byte(valueText), &value); err != nil {
		return JSONExpectation{}, fmt.Errorf("expect_json value %q: %w", valueText, err)
	}
	value = normalizeConfigValue(value)
	exp := JSONExpectation{Path: parts[0], Predicate: parts[1], Value: value}
	return exp, validateJSONExpectation(exp)
}

func validateJSONExpectation(exp JSONExpectation) error {
	if exp.Path == "" {
		return fmt.Errorf("expect_json path is empty")
	}
	if exp.Path != "$" && !strings.HasPrefix(exp.Path, "$.") {
		return fmt.Errorf("expect_json path %q must start with $ or $.", exp.Path)
	}
	switch exp.Predicate {
	case "", "exists", "equals", "==":
		return nil
	default:
		return fmt.Errorf("unsupported expect_json predicate %q", exp.Predicate)
	}
}

func normalizeConfigValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return value
	}
	return normalized
}

// FailurePolicy implements consecutive-result hysteresis on liveness
// transitions. A check needs FailAfter consecutive failures to go down,
// and RecoverAfter consecutive successes to come back up. Zero means 1.
type FailurePolicy struct {
	FailAfter    int `yaml:"fail_after,omitempty"`
	RecoverAfter int `yaml:"recover_after,omitempty"`
}

// StateAggregationTask fetches a target's state display document and
// mirrors its tree locally.
type StateAggregationTask struct {
	Path     string            `yaml:"path"`
	Labels   map[string]string `yaml:"labels,omitempty"`
	Interval Duration          `yaml:"interval,omitempty"`
	Timeout  Duration          `yaml:"timeout,omitempty"`
}

// MetricsTask scrapes Prometheus text from one or more paths and
// re-emits the samples under the scraper's registry.
type MetricsTask struct {
	Paths    []string          `yaml:"paths"`
	Labels   map[string]string `yaml:"labels,omitempty"`
	Interval Duration          `yaml:"interval,omitempty"`
	Timeout  Duration          `yaml:"timeout,omitempty"`
}

// Duration wraps time.Duration for YAML parsing of strings like "15s".
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// LoadConfig reads and parses a YAML config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// firstNonZero returns the first non-zero duration, or 0.
func firstNonZero(values ...Duration) time.Duration {
	for _, v := range values {
		if v != 0 {
			return v.Std()
		}
	}
	return 0
}
