package scraper

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestConfigDecodesHTTPLivenessExpectations(t *testing.T) {
	const text = `
defaults:
  interval: 15s
  timeout: 5s
  expiration: 1m
  http_liveness:
    expect_status: [200, 204]
    max_latency: 750ms
    failure_policy:
      fail_after: 3
      recover_after: 2
targets:
  - id: issuer-prod-use1
    name: issuer
    group_name: payments
    base_url: http://issuer.example
    liveness:
      - id: health
        path: /health
        expect_body_regex: '"status"\s*:\s*"ok"'
        expect_json: "$.status equals ok"
        expect_json_path: $.status
        expect_contents: ok
`
	var cfg Config
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Defaults.HTTPLiveness.ExpectStatus; len(got) != 2 || got[0] != 200 || got[1] != 204 {
		t.Fatalf("default expect_status = %+v", got)
	}
	if got := cfg.Defaults.HTTPLiveness.MaxLatency.Std(); got != 750*time.Millisecond {
		t.Fatalf("default max_latency = %v", got)
	}
	if got := cfg.Defaults.HTTPLiveness.FailurePolicy; got.FailAfter != 3 || got.RecoverAfter != 2 {
		t.Fatalf("default failure_policy = %+v", got)
	}
	check := cfg.Targets[0].Liveness[0]
	if check.ExpectBodyRegex == "" || len(check.ExpectJSON) != 1 || check.ExpectJSONPath != "$.status" || check.ExpectContents != "ok" {
		t.Fatalf("check expectations = %+v", check)
	}
	if check.ExpectJSON[0].Path != "$.status" || check.ExpectJSON[0].Predicate != "equals" || check.ExpectJSON[0].Value != "ok" {
		t.Fatalf("expect_json = %+v", check.ExpectJSON)
	}
}

func TestValidateRejectsDuplicateTargetsAndInvalidRegex(t *testing.T) {
	cfg := Config{Targets: []TargetConfig{
		{Name: "issuer-a", ID: "issuer", BaseURL: "http://a.example"},
		{Name: "issuer-b", ID: "issuer", BaseURL: "http://b.example"},
	}}
	if err := validate(&cfg); err == nil || !strings.Contains(err.Error(), "duplicate target") {
		t.Fatalf("duplicate target validate err = %v", err)
	}

	cfg = Config{Targets: []TargetConfig{{
		Name:    "issuer",
		BaseURL: "http://issuer.example",
		Liveness: []LivenessTask{{
			ID:              "health",
			Path:            "/health",
			ExpectBodyRegex: "[",
		}},
	}}}
	if err := validate(&cfg); err == nil || !strings.Contains(err.Error(), "invalid expect_body_regex") {
		t.Fatalf("invalid regex validate err = %v", err)
	}

	cfg = Config{Targets: []TargetConfig{{
		Name:    "issuer",
		BaseURL: "http://issuer.example",
		Liveness: []LivenessTask{{
			ID:   "health",
			Path: "/health",
			ExpectJSON: JSONExpectations{{
				Path:      "$.status",
				Predicate: "aroundish",
			}},
		}},
	}}}
	if err := validate(&cfg); err == nil || !strings.Contains(err.Error(), "invalid expect_json") {
		t.Fatalf("invalid expect_json validate err = %v", err)
	}
}

func TestLivenessSeparatesDownFromExpectationFailure(t *testing.T) {
	target := TargetConfig{ID: "issuer", Name: "issuer", GroupName: "payments", BaseURL: "http://issuer.example"}
	cfg := Config{Labels: map[string]string{"region": "use1"}}

	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})}
	runner, state := buildLiveness(target, LivenessTask{ID: "up", Path: "/"}, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Down {
		t.Fatalf("transport failure status = %v, want down", snap.Status)
	}

	client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(500, "not ok"), nil
	})}
	runner, state = buildLiveness(target, LivenessTask{ID: "up", Path: "/", ExpectStatus: []int{200}}, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("expectation failure status = %v, want fail", snap.Status)
	}
}

func TestLivenessJSONPathAndContentsExpectations(t *testing.T) {
	target := TargetConfig{ID: "issuer", Name: "issuer", BaseURL: "http://issuer.example"}
	cfg := Config{}
	check := LivenessTask{
		ID:             "health",
		Path:           "/health",
		ExpectJSONPath: "$.status",
		ExpectContents: "ok",
	}

	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"status":"ok"}`), nil
	})}
	runner, state := buildLiveness(target, check, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Pass {
		t.Fatalf("json path success status = %v, want pass", snap.Status)
	}

	client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"state":"ok"}`), nil
	})}
	runner, state = buildLiveness(target, check, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("json path missing status = %v, want fail", snap.Status)
	}

	client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"status":""}`), nil
	})}
	runner, state = buildLiveness(target, LivenessTask{
		ID:             "health",
		Path:           "/health",
		ExpectJSONPath: "$.status",
	}, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("json path empty status = %v, want fail", snap.Status)
	}
}

func TestLivenessExpectJSONShorthand(t *testing.T) {
	target := TargetConfig{ID: "issuer", Name: "issuer", BaseURL: "http://issuer.example"}
	cfg := Config{}

	var expectations JSONExpectations
	if err := yaml.Unmarshal([]byte(`["$.status equals ok", "$.errors equals []"]`), &expectations); err != nil {
		t.Fatal(err)
	}
	check := LivenessTask{
		ID:         "health",
		Path:       "/health",
		ExpectJSON: expectations,
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"status":"ok","errors":[]}`), nil
	})}
	runner, state := buildLiveness(target, check, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Pass {
		t.Fatalf("expect_json shorthand success status = %v, want pass", snap.Status)
	}

	client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"status":"bad","errors":[]}`), nil
	})}
	runner, state = buildLiveness(target, check, 0, cfg, client)
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("expect_json shorthand mismatch status = %v, want fail", snap.Status)
	}
}

func TestLivenessBodyContainsExpectation(t *testing.T) {
	target := TargetConfig{ID: "issuer", Name: "issuer", BaseURL: "http://issuer.example"}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, "issuer status ok"), nil
	})}
	runner, state := buildLiveness(target, LivenessTask{
		ID:             "health",
		Path:           "/health",
		ExpectContents: "status ok",
	}, 0, Config{}, client)

	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Pass {
		t.Fatalf("body contains status = %v, want pass", snap.Status)
	}
}

func TestLivenessFailurePolicyHysteresis(t *testing.T) {
	target := TargetConfig{ID: "issuer", Name: "issuer", BaseURL: "http://issuer.example"}
	cfg := Config{}
	check := LivenessTask{
		ID:           "health",
		Path:         "/health",
		ExpectStatus: []int{200},
		FailurePolicy: FailurePolicy{
			FailAfter:    2,
			RecoverAfter: 2,
		},
	}

	statuses := []int{500, 500, 200, 200}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		status := statuses[0]
		statuses = statuses[1:]
		return response(status, "ok"), nil
	})}
	runner, state := buildLiveness(target, check, 0, cfg, client)

	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Warn {
		t.Fatalf("first failed probe status = %v, want warn", snap.Status)
	}
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Fail {
		t.Fatalf("second failed probe status = %v, want fail", snap.Status)
	}
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Warn {
		t.Fatalf("first recovering probe status = %v, want warn", snap.Status)
	}
	runner.tick(context.Background())
	if snap := state.Snapshot(); snap.Status != statekit.Pass {
		t.Fatalf("second recovering probe status = %v, want pass", snap.Status)
	}
}

func TestLivenessSnapshotDataIncludesProbeMetadata(t *testing.T) {
	target := TargetConfig{
		ID:        "issuer-prod-use1",
		Name:      "issuer",
		GroupName: "payments",
		BaseURL:   "http://issuer.example",
	}
	cfg := Config{Labels: map[string]string{"region": "use1"}}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(204, ""), nil
	})}
	runner, state := buildLiveness(target, LivenessTask{ID: "ready", Path: "/ready", ExpectStatus: []int{204}}, 0, cfg, client)
	runner.tick(context.Background())

	snap := state.Snapshot()
	data, ok := snap.Data.(map[string]any)
	if !ok {
		t.Fatalf("snapshot data type = %T", snap.Data)
	}
	if data["http_status"] != 204 {
		t.Fatalf("http_status = %+v", data["http_status"])
	}
	if _, ok := data["updated_at"]; !ok {
		t.Fatalf("updated_at missing from data: %+v", data)
	}
	if _, ok := data["updated_secs_ago"]; !ok {
		t.Fatalf("updated_secs_ago missing from data: %+v", data)
	}
	labels, ok := data["labels"].(map[string]string)
	if !ok {
		t.Fatalf("labels type = %T", data["labels"])
	}
	if labels["target_id"] != "issuer-prod-use1" || labels["group_name"] != "payments" || labels["region"] != "use1" {
		t.Fatalf("labels = %+v", labels)
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}
