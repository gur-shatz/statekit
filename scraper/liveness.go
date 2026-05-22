package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

// livenessState mirrors a target's reachability as a statekit.State.
// Successful probes -> pass; unreachable probes -> down; expectation
// failures -> fail. A FailurePolicy can require N consecutive results
// before transitioning.
type livenessState struct {
	name        string
	labels      map[string]string
	expiration  time.Duration
	policy      FailurePolicy
	scrapedFrom string // set by Scraper.New; surfaces as Snapshot.scraped_from

	mu         sync.Mutex
	inner      *statekit.ManualState
	updatedAt  time.Time
	cur        statekit.Status // last applied status
	failCount  int             // consecutive failures
	passCount  int             // consecutive successes
	latency    time.Duration
	httpStatus int
}

func newLivenessState(name string, importance statekit.Importance, labels map[string]string, expiration time.Duration, policy FailurePolicy) *livenessState {
	return &livenessState{
		name:       name,
		labels:     labels,
		expiration: expiration,
		policy:     policy,
		inner:      statekit.NewManualState(name, statekit.WithImportance(importance)),
		updatedAt:  time.Now(),
		cur:        statekit.Pass,
	}
}

func (this *livenessState) Name() string { return this.name }

func (this *livenessState) Snapshot() statekit.Snapshot {
	this.mu.Lock()
	if this.expiration > 0 && time.Since(this.updatedAt) > this.expiration && this.cur != statekit.Down {
		this.cur = statekit.Down
		this.inner.Down("stale (no scrape within expiration)", this.dataLocked())
	}
	snap := this.inner.Snapshot()
	if data := this.dataLocked(); len(data) > 0 {
		snap.Data = data
	}
	snap.UpdatedAt = this.updatedAt
	snap.UpdatedSecsAgo = int64(time.Since(this.updatedAt).Seconds())
	this.mu.Unlock()
	if this.scrapedFrom != "" {
		// First producer wins for ScrapedFrom; chain prepends for ScrapePath.
		// Matches annotateScrape's contract so multi-hop liveness chains are
		// honoured the same way as state_aggregation chains.
		if snap.ScrapedFrom == "" {
			snap.ScrapedFrom = this.scrapedFrom
		}
		if snap.ScrapePath == "" {
			snap.ScrapePath = this.scrapedFrom
		} else {
			snap.ScrapePath = this.scrapedFrom + " > " + snap.ScrapePath
		}
	}
	return snap
}

func (this *livenessState) recordSuccess(msg string, latency time.Duration, status int) {
	this.mu.Lock()
	defer this.mu.Unlock()
	now := time.Now()
	this.updatedAt = now
	this.latency = latency
	this.httpStatus = status
	this.passCount++
	this.failCount = 0
	threshold := this.policy.RecoverAfter
	if threshold <= 0 {
		threshold = 1
	}
	if this.cur == statekit.Pass {
		this.inner.Pass(msg, this.dataLocked())
		return
	}
	if this.passCount >= threshold {
		this.cur = statekit.Pass
		this.inner.Pass(msg, this.dataLocked())
		return
	}
	this.inner.Warn("recovering", this.dataLocked())
}

func (this *livenessState) recordFailure(status statekit.Status, err error, latency time.Duration, httpStatus int) {
	this.mu.Lock()
	defer this.mu.Unlock()
	now := time.Now()
	this.updatedAt = now
	this.latency = latency
	this.httpStatus = httpStatus
	this.failCount++
	this.passCount = 0
	threshold := this.policy.FailAfter
	if threshold <= 0 {
		threshold = 1
	}
	if this.cur == status {
		this.inner.Set(status, err.Error(), this.dataLocked())
		return
	}
	if this.failCount >= threshold {
		this.cur = status
		this.inner.Set(status, err.Error(), this.dataLocked())
		return
	}
	this.inner.Warn(err.Error(), this.dataLocked())
}

func (this *livenessState) dataLocked() map[string]any {
	data := map[string]any{}
	if this.failCount > 0 {
		data["consecutive_failures"] = this.failCount
	}
	if this.passCount > 0 {
		data["consecutive_successes"] = this.passCount
	}
	if len(this.labels) > 0 {
		data["labels"] = this.labels
	}
	if ms := this.latency.Milliseconds(); ms > 0 {
		data["latency_ms"] = ms
	}
	if this.httpStatus != 0 {
		data["http_status"] = this.httpStatus
	}
	if len(data) == 0 {
		return nil
	}
	return data
}

func buildLiveness(target TargetConfig, check LivenessTask, idx int, cfg Config, client *http.Client) (*taskRunner, *livenessState) {
	check = applyHTTPLivenessDefaults(check, cfg.Defaults.HTTPLiveness)
	importance := parseImportance(check.Importance)
	labels := targetLabels(cfg.Labels, target, check.Labels)
	expiration := firstNonZero(target.Expiration, cfg.Defaults.Expiration)
	interval := resolveInterval(check.Interval, target.Interval, cfg.Defaults.Interval)
	timeout := resolveTimeout(check.Timeout, target.Timeout, cfg.Defaults.Timeout)
	maxLatency := check.MaxLatency.Std()

	method := strings.ToUpper(check.Method)
	if method == "" {
		method = http.MethodGet
	}

	var bodyRegex *regexp.Regexp
	if check.ExpectBodyRegex != "" {
		bodyRegex = regexp.MustCompile(check.ExpectBodyRegex)
	}
	expectStatus := append([]int(nil), check.ExpectStatus...)

	name := fmt.Sprintf("%s.%s", targetIdentifier(target), checkID(check, idx))
	state := newLivenessState(name, importance, labels, expiration, check.FailurePolicy)
	url := resolveURL(target.BaseURL, check.Path)

	tick := func(ctx context.Context) {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, method, url, nil)
		if err != nil {
			state.recordFailure(statekit.Down, err, 0, 0)
			return
		}
		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			state.recordFailure(statekit.Down, err, elapsed, 0)
			return
		}
		defer resp.Body.Close()
		needsBody := bodyRegex != nil || len(check.ExpectJSON) > 0 || check.ExpectJSONPath != "" || check.ExpectContents != ""
		var body []byte
		if needsBody {
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				state.recordFailure(statekit.Down, err, elapsed, resp.StatusCode)
				return
			}
		}
		if !statusOK(resp.StatusCode, expectStatus) {
			state.recordFailure(statekit.Fail, fmt.Errorf("status %d", resp.StatusCode), elapsed, resp.StatusCode)
			return
		}
		if maxLatency > 0 && elapsed > maxLatency {
			state.recordFailure(statekit.Fail, fmt.Errorf("latency %v exceeds %v", elapsed.Truncate(time.Millisecond), maxLatency), elapsed, resp.StatusCode)
			return
		}
		if bodyRegex != nil {
			if !bodyRegex.Match(body) {
				state.recordFailure(statekit.Fail, fmt.Errorf("body did not match %q", check.ExpectBodyRegex), elapsed, resp.StatusCode)
				return
			}
		}
		if check.ExpectJSONPath != "" {
			value, ok, err := resolveJSONPath(body, check.ExpectJSONPath)
			if err != nil {
				state.recordFailure(statekit.Fail, err, elapsed, resp.StatusCode)
				return
			}
			if !ok {
				state.recordFailure(statekit.Fail, fmt.Errorf("json path %q missing", check.ExpectJSONPath), elapsed, resp.StatusCode)
				return
			}
			if jsonPathEmpty(value) {
				state.recordFailure(statekit.Fail, fmt.Errorf("json path %q was empty", check.ExpectJSONPath), elapsed, resp.StatusCode)
				return
			}
		}
		for _, exp := range check.ExpectJSON {
			if err := evaluateJSONExpectation(body, exp); err != nil {
				state.recordFailure(statekit.Fail, err, elapsed, resp.StatusCode)
				return
			}
		}
		if check.ExpectContents != "" && !strings.Contains(string(body), check.ExpectContents) {
			state.recordFailure(statekit.Fail, fmt.Errorf("body did not contain %q", check.ExpectContents), elapsed, resp.StatusCode)
			return
		}
		state.recordSuccess(fmt.Sprintf("ok (%v)", elapsed.Truncate(time.Millisecond)), elapsed, resp.StatusCode)
	}

	return &taskRunner{name: name, interval: interval, tick: tick}, state
}

func applyHTTPLivenessDefaults(check LivenessTask, defaults HTTPLivenessDefaults) LivenessTask {
	if len(check.ExpectStatus) == 0 {
		check.ExpectStatus = append([]int(nil), defaults.ExpectStatus...)
	}
	if check.MaxLatency == 0 {
		check.MaxLatency = defaults.MaxLatency
	}
	if check.FailurePolicy.FailAfter == 0 {
		check.FailurePolicy.FailAfter = defaults.FailurePolicy.FailAfter
	}
	if check.FailurePolicy.RecoverAfter == 0 {
		check.FailurePolicy.RecoverAfter = defaults.FailurePolicy.RecoverAfter
	}
	return check
}

func statusOK(code int, allow []int) bool {
	if len(allow) == 0 {
		return code >= 200 && code < 300
	}
	return slices.Contains(allow, code)
}

func resolveJSONPath(body []byte, path string) (any, bool, error) {
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, false, fmt.Errorf("json decode: %w", err)
	}
	path = strings.TrimSpace(path)
	if path == "$" {
		return root, true, nil
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, false, fmt.Errorf("unsupported json path %q", path)
	}
	current := root
	for _, token := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		name, indexes, err := parsePathToken(token)
		if err != nil {
			return nil, false, err
		}
		if name != "" {
			object, ok := current.(map[string]any)
			if !ok {
				return nil, false, nil
			}
			current, ok = object[name]
			if !ok {
				return nil, false, nil
			}
		}
		for _, index := range indexes {
			array, ok := current.([]any)
			if !ok || index < 0 || index >= len(array) {
				return nil, false, nil
			}
			current = array[index]
		}
	}
	return current, true, nil
}

func evaluateJSONExpectation(body []byte, exp JSONExpectation) error {
	value, ok, err := resolveJSONPath(body, exp.Path)
	if err != nil {
		return err
	}
	predicate := exp.Predicate
	if predicate == "" {
		predicate = "exists"
	}
	switch predicate {
	case "exists":
		if !ok {
			return fmt.Errorf("json path %q missing", exp.Path)
		}
		return nil
	case "equals", "==":
		if !ok {
			return fmt.Errorf("json path %q missing", exp.Path)
		}
		if !reflect.DeepEqual(value, exp.Value) {
			return fmt.Errorf("json path %q = %v, want %v", exp.Path, value, exp.Value)
		}
		return nil
	default:
		return fmt.Errorf("unsupported json predicate %q", exp.Predicate)
	}
}

func jsonPathEmpty(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	default:
		return false
	}
}

func parsePathToken(token string) (string, []int, error) {
	var indexes []int
	nameEnd := strings.Index(token, "[")
	if nameEnd < 0 {
		return token, nil, nil
	}
	name := token[:nameEnd]
	rest := token[nameEnd:]
	for rest != "" {
		if !strings.HasPrefix(rest, "[") {
			return "", nil, fmt.Errorf("unsupported json path token %q", token)
		}
		end := strings.Index(rest, "]")
		if end < 0 {
			return "", nil, fmt.Errorf("unsupported json path token %q", token)
		}
		index, err := strconv.Atoi(rest[1:end])
		if err != nil {
			return "", nil, fmt.Errorf("unsupported json path token %q", token)
		}
		indexes = append(indexes, index)
		rest = rest[end+1:]
	}
	return name, indexes, nil
}
