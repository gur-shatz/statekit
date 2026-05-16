package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

// remoteStateMirror is the statekit.StateCollection exposed for one target's
// state_aggregation task. Its Snapshots are the remote top-level states,
// annotated with scraped_from/scrape_path and rolled up flatly into the
// scraper registry.
//
// Snapshot returns a local summary/fallback for direct callers only. Registry
// display and Prometheus use Snapshots, so aggregation does not create a
// visible wrapper state and does not turn remote states into checks.
type remoteStateMirror struct {
	source     string // target identifier; scrape annotation for children and pre-scrape Name
	expiration time.Duration

	mu        sync.RWMutex
	scraped   []statekit.Snapshot
	hasData   bool
	fetchedAt time.Time
}

func newRemoteStateMirror(source string, expiration time.Duration) *remoteStateMirror {
	return &remoteStateMirror{source: source, expiration: expiration}
}

func (this *remoteStateMirror) Name() string {
	return this.source + ".state"
}

func (this *remoteStateMirror) Snapshot() statekit.Snapshot {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if !this.hasData {
		return statekit.Snapshot{
			Name:       this.Name(),
			Status:     statekit.Down,
			Importance: statekit.Important,
			Reason:     "no successful scrape yet",
			ChangedAt:  time.Now(),
		}
	}

	snap := this.snapshotLocked()
	if this.expiration > 0 && time.Since(this.fetchedAt) > this.expiration {
		snap.Status = statekit.Down
		snap.Reason = "stale (no scrape within expiration)"
		return snap
	}

	return snap
}

func (this *remoteStateMirror) Snapshots() []statekit.Snapshot {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if !this.hasData {
		return nil
	}
	if this.expiration > 0 && time.Since(this.fetchedAt) > this.expiration {
		return nil
	}
	return append([]statekit.Snapshot(nil), this.scraped...)
}

func (this *remoteStateMirror) setSuccess(doc statekit.StateDisplayDocument) {
	if len(doc.States) == 0 {
		return
	}
	annotated := make([]statekit.Snapshot, 0, len(doc.States))
	for _, state := range doc.States {
		annotated = append(annotated, annotateScrape(state, this.source, doc.LabelPath))
	}
	this.mu.Lock()
	this.scraped = annotated
	this.hasData = true
	this.fetchedAt = time.Now()
	this.mu.Unlock()
}

func (this *remoteStateMirror) setFailure(_ error) {
	// Keep last-known scraped state intact. fetchedAt is NOT updated,
	// so expiration counts from the last success. Scrape failure
	// surfaces via the target's liveness state.
}

func (this *remoteStateMirror) snapshotLocked() statekit.Snapshot {
	now := time.Now()
	status := statekit.Pass
	reason := ""
	for _, child := range this.scraped {
		if child.Status > status {
			status = child.Status
			reason = child.Name
			if child.Reason != "" {
				reason += ": " + child.Reason
			}
		}
	}
	return statekit.Snapshot{
		Name:           this.Name(),
		Status:         status,
		Importance:     statekit.Important,
		Reason:         reason,
		ChangedAt:      this.fetchedAt,
		ChangedSecsAgo: int64(now.Sub(this.fetchedAt).Seconds()),
	}
}

// annotateScrape stamps origin and chain on a scraped top-level state.
//
//   - ScrapedFrom: the origin / first producer. Set on the first hop
//     (when the field is empty), preserved on every subsequent hop.
//   - ScrapePath:  the chain "nearest > ... > origin". Each hop
//     prepends the source it scraped from. Rightmost is always the
//     origin (matches ScrapedFrom by construction).
//
// Children are not recursively annotated: a scraped top owns its
// subtree, so the top's fields are enough to indicate origin/chain for
// the whole branch.
func annotateScrape(s statekit.Snapshot, source string, labelPath []statekit.StateDisplayLabel) statekit.Snapshot {
	if s.ScrapedFrom == "" {
		s.ScrapedFrom = source
	}
	if s.ScrapePath == "" {
		s.ScrapePath = source
	} else {
		s.ScrapePath = source + " > " + s.ScrapePath
	}
	if len(labelPath) > 0 {
		labels := map[string]any{}
		for _, label := range labelPath {
			labels[label.Name] = label.Value
		}
		if s.Data == nil {
			s.Data = map[string]any{}
		}
		if existing, ok := s.Data["labels"]; ok {
			for k, v := range labelsFromAny(existing) {
				labels[k] = v
			}
		}
		s.Data["labels"] = labels
	}
	return s
}

func labelsFromAny(value any) map[string]any {
	out := map[string]any{}
	switch labels := value.(type) {
	case map[string]string:
		for k, v := range labels {
			out[k] = v
		}
	case map[string]any:
		for k, v := range labels {
			out[k] = v
		}
	}
	return out
}

func buildAggregation(target TargetConfig, cfg Config, client *http.Client) (*taskRunner, *remoteStateMirror) {
	expiration := firstNonZero(target.Expiration, cfg.Defaults.Expiration)
	interval := resolveInterval(target.StateAggregation.Interval, target.Interval, cfg.Defaults.Interval)
	timeout := resolveTimeout(target.StateAggregation.Timeout, target.Timeout, cfg.Defaults.Timeout)

	identifier := targetIdentifier(target)
	mirror := newRemoteStateMirror(identifier, expiration)
	url := resolveURL(target.BaseURL, target.StateAggregation.Path)

	tick := func(ctx context.Context) {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			mirror.setFailure(err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			mirror.setFailure(err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			mirror.setFailure(fmt.Errorf("status %d", resp.StatusCode))
			return
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			mirror.setFailure(err)
			return
		}
		doc, err := decodeStateDoc(resp.Header.Get("Content-Type"), body)
		if err != nil {
			mirror.setFailure(err)
			return
		}
		mirror.setSuccess(doc)
	}

	return &taskRunner{name: identifier + ".state", interval: interval, tick: tick}, mirror
}

func decodeStateDoc(_ string, body []byte) (statekit.StateDisplayDocument, error) {
	var doc statekit.StateDisplayDocument
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return doc, fmt.Errorf("yaml decode: %w", err)
	}
	return doc, nil
}
