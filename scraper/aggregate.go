package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

// remoteStateMirror is the statekit.State exposed for one target's
// state_aggregation task. Its Snapshot is the remote's first top-level
// scraped state, annotated with _scraped_from (prepending the target
// identifier; existing _scraped_from chains accumulate).
//
// Before the first successful scrape (or after expiration), Snapshot
// returns a synthetic Down placeholder named after the target.
type remoteStateMirror struct {
	source     string // target identifier; scraped_from contribution and pre-scrape Name
	expiration time.Duration

	mu        sync.RWMutex
	scraped   statekit.Snapshot
	hasData   bool
	fetchedAt time.Time
}

func newRemoteStateMirror(source string, expiration time.Duration) *remoteStateMirror {
	return &remoteStateMirror{source: source, expiration: expiration}
}

func (this *remoteStateMirror) Name() string {
	this.mu.RLock()
	defer this.mu.RUnlock()
	if this.hasData {
		return this.scraped.Name
	}
	return this.source
}

func (this *remoteStateMirror) Snapshot() statekit.Snapshot {
	this.mu.RLock()
	defer this.mu.RUnlock()

	if !this.hasData {
		return statekit.Snapshot{
			ScrapedFrom: this.source,
			Name:        this.source,
			Status:      statekit.Down,
			Importance:  statekit.Important,
			Message:     "no successful scrape yet",
			ChangedAt:   time.Now(),
		}
	}

	if this.expiration > 0 && time.Since(this.fetchedAt) > this.expiration {
		snap := this.scraped
		snap.Status = statekit.Down
		snap.Message = "stale (no scrape within expiration)"
		return snap
	}

	return this.scraped
}

func (this *remoteStateMirror) setSuccess(doc statekit.StateDisplayDocument) {
	if len(doc.States) == 0 {
		return
	}
	annotated := annotateScrapedFrom(doc.States[0], this.source)
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

// annotateScrapedFrom prepends source to the snapshot's ScrapedFrom.
// Children are not recursively annotated: a scraped top owns its
// subtree, so the top's _scraped_from is enough to indicate origin for
// the whole branch.
func annotateScrapedFrom(s statekit.Snapshot, source string) statekit.Snapshot {
	if s.ScrapedFrom == "" {
		s.ScrapedFrom = source
	} else {
		s.ScrapedFrom = source + " > " + s.ScrapedFrom
	}
	return s
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

func decodeStateDoc(contentType string, body []byte) (statekit.StateDisplayDocument, error) {
	var doc statekit.StateDisplayDocument
	if strings.Contains(strings.ToLower(contentType), "yaml") {
		if err := yaml.Unmarshal(body, &doc); err != nil {
			return doc, fmt.Errorf("yaml decode: %w", err)
		}
		return doc, nil
	}
	if jerr := json.Unmarshal(body, &doc); jerr == nil {
		return doc, nil
	} else if yerr := yaml.Unmarshal(body, &doc); yerr == nil {
		return doc, nil
	} else {
		return doc, fmt.Errorf("decode failed: json: %v; yaml: %v", jerr, yerr)
	}
}
