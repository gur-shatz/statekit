package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

type EscalationIngestor interface {
	IngestEscalations(ctx context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error
}

func buildEscalations(target TargetConfig, cfg Config, client *http.Client, ingestor EscalationIngestor) *taskRunner {
	labels := targetLabels(cfg.Labels, target, target.Escalations.Labels)
	interval := resolveInterval(target.Escalations.Interval, target.Interval, cfg.Defaults.Interval)
	timeout := resolveTimeout(target.Escalations.Timeout, target.Timeout, cfg.Defaults.Timeout)
	source := targetIdentifier(target)
	base := resolveURL(target.BaseURL, target.Escalations.Path)
	name := source + ".escalations"
	var watermark string

	tick := func(ctx context.Context) {
		reqURL, err := escalationURL(base, watermark)
		if err != nil {
			return
		}
		doc, err := scrapeEscalations(ctx, client, reqURL, timeout)
		if err != nil {
			return
		}
		doc = annotateEscalationScrape(doc, source, labels)
		if err := ingestor.IngestEscalations(ctx, source, doc, time.Now()); err != nil {
			return
		}
		watermark = doc.Watermark
	}

	return &taskRunner{name: name, interval: interval, tick: tick}
}

func annotateEscalationScrape(doc statekit.EscalationDisplayDocument, source string, labels map[string]string) statekit.EscalationDisplayDocument {
	out := doc
	out.Incidents = make([]statekit.EscalationIncident, len(doc.Incidents))
	for i, incident := range doc.Incidents {
		if incident.ScrapedFrom == "" {
			incident.ScrapedFrom = source
		}
		if incident.ScrapePath == "" {
			incident.ScrapePath = source
		} else {
			incident.ScrapePath = source + " > " + incident.ScrapePath
		}
		incident.Labels = mergeEscalationLabels(labels, incident.Labels)
		out.Incidents[i] = incident
	}
	return out
}

func mergeEscalationLabels(base, incident map[string]string) map[string]string {
	if len(base) == 0 && len(incident) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(incident))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range incident {
		out[k] = v
	}
	return out
}

func escalationURL(base, watermark string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if watermark != "" {
		q.Set("after", watermark)
		q.Set("ack", watermark)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func scrapeEscalations(ctx context.Context, client *http.Client, url string, timeout time.Duration) (statekit.EscalationDisplayDocument, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return statekit.EscalationDisplayDocument{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return statekit.EscalationDisplayDocument{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return statekit.EscalationDisplayDocument{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return statekit.EscalationDisplayDocument{}, err
	}
	var doc statekit.EscalationDisplayDocument
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return statekit.EscalationDisplayDocument{}, fmt.Errorf("yaml decode: %w", err)
	}
	return doc, nil
}
