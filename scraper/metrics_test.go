package scraper

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMetricsScrapeAddsScrapedFromLabel(t *testing.T) {
	target := TargetConfig{
		ID:        "regional-east",
		Name:      "regional",
		GroupName: "payments",
		BaseURL:   "http://regional.example",
		Metrics: &MetricsTask{
			Paths:  []string{"/metrics"},
			Labels: map[string]string{"subsystem": "issuer"},
		},
	}
	cfg := Config{Labels: map[string]string{"region": "use1"}}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(strings.NewReader(`
plain_metric 1
upstream_metric{scraped_from="origin-service",kind="remote"} 2
`)),
		}, nil
	})}
	collector := newScrapedMetricsCollector()
	runner := buildMetrics(target, cfg, client, collector)

	runner.tick(context.Background())

	samples := collector.CollectPrometheus()
	if len(samples) != 2 {
		t.Fatalf("samples len = %d, want 2: %+v", len(samples), samples)
	}
	byName := map[string]map[string]string{}
	for _, sample := range samples {
		byName[sample.Name] = sample.Labels
	}
	plain := byName["plain_metric"]
	if plain["scraped_from"] != "regional-east" {
		t.Fatalf("plain scraped_from = %q, labels = %+v", plain["scraped_from"], plain)
	}
	if plain["target_id"] != "regional-east" || plain["group_name"] != "payments" || plain["region"] != "use1" || plain["subsystem"] != "issuer" {
		t.Fatalf("plain labels = %+v", plain)
	}
	upstream := byName["upstream_metric"]
	if upstream["scraped_from"] != "origin-service" {
		t.Fatalf("upstream scraped_from = %q, labels = %+v", upstream["scraped_from"], upstream)
	}
	if upstream["target_id"] != "regional-east" || upstream["kind"] != "remote" {
		t.Fatalf("upstream labels = %+v", upstream)
	}
}

func TestMetricsScrapeStructuralScrapedFromOverridesConfigLabel(t *testing.T) {
	target := TargetConfig{
		ID:      "issuer-prod-use1",
		Name:    "issuer",
		BaseURL: "http://issuer.example",
		Metrics: &MetricsTask{
			Paths:  []string{"/metrics"},
			Labels: map[string]string{"scraped_from": "task-label"},
		},
	}
	cfg := Config{Labels: map[string]string{"scraped_from": "global-label"}}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("plain_metric 1\n")),
		}, nil
	})}
	collector := newScrapedMetricsCollector()
	runner := buildMetrics(target, cfg, client, collector)

	runner.tick(context.Background())

	samples := collector.CollectPrometheus()
	if len(samples) != 1 {
		t.Fatalf("samples len = %d, want 1: %+v", len(samples), samples)
	}
	if got := samples[0].Labels["scraped_from"]; got != "issuer-prod-use1" {
		t.Fatalf("scraped_from = %q, want target identifier; labels = %+v", got, samples[0].Labels)
	}
}
