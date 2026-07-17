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
	if plain["scrape_path"] != "regional-east" {
		t.Fatalf("plain scrape_path = %q, labels = %+v", plain["scrape_path"], plain)
	}
	if plain["target_id"] != "regional-east" || plain["group_name"] != "payments" || plain["region"] != "use1" || plain["subsystem"] != "issuer" {
		t.Fatalf("plain labels = %+v", plain)
	}
	upstream := byName["upstream_metric"]
	if upstream["scraped_from"] != "origin-service" {
		t.Fatalf("upstream scraped_from = %q, labels = %+v", upstream["scraped_from"], upstream)
	}
	if upstream["scrape_path"] != "regional-east > origin-service" {
		t.Fatalf("upstream scrape_path = %q, labels = %+v", upstream["scrape_path"], upstream)
	}
	if upstream["target_id"] != "regional-east" || upstream["kind"] != "remote" {
		t.Fatalf("upstream labels = %+v", upstream)
	}
}

func TestMetricsScrapePrependsScrapePath(t *testing.T) {
	target := TargetConfig{
		ID:      "fleet-east",
		Name:    "fleet",
		BaseURL: "http://fleet.example",
		Metrics: &MetricsTask{Paths: []string{"/metrics"}},
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(strings.NewReader(
				`requests_total{scraped_from="checkout-east",scrape_path="regional-east > checkout-east"} 2`,
			)),
		}, nil
	})}
	collector := newScrapedMetricsCollector()
	runner := buildMetrics(target, Config{}, client, collector)

	runner.tick(context.Background())

	samples := collector.CollectPrometheus()
	if len(samples) != 1 {
		t.Fatalf("samples len = %d, want 1: %+v", len(samples), samples)
	}
	labels := samples[0].Labels
	if labels["scraped_from"] != "checkout-east" {
		t.Fatalf("scraped_from = %q, labels = %+v", labels["scraped_from"], labels)
	}
	if labels["scrape_path"] != "fleet-east > regional-east > checkout-east" {
		t.Fatalf("scrape_path = %q, labels = %+v", labels["scrape_path"], labels)
	}
}

func TestMetricsScrapeCanDropScrapePath(t *testing.T) {
	target := TargetConfig{
		ID:      "external-export",
		Name:    "external",
		BaseURL: "http://external.example",
		Metrics: &MetricsTask{
			Paths:          []string{"/metrics"},
			DropScrapePath: true,
		},
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(strings.NewReader(
				`requests_total{scraped_from="checkout-east",scrape_path="regional-east > checkout-east"} 2`,
			)),
		}, nil
	})}
	collector := newScrapedMetricsCollector()
	runner := buildMetrics(target, Config{}, client, collector)

	runner.tick(context.Background())

	samples := collector.CollectPrometheus()
	if len(samples) != 1 {
		t.Fatalf("samples len = %d, want 1: %+v", len(samples), samples)
	}
	if _, ok := samples[0].Labels["scrape_path"]; ok {
		t.Fatalf("scrape_path was not dropped: %+v", samples[0].Labels)
	}
	if samples[0].Labels["scraped_from"] != "checkout-east" {
		t.Fatalf("scraped_from = %q, labels = %+v", samples[0].Labels["scraped_from"], samples[0].Labels)
	}
}

func TestMetricsScrapePreservesConfiguredScrapedFromLabel(t *testing.T) {
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
	if got := samples[0].Labels["scraped_from"]; got != "task-label" {
		t.Fatalf("scraped_from = %q, want configured label; labels = %+v", got, samples[0].Labels)
	}
}
