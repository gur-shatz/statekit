package scraper

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gur-shatz/statekit"
	"gopkg.in/yaml.v3"
)

type recordingEscalationIngestor struct {
	docs []statekit.EscalationDisplayDocument
}

func (r *recordingEscalationIngestor) IngestEscalations(_ context.Context, _ string, doc statekit.EscalationDisplayDocument, _ time.Time) error {
	r.docs = append(r.docs, doc)
	return nil
}

func TestEscalationScrapeUsesSameEndpointForAfterAndAck(t *testing.T) {
	var urls []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		urls = append(urls, req.URL.String())
		doc := statekit.EscalationDisplayDocument{
			Kind:      statekit.EscalationDisplayKind,
			Watermark: "3",
			Incidents: []statekit.EscalationIncident{{
				ID:     "ID-0000000001",
				Title:  "support case",
				Labels: map[string]string{"incident_label": "kept"},
			}},
		}
		body, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(body))),
		}, nil
	})}
	ingestor := &recordingEscalationIngestor{}
	target := TargetConfig{
		ID:        "issuer-east",
		Name:      "issuer",
		GroupName: "payments",
		BaseURL:   "http://issuer.example",
		Labels:    map[string]string{"service": "issuer"},
		Escalations: &EscalationsTask{
			Path:   "/escalations",
			Labels: map[string]string{"subsystem": "support"},
		},
	}
	runner := buildEscalations(target, Config{Labels: map[string]string{"region": "use1"}}, client, ingestor)

	runner.tick(context.Background())
	runner.tick(context.Background())

	if len(ingestor.docs) != 2 {
		t.Fatalf("ingested docs = %d, want 2", len(ingestor.docs))
	}
	if urls[0] != "http://issuer.example/escalations" {
		t.Fatalf("first scrape url = %q", urls[0])
	}
	if urls[1] != "http://issuer.example/escalations?ack=3&after=3" {
		t.Fatalf("second scrape url = %q", urls[1])
	}
	incident := ingestor.docs[0].Incidents[0]
	if incident.ScrapedFrom != "issuer-east" || incident.ScrapePath != "issuer-east" {
		t.Fatalf("scrape provenance = %q %q", incident.ScrapedFrom, incident.ScrapePath)
	}
	if incident.Labels["region"] != "use1" || incident.Labels["service"] != "issuer" ||
		incident.Labels["group_name"] != "payments" || incident.Labels["target_id"] != "issuer-east" ||
		incident.Labels["subsystem"] != "support" || incident.Labels["incident_label"] != "kept" {
		t.Fatalf("labels = %+v", incident.Labels)
	}
}
