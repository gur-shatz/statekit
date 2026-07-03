package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gur-shatz/statekit"
)

// TestOpenAPIContract keeps the embedded spec and the handlers from
// drifting: every served route must be documented, every documented route
// must be served, and every documented route is exercised against a seeded
// store with its JSON response validated against the spec's schema and its
// documented ETag header checked.
func TestOpenAPIContract(t *testing.T) {
	data, err := openAPIFS.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	loader := openapi3.NewLoader()
	spec, err := loader.LoadFromData(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		t.Fatalf("spec invalid: %v", err)
	}

	store := NewMemoryStore()
	api := NewAPI(store)

	served := map[string]bool{}
	for _, route := range api.routes() {
		served[route.Method+" "+specPath(route.Pattern)] = true
	}
	documented := map[string]bool{}
	for path, item := range spec.Paths.Map() {
		for method := range item.Operations() {
			documented[method+" "+path] = true
		}
	}
	for key := range served {
		if !documented[key] {
			t.Errorf("served route %s is not documented in openapi.yaml", key)
		}
	}
	for key := range documented {
		if !served[key] {
			t.Errorf("documented route %s is not served", key)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	now := time.Now()
	seedContractStore(t, store, now)
	server := httptest.NewServer(api.Handler())
	defer server.Close()

	targets, err := store.Targets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	substitutions := map[string]string{
		"{key}":      url.PathEscape(targets[0].Key),
		"{identity}": identityByName(t, store, "checkout-api"),
		"{source}":   "issuer-east",
		"{id}":       "ID-0000000001",
	}
	queries := map[string]string{
		"/state/timeline/bucket": "t=" + url.QueryEscape(now.Format(time.RFC3339Nano)),
		"/escalations/doc":       "source=contract",
		"/escalations/ack":       "source=issuer-east&id=ID-0000000001",
	}
	postBodies := map[string][2]string{
		"/state/doc":          {"text/yaml", "kind: statekit.state.v1\nlabel_path:\n  - name: service\n    value: contract\nstates:\n  - name: contract-state\n    status: pass\n    importance: important\n    changed_at: " + now.Format(time.RFC3339) + "\n    changed_secs_ago: 0\n"},
		"/escalations/doc":    {"text/yaml", "kind: statekit.escalation.v1\nincidents:\n  - id: contract-incident\n    title: contract\n    status: open\n    created_at: " + now.Format(time.RFC3339) + "\n    last_updated_at: " + now.Format(time.RFC3339) + "\n"},
		"/escalations/global": {"application/json", `{"type":"deployment","title":"contract deploy"}`},
	}

	for path, item := range spec.Paths.Map() {
		for method, op := range item.Operations() {
			requestURL := server.URL + expandPath(path, substitutions)
			if query := queries[path]; query != "" {
				requestURL += "?" + query
			}
			var body io.Reader
			contentType := ""
			if method == http.MethodPost && op.RequestBody != nil {
				spec, ok := postBodies[path]
				if !ok {
					t.Errorf("no contract-test body for POST %s", path)
					continue
				}
				contentType = spec[0]
				body = strings.NewReader(spec[1])
			}
			req, err := http.NewRequest(method, requestURL, body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", "application/json")
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}

			response := op.Responses.Status(resp.StatusCode)
			if response == nil || response.Value == nil {
				t.Errorf("%s %s returned undocumented status %d: %s", method, path, resp.StatusCode, payload)
				continue
			}
			if resp.StatusCode >= 300 {
				t.Errorf("%s %s status = %d: %s", method, path, resp.StatusCode, payload)
				continue
			}
			if _, documentsETag := response.Value.Headers["ETag"]; documentsETag && resp.Header.Get("ETag") == "" {
				t.Errorf("%s %s documents an ETag header but returned none", method, path)
			}
			validateJSONResponse(t, method, path, response.Value, resp, payload)
		}
	}
}

func validateJSONResponse(t *testing.T, method, path string, response *openapi3.Response, resp *http.Response, payload []byte) {
	t.Helper()
	if !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		return
	}
	media := response.Content.Get("application/json")
	if media == nil {
		t.Errorf("%s %s answered JSON but the spec documents none for status %d", method, path, resp.StatusCode)
		return
	}
	if media.Schema == nil || media.Schema.Value == nil {
		return
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Errorf("%s %s body is not valid JSON: %v", method, path, err)
		return
	}
	if err := media.Schema.Value.VisitJSON(decoded); err != nil {
		t.Errorf("%s %s body does not match its schema: %v", method, path, err)
	}
}

// specPath converts a ServeMux pattern to its OpenAPI form, e.g.
// /state/targets/{key...} to /state/targets/{key}.
func specPath(pattern string) string {
	return strings.ReplaceAll(pattern, "...}", "}")
}

func expandPath(path string, substitutions map[string]string) string {
	for placeholder, value := range substitutions {
		path = strings.ReplaceAll(path, placeholder, value)
	}
	return path
}

func seedContractStore(t *testing.T, store *MemoryStore, now time.Time) {
	t.Helper()
	ctx := context.Background()
	doc := testDocument()
	doc.States[0].ChangedAt = now.Add(-time.Minute)
	doc.States[0].Checks[0].ChangedAt = now.Add(-time.Minute)
	if err := store.IngestDocument(ctx, doc, now); err != nil {
		t.Fatal(err)
	}
	if err := store.IngestEscalations(ctx, "issuer-east", statekit.EscalationDisplayDocument{
		Kind: statekit.EscalationDisplayKind,
		Incidents: []statekit.EscalationIncident{{
			ID:            "ID-0000000001",
			Title:         "checkout failed",
			Status:        statekit.EscalationOpen,
			CreatedAt:     now,
			ExpiresAt:     now.Add(time.Hour),
			LastUpdatedAt: now,
			Events: []statekit.EscalationEvent{
				{Seq: "1", Timestamp: now, Topic: "incident", Message: "created"},
			},
		}},
	}, now); err != nil {
		t.Fatal(err)
	}
}
