package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func TestHandlerRendersIndexWithInjectedAPIBase(t *testing.T) {
	handler := Handler(Options{Title: "issuer", APIBase: "/svc/api/"})

	response := get(t, handler, "/")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := response.Body.String()
	// html/template escapes "/" as "\/" inside <script>; the JS value still
	// evaluates to "/svc/api" and the trailing slash is trimmed.
	if !strings.Contains(body, `window.STATEKIT_API_BASE = "\/svc\/api";`) {
		t.Fatalf("api base not injected (trailing slash should be trimmed): %q", body)
	}
	if !strings.Contains(body, "issuer") {
		t.Fatal("title not rendered into index")
	}
	if ct := response.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestHandlerDefaults(t *testing.T) {
	handler := Handler(Options{})

	body := get(t, handler, "/").Body.String()
	if !strings.Contains(body, `window.STATEKIT_API_BASE = "\/api";`) {
		t.Fatal("default api base should be /api")
	}
	if !strings.Contains(body, "statekit") {
		t.Fatal("default title should be statekit")
	}
}

func TestHandlerServesAssets(t *testing.T) {
	handler := Handler(Options{})

	for _, tc := range []struct {
		path        string
		contentType string
		needle      string
	}{
		{"/app.css", "text/css", ".fleet"},
		{"/app.js", "text/javascript", "STATEKIT_API_BASE"},
	} {
		response := get(t, handler, tc.path)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d", tc.path, response.Code)
		}
		if ct := response.Header().Get("Content-Type"); !strings.HasPrefix(ct, tc.contentType) {
			t.Fatalf("%s content-type = %q", tc.path, ct)
		}
		if !strings.Contains(response.Body.String(), tc.needle) {
			t.Fatalf("%s body missing %q", tc.path, tc.needle)
		}
	}
}
