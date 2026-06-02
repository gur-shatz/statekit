package statekit

import (
	"net/http"
	"strings"
)

// Mount registers the registry's standard HTTP endpoints on mux under prefix.
//
// For prefix "/issuer-east", the mounted endpoints are:
//   - /issuer-east/state
//   - /issuer-east/health
//   - /issuer-east/metrics
//   - /issuer-east/escalations
//
// Use an empty prefix or "/" to mount at /state, /health, /metrics, and
// /escalations.
func (r *Registry) Mount(mux *http.ServeMux, prefix string) {
	prefix = cleanMountPrefix(prefix)
	mux.Handle(prefix+"/state", r.StateDisplayYAMLHandler())
	mux.Handle(prefix+"/health", r.HealthHandler())
	mux.Handle(prefix+"/metrics", r.PrometheusHandler())
	mux.Handle(prefix+"/escalations", r.EscalationHandler())
}

func cleanMountPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return ""
	}
	prefix = "/" + strings.Trim(prefix, "/")
	return prefix
}
