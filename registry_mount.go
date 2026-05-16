package statekit

import (
	"net/http"
	"strings"
)

// Mount registers the registry's standard HTTP endpoints on mux under prefix.
//
// For prefix "/issuer-east", the mounted endpoints are:
//   - /issuer-east/state
//   - /issuer-east/metrics
//
// Use an empty prefix or "/" to mount at /state and /metrics.
func (r *Registry) Mount(mux *http.ServeMux, prefix string) {
	prefix = cleanMountPrefix(prefix)
	mux.Handle(prefix+"/state", r.StateDisplayYAMLHandler())
	mux.Handle(prefix+"/metrics", r.PrometheusHandler())
}

func cleanMountPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return ""
	}
	prefix = "/" + strings.Trim(prefix, "/")
	return prefix
}
