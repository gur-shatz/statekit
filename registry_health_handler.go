package statekit

import "net/http"

func (r *Registry) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(publicHealthStatus(r.Snapshot()[0].Status)))
	})
}

func publicHealthStatus(status Status) string {
	if status >= Fail {
		return Fail.String()
	}
	return status.String()
}
