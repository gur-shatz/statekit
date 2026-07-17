// Package console serves the statekit Fleet State Console: a standalone,
// single-page observability dashboard.
//
// The console is a pure client of the storage JSON API. It talks to that API
// over HTTP at runtime (via the injected API base) and has no compile-time
// dependency on the storage package, so the dashboard and the store can evolve
// independently. Mount it wherever the storage API is reachable from the
// browser:
//
//	mux.Handle("/console/", http.StripPrefix("/console",
//		console.Handler(console.Options{APIBase: "/api"})))
package console

import (
	"embed"
	"html/template"
	"net/http"
	"strings"
)

//go:embed index.html app.css app.js vendor/uplot/uPlot.iife.min.js vendor/uplot/uPlot.min.css vendor/uplot/LICENSE
var assets embed.FS

var indexTemplate = template.Must(template.ParseFS(assets, "index.html"))

// Options configures the console handler. The zero value is usable and yields
// a console titled "statekit" that reads from the "/api" base.
type Options struct {
	// Title appears in the browser tab and the header wordmark tooltip.
	Title string
	// APIBase is the base path of the storage JSON API the console reads from,
	// for example "/api". A trailing slash is optional.
	APIBase string
}

type indexData struct {
	Title   string
	APIBase string
}

// Handler returns an http.Handler serving the console single-page app and its
// static assets (app.css, app.js). It is safe for concurrent use.
func Handler(opts Options) http.Handler {
	if strings.TrimSpace(opts.Title) == "" {
		opts.Title = "statekit"
	}
	base := strings.TrimRight(strings.TrimSpace(opts.APIBase), "/")
	if base == "" {
		base = "/api"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := indexTemplate.Execute(w, indexData{Title: opts.Title, APIBase: base}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("GET /app.css", asset("app.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /app.js", asset("app.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /vendor/uplot/uPlot.min.css", asset("vendor/uplot/uPlot.min.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /vendor/uplot/uPlot.iife.min.js", asset("vendor/uplot/uPlot.iife.min.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /vendor/uplot/LICENSE", asset("vendor/uplot/LICENSE", "text/plain; charset=utf-8"))
	return mux
}

func asset(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, err := assets.ReadFile(name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(data)
	}
}
