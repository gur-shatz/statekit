package storage

import (
	"embed"
	"html/template"
	"net/http"
	"strings"
)

//go:embed ui.html ui.css ui.js
var uiFS embed.FS

var uiTemplate = template.Must(template.ParseFS(uiFS, "ui.html"))

type UIOptions struct {
	APIBase string
}

func UIHandler(opts UIOptions) http.Handler {
	apiBase := strings.TrimRight(opts.APIBase, "/")
	if apiBase == "" {
		apiBase = "/api"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := uiTemplate.Execute(w, map[string]string{"APIBase": apiBase}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("GET /ui.css", func(w http.ResponseWriter, _ *http.Request) {
		serveUIFile(w, "ui.css", "text/css; charset=utf-8")
	})
	mux.HandleFunc("GET /ui.js", func(w http.ResponseWriter, _ *http.Request) {
		serveUIFile(w, "ui.js", "text/javascript; charset=utf-8")
	})
	return mux
}

func serveUIFile(w http.ResponseWriter, name, contentType string) {
	data, err := uiFS.ReadFile(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(data)
}
