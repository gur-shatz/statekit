// Package infopages exposes small mountable HTML pages that describe a
// statekit installation and the endpoints an application has mounted.
package infopages

import (
	"context"
	"embed"
	"html/template"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/storage"
)

//go:embed page.html ui.css
var pageFS embed.FS

var pageTemplate = template.Must(template.ParseFS(pageFS, "page.html"))

type Options struct {
	Title       string
	Registry    *statekit.Registry
	Storage     storage.Store
	RegistryURL string
	StorageURL  string
	APIURL      string
	GeneratedAt func() time.Time
}

type pageData struct {
	Title       string
	Active      string
	GeneratedAt time.Time
	RegistryURL string
	StorageURL  string
	APIURL      string
	Registry    *registryData
	Storage     *storageData
}

type registryData struct {
	Info       statekit.RegistryInfo
	LabelRows  []keyValue
	StateRows  []stateRow
	Endpoints  []endpoint
	MetricRows []metricRow
}

type storageData struct {
	Type             string
	StateCount       int
	TargetCount      int
	IncidentCount    int
	StatusCounts     map[string]int
	LastObservedAt   time.Time
	EndpointRows     []endpoint
	LastObservedText string
}

type keyValue struct {
	Key   string
	Value string
}

type stateRow struct {
	Name       string
	Status     string
	Reason     string
	Importance string
	UpdatedAt  time.Time
}

type metricRow struct {
	Name string
	Type string
	Help string
}

type endpoint struct {
	Path        string
	Description string
}

func Handler(opts Options) http.Handler {
	if opts.Title == "" {
		opts.Title = "statekit"
	}
	if opts.GeneratedAt == nil {
		opts.GeneratedAt = time.Now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		render(w, r, opts, "overview")
	})
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		render(w, r, opts, "config")
	})
	mux.HandleFunc("GET /storage", func(w http.ResponseWriter, r *http.Request) {
		render(w, r, opts, "storage")
	})
	mux.HandleFunc("GET /ui.css", func(w http.ResponseWriter, _ *http.Request) {
		data, err := pageFS.ReadFile("ui.css")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write(data)
	})
	return mux
}

func render(w http.ResponseWriter, r *http.Request, opts Options, active string) {
	data := pageData{
		Title:       opts.Title,
		Active:      active,
		GeneratedAt: opts.GeneratedAt(),
		RegistryURL: cleanBase(opts.RegistryURL),
		StorageURL:  cleanBase(opts.StorageURL),
		APIURL:      cleanBase(opts.APIURL),
	}
	if opts.Storage != nil && data.APIURL == "" {
		data.APIURL = "/api"
	}
	if opts.Registry != nil {
		data.Registry = registryPageData(opts.Registry, data.RegistryURL)
	}
	if opts.Storage != nil {
		storageData, err := storagePageData(r.Context(), opts.Storage, data.APIURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data.Storage = storageData
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pageTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func registryPageData(reg *statekit.Registry, base string) *registryData {
	info := reg.Info()
	display := reg.StateDisplay()
	out := &registryData{
		Info: info,
		Endpoints: []endpoint{
			{Path: joinBase(base, "/state"), Description: "State display document"},
			{Path: joinBase(base, "/health"), Description: "Plain health status"},
			{Path: joinBase(base, "/metrics"), Description: "Prometheus metrics"},
			{Path: joinBase(base, "/escalations"), Description: "Escalation display document"},
		},
	}
	for _, name := range info.LabelOrder {
		if value, ok := info.Labels[name]; ok {
			out.LabelRows = append(out.LabelRows, keyValue{Key: name, Value: value})
		}
	}
	seen := make(map[string]struct{}, len(out.LabelRows))
	for _, row := range out.LabelRows {
		seen[row.Key] = struct{}{}
	}
	for name, value := range info.Labels {
		if _, ok := seen[name]; ok {
			continue
		}
		out.LabelRows = append(out.LabelRows, keyValue{Key: name, Value: value})
	}
	for _, snap := range display.States {
		out.StateRows = append(out.StateRows, stateRow{
			Name:       snap.Name,
			Status:     snap.Status.String(),
			Reason:     snap.Reason,
			Importance: snap.Importance.String(),
			UpdatedAt:  snap.UpdatedAt,
		})
	}
	for _, desc := range info.PrometheusDescs {
		out.MetricRows = append(out.MetricRows, metricRow{
			Name: desc.Name,
			Type: string(desc.Type),
			Help: desc.Help,
		})
	}
	return out
}

func storagePageData(ctx context.Context, store storage.Store, apiBase string) (*storageData, error) {
	summary, err := store.Summary(ctx)
	if err != nil {
		return nil, err
	}
	incidents, err := store.Incidents(ctx, storage.IncidentFilter{})
	if err != nil {
		return nil, err
	}
	out := &storageData{
		Type:           reflect.TypeOf(store).String(),
		TargetCount:    summary.Targets.Total,
		IncidentCount:  len(incidents),
		StatusCounts:   summary.StatusCounts,
		LastObservedAt: summary.ObservedAt,
		EndpointRows: []endpoint{
			{Path: joinBase(apiBase, "/state/summary"), Description: "Fleet rollup"},
			{Path: joinBase(apiBase, "/state/targets"), Description: "Target summaries with state headers"},
			{Path: joinBase(apiBase, "/state/timeline"), Description: "Bucketed triggering-state counts"},
			{Path: joinBase(apiBase, "/escalations/incidents"), Description: "Stored escalation incidents"},
			{Path: joinBase(apiBase, "/openapi.yaml"), Description: "Storage API contract"},
		},
	}
	for _, count := range summary.StatusCounts {
		out.StateCount += count
	}
	if out.LastObservedAt.IsZero() {
		out.LastObservedText = "never"
	} else {
		out.LastObservedText = out.LastObservedAt.Format(time.RFC3339)
	}
	return out, nil
}

func cleanBase(base string) string {
	base = strings.TrimSpace(base)
	if base == "" || base == "/" {
		return ""
	}
	return "/" + strings.Trim(base, "/")
}

func joinBase(base, path string) string {
	base = cleanBase(base)
	path = "/" + strings.TrimLeft(path, "/")
	if base == "" {
		return path
	}
	return base + path
}
