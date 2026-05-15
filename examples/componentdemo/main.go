package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gur-shatz/statekit"
)

//go:embed page.html
var pageFS embed.FS

type demoComponent struct {
	mu sync.RWMutex

	app      *statekit.AggregateState
	database *statekit.ManualState
	cache    *statekit.ManualState
	worker   *statekit.ManualState
	upstream *statekit.FailRatio

	requests *statekit.Counter
	errors   *statekit.Counter
	queue    *statekit.Gauge
	latency  *statekit.Gauge
}

func newDemoComponent() *demoComponent {
	c := &demoComponent{
		app:      statekit.NewStateAggregator("checkout-api"),
		database: statekit.NewManualState("database"),
		cache:    statekit.NewManualState("cache", statekit.WithImportance(statekit.Informational)),
		worker:   statekit.NewManualState("worker"),
		upstream: statekit.NewFailRatio("payments-upstream", 5*time.Minute, statekit.RatioPolicy{
			MinSamples: 3,
			WarnAt:     0.25,
			FailAt:     0.50,
			DownAt:     0.85,
		}),
		requests: statekit.NewCounter("demo_requests_total", "Total demo requests processed."),
		errors:   statekit.NewCounter("demo_errors_total", "Total demo request errors."),
		queue:    statekit.NewGauge("demo_queue_depth", "Current demo worker queue depth."),
		latency:  statekit.NewGauge("demo_latency_ms", "Current demo request latency in milliseconds."),
	}
	c.app.Add(c.database, c.cache, c.worker, c.upstream)
	c.queue.Set(4)
	c.latency.Set(42)
	c.requests.Add(120)
	c.errors.Add(3)
	c.upstream.Pass()
	c.upstream.Pass()
	c.upstream.Fail()
	return c
}

func (c *demoComponent) register(reg *statekit.Registry) {
	must(reg.Register(c.app))
	must(reg.RegisterCollectors(c.requests, c.errors, c.queue, c.latency))
}

func (c *demoComponent) view() demoView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return demoView{
		Snapshot: c.app.Snapshot(),
		Metrics: []metricView{
			{Name: "demo_requests_total", Value: fmt.Sprint(c.requests.Get())},
			{Name: "demo_errors_total", Value: fmt.Sprint(c.errors.Get())},
			{Name: "demo_queue_depth", Value: fmt.Sprint(c.queue.Get())},
			{Name: "demo_latency_ms", Value: fmt.Sprint(c.latency.Get())},
		},
		Ratio: c.upstream.RatioSnapshot(),
	}
}

func (c *demoComponent) handleSetState(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := r.FormValue("target")
	status, err := parseStatus(r.FormValue("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	message := strings.TrimSpace(r.FormValue("message"))
	if message == "" {
		message = status.String()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch target {
	case "database":
		c.database.Set(status, message, nil)
	case "cache":
		c.cache.Set(status, message, nil)
	case "worker":
		c.worker.Set(status, message, nil)
	default:
		http.Error(w, "unknown state target", http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (c *demoComponent) handleRecordOutcome(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch r.FormValue("outcome") {
	case "pass":
		c.upstream.Pass()
	case "fail":
		c.upstream.Fail()
		c.errors.Inc()
	default:
		http.Error(w, "unknown outcome", http.StatusBadRequest)
		return
	}
	c.requests.Inc()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (c *demoComponent) handleSetMetric(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	value, err := strconv.ParseInt(r.FormValue("value"), 10, 64)
	if err != nil {
		http.Error(w, "metric value must be an integer", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch r.FormValue("metric") {
	case "queue":
		c.queue.Set(value)
	case "latency":
		c.latency.Set(value)
	case "requests":
		if value < 0 {
			http.Error(w, "counter increment must be non-negative", http.StatusBadRequest)
			return
		}
		c.requests.Add(uint64(value))
	case "errors":
		if value < 0 {
			http.Error(w, "counter increment must be non-negative", http.StatusBadRequest)
			return
		}
		c.errors.Add(uint64(value))
	default:
		http.Error(w, "unknown metric", http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func main() {
	component := newDemoComponent()
	reg := statekit.NewRegistry(
		statekit.WithLabel("service", "checkout"),
		statekit.WithLabel("example", "componentdemo"),
	)
	component.register(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if err := page.ExecuteTemplate(w, "page.html", component.view()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("POST /state", component.handleSetState)
	mux.HandleFunc("POST /outcome", component.handleRecordOutcome)
	mux.HandleFunc("POST /metric", component.handleSetMetric)
	mux.Handle("/state", reg.JSONHandler())
	mux.Handle("/state/display.json", reg.StateDisplayJSONHandler())
	mux.Handle("/state/display.yaml", reg.StateDisplayYAMLHandler())
	mux.Handle("/metrics", reg.PrometheusHandler())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	log.Printf("statekit component demo listening on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func parseStatus(s string) (statekit.Status, error) {
	switch strings.ToLower(s) {
	case "pass":
		return statekit.Pass, nil
	case "warn":
		return statekit.Warn, nil
	case "fail":
		return statekit.Fail, nil
	case "down":
		return statekit.Down, nil
	default:
		return statekit.Pass, fmt.Errorf("unknown status %q", s)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type demoView struct {
	Snapshot statekit.Snapshot
	Metrics  []metricView
	Ratio    statekit.FailRatioSnapshot
}

type metricView struct {
	Name  string
	Value string
}

func toJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

var page = template.Must(template.New("page.html").Funcs(template.FuncMap{
	"json": toJSON,
}).ParseFS(pageFS, "page.html"))
