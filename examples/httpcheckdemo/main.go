package main

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/collectors"
	"github.com/gur-shatz/statekit/scraper"
)

//go:embed page.html
var pageFS embed.FS

type demo struct {
	port       string
	metrics    *collectors.HTTPMetrics
	httpState  *statekit.AggregateState
	sourceReg  *statekit.Registry
	scraperReg *statekit.Registry
	tracked    http.Handler
}

type metricView struct {
	Name  string
	Value string
}

type pageView struct {
	Source  statekit.Snapshot
	Scraped []statekit.Snapshot
	Metrics []metricView
}

func newDemo(port string) *demo {
	metrics := collectors.NewHTTPMetrics()
	httpState := statekit.NewStateAggregator("http")
	httpState.AddTest(
		collectors.NewHTTPErrorRatioCheck(metrics, "error ratio", 5, 0.20, 0.50, 0),
		collectors.NewHTTPAverageLatencyCheck(metrics, "average latency", 120*time.Millisecond, 220*time.Millisecond, 0),
	)

	sourceReg := statekit.NewRegistry(
		statekit.WithLabel("example", "httpcheckdemo"),
		statekit.WithLabel("role", "source"),
	)
	must(sourceReg.Register(httpState))
	must(sourceReg.RegisterCollectors(metrics))

	work := http.NewServeMux()
	work.HandleFunc("POST /work/ok", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(15 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})
	work.HandleFunc("POST /work/slow", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(260 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})
	work.HandleFunc("POST /work/fail", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		http.Error(w, "simulated upstream failure", http.StatusInternalServerError)
	})

	return &demo{
		port:      port,
		metrics:   metrics,
		httpState: httpState,
		sourceReg: sourceReg,
		tracked:   metrics.Middleware(work),
	}
}

func (d *demo) configureScraper(ctx context.Context) {
	cfg := scraper.Config{
		Labels: map[string]string{"example": "httpcheckdemo", "role": "scraper"},
		Defaults: scraper.Defaults{
			Interval:   scraper.Duration(time.Second),
			Timeout:    scraper.Duration(time.Second),
			Expiration: scraper.Duration(10 * time.Second),
		},
		Targets: []scraper.TargetConfig{{
			ID:      "httpcheck-source",
			Name:    "httpcheck-source",
			BaseURL: "http://localhost:" + d.port,
			Labels:  map[string]string{"service": "httpcheckdemo"},
			Liveness: []scraper.LivenessTask{{
				ID:           "up",
				Path:         "/",
				ExpectStatus: []int{200},
				MaxLatency:   scraper.Duration(500 * time.Millisecond),
			}},
			StateAggregation: &scraper.StateAggregationTask{Path: "/state"},
			Metrics:          &scraper.MetricsTask{Paths: []string{"/metrics"}},
		}},
	}

	sc, err := scraper.New(cfg)
	if err != nil {
		log.Fatalf("scraper: %v", err)
	}
	scraperReg := statekit.NewRegistry(statekit.WithLabel("example", "httpcheckdemo-scraper"))
	for _, st := range sc.States() {
		must(scraperReg.Register(st))
	}
	must(scraperReg.RegisterCollectors(sc.MetricsCollector()))
	d.scraperReg = scraperReg
	go sc.Run(ctx)
}

func (d *demo) view() pageView {
	scraped := []statekit.Snapshot(nil)
	if d.scraperReg != nil {
		scraped = d.scraperReg.Snapshot()
	}
	return pageView{
		Source:  d.httpState.Snapshot(),
		Scraped: scraped,
		Metrics: []metricView{
			{Name: "requests", Value: fmt.Sprintf("%d", d.metrics.Requests())},
			{Name: "requests/sec", Value: fmt.Sprintf("%.3f", d.metrics.RequestsPerSecond())},
			{Name: "errors", Value: fmt.Sprintf("%d", d.metrics.Errors())},
			{Name: "errors/sec", Value: fmt.Sprintf("%.3f", d.metrics.ErrorsPerSecond())},
			{Name: "avg latency", Value: d.metrics.AverageLatency().String()},
		},
	}
}

func (d *demo) handleGenerate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kind := r.FormValue("kind")
	count, err := strconv.Atoi(r.FormValue("count"))
	if err != nil || count < 1 {
		count = 1
	}
	if count > 50 {
		count = 50
	}
	path := "/work/ok"
	switch kind {
	case "ok":
		path = "/work/ok"
	case "slow":
		path = "/work/slow"
	case "fail":
		path = "/work/fail"
	default:
		http.Error(w, "unknown traffic kind", http.StatusBadRequest)
		return
	}
	for i := 0; i < count; i++ {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		d.tracked.ServeHTTP(httptest.NewRecorder(), req)
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (d *demo) mount(mux *http.ServeMux) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if err := page.ExecuteTemplate(w, "page.html", d.view()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("POST /generate", d.handleGenerate)
	mux.Handle("/work/", d.tracked)
	d.sourceReg.Mount(mux, "/")
	d.scraperReg.Mount(mux, "/scraper")
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "19086"
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	d := newDemo(port)
	d.configureScraper(ctx)

	mux := http.NewServeMux()
	d.mount(mux)

	server := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	log.Printf("statekit HTTP check demo listening on http://localhost:%s", port)
	log.Printf("scraped state available at http://localhost:%s/scraper/state", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var page = template.Must(template.New("page.html").Funcs(template.FuncMap{
	"printf": fmt.Sprintf,
}).ParseFS(pageFS, "page.html"))
