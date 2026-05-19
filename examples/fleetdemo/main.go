// Command fleetdemo runs two statekit components and a scraper in a
// single process. The scraper polls both components and exposes the
// aggregated fleet state and metrics on its own port.
//
// Ports (all chosen to avoid common development conflicts):
//
//	component "issuer-east" : :19082
//	component "issuer-west" : :19083
//	scraper                 : :19084
//
// Try:
//
//	go run ./examples/fleetdemo
//	curl -s http://localhost:19084/state
//	curl -s http://localhost:19084/metrics
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/scraper"
)

const (
	portEast    = 19082
	portWest    = 19083
	portScraper = 19084
)

type fakeComponent struct {
	name string
	port int
	reg  *statekit.Registry
}

func newFakeComponent(name, region string, port int, degradeCache bool) *fakeComponent {
	reg := statekit.NewRegistry(
		statekit.WithLabel("service", name),
		statekit.WithLabel("region", region),
	)
	app := statekit.NewStateAggregator(name+"-internal-state",
		statekit.WithHelp("Internal state of the "+name+" component, as reported by its own statekit registry."))
	db := statekit.NewManualState("database",
		statekit.WithHelp("Connection to the primary Postgres database."))
	cache := statekit.NewManualState("cache",
		statekit.WithImportance(statekit.Informational),
		statekit.WithHelp("Local Redis cache. Failures degrade latency but do not break correctness."))
	queue := statekit.NewManualState("queue",
		statekit.WithHelp("Background job queue processor."))
	app.AddCheck(db, queue)
	app.AddInformationalCheck(cache)
	_ = reg.Register(app)

	db.Pass("connected", nil)
	queue.Pass("idle", nil)
	if degradeCache {
		cache.Warn("high miss rate", nil)
	} else {
		cache.Pass("warm", nil)
	}

	metricPrefix := strings.ReplaceAll(name, "-", "_")
	requests := statekit.NewCounter(metricPrefix+"_requests_total", "Total requests served.")
	queueDepth := statekit.NewGauge(metricPrefix+"_queue_depth", "Queue depth.")
	requests.Add(uint64(100 + port%100))
	queueDepth.Set(int64(port % 10))
	_ = reg.RegisterCollectors(requests, queueDepth)

	return &fakeComponent{name: name, port: port, reg: reg}
}

func (this *fakeComponent) serve() *http.Server {
	mux := http.NewServeMux()
	this.reg.Mount(mux, "/")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, "statekit component %q on :%d\n", this.name, this.port)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", this.port), Handler: mux}
	go func() {
		log.Printf("component %q listening on :%d", this.name, this.port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("%s http: %v", this.name, err)
		}
	}()
	return srv
}

func waitUp(url string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %s", url)
}

func main() {
	east := newFakeComponent("issuer-east", "us-east-1", portEast, false)
	west := newFakeComponent("issuer-west", "us-west-2", portWest, true)
	eastSrv := east.serve()
	westSrv := west.serve()

	waitUp(fmt.Sprintf("http://localhost:%d/state", portEast))
	waitUp(fmt.Sprintf("http://localhost:%d/state", portWest))

	cfg := scraper.Config{
		Labels: map[string]string{"fleet": "demo"},
		Defaults: scraper.Defaults{
			Interval:   scraper.Duration(2 * time.Second),
			Timeout:    scraper.Duration(time.Second),
			Expiration: scraper.Duration(30 * time.Second),
		},
		Targets: []scraper.TargetConfig{
			{
				ID:        "issuer-east",
				Name:      "issuer-east",
				GroupName: "issuers",
				BaseURL:   fmt.Sprintf("http://localhost:%d", portEast),
				Labels:    map[string]string{"region": "us-east-1"},
				Liveness: []scraper.LivenessTask{{
					ID:           "up",
					Path:         "/",
					Importance:   "important",
					ExpectStatus: []int{200},
					MaxLatency:   scraper.Duration(time.Second),
					Labels:       map[string]string{"probe": "http"},
				}},
				StateAggregation: &scraper.StateAggregationTask{
					Path:   "/state",
					Labels: map[string]string{"subsystem": "issuer"},
				},
				Metrics: &scraper.MetricsTask{
					Paths:  []string{"/metrics"},
					Labels: map[string]string{"subsystem": "issuer"},
				},
			},
			{
				ID:        "issuer-west",
				Name:      "issuer-west",
				GroupName: "issuers",
				BaseURL:   fmt.Sprintf("http://localhost:%d", portWest),
				Labels:    map[string]string{"region": "us-west-2"},
				Liveness: []scraper.LivenessTask{{
					ID:           "up",
					Path:         "/",
					Importance:   "important",
					ExpectStatus: []int{200},
					MaxLatency:   scraper.Duration(time.Second),
					Labels:       map[string]string{"probe": "http"},
				}},
				StateAggregation: &scraper.StateAggregationTask{
					Path:   "/state",
					Labels: map[string]string{"subsystem": "issuer"},
				},
				Metrics: &scraper.MetricsTask{
					Paths:  []string{"/metrics"},
					Labels: map[string]string{"subsystem": "issuer"},
				},
			},
		},
	}

	sc, err := scraper.New(cfg)
	if err != nil {
		log.Fatalf("scraper: %v", err)
	}

	scraperReg := statekit.NewRegistry(statekit.WithLabel("role", "scraper"))
	for _, st := range sc.States() {
		_ = scraperReg.Register(st)
	}
	_ = scraperReg.RegisterCollectors(sc.MetricsCollector())

	scraperMux := http.NewServeMux()
	scraperReg.Mount(scraperMux, "/")
	scraperSrv := &http.Server{Addr: fmt.Sprintf(":%d", portScraper), Handler: scraperMux}

	go func() {
		log.Printf("scraper listening on :%d", portScraper)
		if err := scraperSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("scraper http: %v", err)
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go sc.Run(ctx)

	<-ctx.Done()
	log.Println("shutting down")
	_ = eastSrv.Close()
	_ = westSrv.Close()
	_ = scraperSrv.Close()
}
