// Command scrapedemo runs a statekit scraper that polls another
// statekit component and re-serves the aggregated state and metrics on
// its own HTTP port.
//
// Usage:
//
//	# Terminal 1: run a statekit component (the scrape source).
//	go run ./examples/componentdemo
//
//	# Terminal 2: run the scraper, which polls localhost:8080.
//	go run ./examples/scrapedemo
//
//	# Look at the aggregated output:
//	curl http://localhost:8081/state/display.yaml
//	curl http://localhost:8081/metrics
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/scraper"
)

func main() {
	cfgPath := flag.String("config", "examples/scrapedemo/scraper.yaml", "path to scraper YAML config")
	addr := flag.String("addr", ":19081", "address to listen on")
	flag.Parse()

	if _, err := os.Stat(*cfgPath); err != nil {
		// fall back to "scraper.yaml" next to the executable for `go run` from other CWDs.
		if exe, eerr := os.Executable(); eerr == nil {
			alt := filepath.Join(filepath.Dir(exe), "scraper.yaml")
			if _, serr := os.Stat(alt); serr == nil {
				*cfgPath = alt
			}
		}
	}

	cfg, err := scraper.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config %q: %v", *cfgPath, err)
	}

	sc, err := scraper.New(*cfg)
	if err != nil {
		log.Fatalf("new scraper: %v", err)
	}

	reg := statekit.NewRegistry(statekit.WithLabel("role", "scraper"))
	for _, st := range sc.States() {
		if err := reg.Register(st); err != nil {
			log.Fatalf("register state %q: %v", st.Name(), err)
		}
	}
	if err := reg.RegisterCollectors(sc.MetricsCollector()); err != nil {
		log.Fatalf("register metrics: %v", err)
	}

	mux := http.NewServeMux()
	reg.Mount(mux, "/")

	server := &http.Server{Addr: *addr, Handler: mux}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		log.Printf("scraper HTTP listening on http://localhost%s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	log.Printf("scraping %d target(s) from %s", len(cfg.Targets), *cfgPath)
	go sc.Run(ctx)

	<-ctx.Done()
	log.Println("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = server.Shutdown(shutCtx)
}
