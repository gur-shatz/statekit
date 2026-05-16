PORT         ?= 19080
SCRAPER_PORT ?= 19081
GOCACHE      ?= $(CURDIR)/.cache/go-build

.PHONY: help fmt test demo demo-component demo-scraper demo-fleet demo-urls

help:
	@echo "statekit targets:"
	@echo "  make fmt             Format all Go code"
	@echo "  make test            Run all tests"
	@echo "  make demo-component  Run the component demo server (PORT=$(PORT))"
	@echo "  make demo-scraper    Run the scraper demo (SCRAPER_PORT=$(SCRAPER_PORT))"
	@echo "                       (expects demo-component running on PORT)"
	@echo "  make demo-fleet      Run the self-contained fleet demo"
	@echo "                       (two components and a scraper in one process)"
	@echo "  make demo            Alias for demo-fleet"
	@echo "  make demo-urls       Print URLs for the running demos"
	@echo ""
	@echo "Variables:"
	@echo "  PORT=$(PORT)              Port for componentdemo"
	@echo "  SCRAPER_PORT=$(SCRAPER_PORT)      Port for scrapedemo"
	@echo "  GOCACHE=$(GOCACHE)"

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

test:
	GOCACHE=$(GOCACHE) go test ./...

demo-component:
	GOCACHE=$(GOCACHE) PORT=$(PORT) go run ./examples/componentdemo

demo-scraper:
	GOCACHE=$(GOCACHE) go run ./examples/scrapedemo -addr=:$(SCRAPER_PORT)

demo-fleet:
	GOCACHE=$(GOCACHE) go run ./examples/fleetdemo

demo: demo-fleet

demo-urls:
	@echo "componentdemo:"
	@echo "  UI / liveness:  http://localhost:$(PORT)/"
	@echo "  State (JSON):   http://localhost:$(PORT)/state"
	@echo "  Display (YAML): http://localhost:$(PORT)/state/display.yaml"
	@echo "  Metrics:        http://localhost:$(PORT)/metrics"
	@echo ""
	@echo "scrapedemo:"
	@echo "  State (JSON):   http://localhost:$(SCRAPER_PORT)/state"
	@echo "  Display (YAML): http://localhost:$(SCRAPER_PORT)/state/display.yaml"
	@echo "  Metrics:        http://localhost:$(SCRAPER_PORT)/metrics"
	@echo ""
	@echo "fleetdemo:"
	@echo "  issuer-east:    http://localhost:19082/state/display.yaml"
	@echo "  issuer-west:    http://localhost:19083/state/display.yaml"
	@echo "  Scraper:        http://localhost:19084/state/display.yaml"
	@echo "  Scraper metrics:http://localhost:19084/metrics"
