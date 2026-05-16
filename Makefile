PORT           ?= 19080
SCRAPER_PORT   ?= 19081
STACKDEMO_PORT ?= 19110
STACKDEMO_FLAGS ?= -kill-url

RUNCTL ?= go run github.com/gur-shatz/go-run/cmd/runctl@latest
RUNCTL_FLAGS ?= -ui

.PHONY: help fmt test runctl demo demo-component demo-scraper demo-fleet demo-stack demo-urls

help:
	@echo "statekit targets:"
	@echo "  make fmt             Format all Go code"
	@echo "  make test            Run all tests"
	@echo "  make demo-component  Run componentdemo under runctl (PORT=$(PORT))"
	@echo "  make demo-scraper    Run scrapedemo under runctl (SCRAPER_PORT=$(SCRAPER_PORT))"
	@echo "                       (expects demo-component running on PORT)"
	@echo "  make demo-fleet      Run the self-contained fleetdemo under runctl"
	@echo "  make demo-stack      Run stackdemo under runctl"
	@echo "                       (STACKDEMO_FLAGS=$(STACKDEMO_FLAGS))"
	@echo "  make demo            Alias for demo-fleet"
	@echo "  make demo-urls       Print URLs for the running demos"
	@echo ""
	@echo "Variables:"
	@echo "  PORT=$(PORT)              Port for componentdemo"
	@echo "  SCRAPER_PORT=$(SCRAPER_PORT)      Port for scrapedemo"
	@echo "  STACKDEMO_PORT=$(STACKDEMO_PORT)     Port for stackdemo"
	@echo "  STACKDEMO_FLAGS=$(STACKDEMO_FLAGS)  Extra flags for stackdemo"
	@echo "  RUNCTL=$(RUNCTL)"
	@echo "                       (override to use a locally-installed runctl binary)"
	@echo "  RUNCTL_FLAGS=$(RUNCTL_FLAGS)         Extra flags passed to runctl"

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

test:
	go test ./...

runctl: demo-fleet

demo: demo-fleet

demo-component:
	cd examples/componentdemo && PORT=$(PORT) $(RUNCTL) $(RUNCTL_FLAGS) -config runctl.yaml

demo-scraper:
	cd examples/scrapedemo && PORT=$(SCRAPER_PORT) SOURCE_PORT=$(PORT) $(RUNCTL) $(RUNCTL_FLAGS) -config runctl.yaml

demo-fleet:
	cd examples/fleetdemo && $(RUNCTL) $(RUNCTL_FLAGS) -config runctl.yaml

demo-stack:
	cd examples/stackdemo && PORT=$(STACKDEMO_PORT) FLAGS="$(STACKDEMO_FLAGS)" $(RUNCTL) $(RUNCTL_FLAGS) -config runctl.yaml

demo-urls:
	@echo "componentdemo:"
	@echo "  runctl UI:      http://localhost:19180/"
	@echo "  UI / liveness:  http://localhost:$(PORT)/"
	@echo "  State (YAML):   http://localhost:$(PORT)/state"
	@echo "  Metrics:        http://localhost:$(PORT)/metrics"
	@echo ""
	@echo "scrapedemo:"
	@echo "  runctl UI:      http://localhost:19181/"
	@echo "  State (YAML):   http://localhost:$(SCRAPER_PORT)/state"
	@echo "  Metrics:        http://localhost:$(SCRAPER_PORT)/metrics"
	@echo ""
	@echo "fleetdemo:"
	@echo "  runctl UI:      http://localhost:19184/"
	@echo "  issuer-east:    http://localhost:19082/state"
	@echo "  issuer-west:    http://localhost:19083/state"
	@echo "  Scraper:        http://localhost:19084/state"
	@echo "  Scraper metrics:http://localhost:19084/metrics"
	@echo ""
	@echo "stackdemo:"
	@echo "  runctl UI:      http://localhost:19210/"
	@echo "  UI:             http://localhost:$(STACKDEMO_PORT)/"
	@echo "  Fleet state:    http://localhost:$(STACKDEMO_PORT)/fleet/state"
	@echo "  Storage groups: http://localhost:$(STACKDEMO_PORT)/api/state/groups?by=group_name"
	@echo "  Kill URL:       http://localhost:$(STACKDEMO_PORT)/-/quit"
