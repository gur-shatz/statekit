PORT ?= 8080
GOCACHE ?= $(CURDIR)/.cache/go-build

.PHONY: help fmt test demo demo-url

help:
	@echo "statekit targets:"
	@echo "  make fmt       Format all Go code"
	@echo "  make test      Run all tests"
	@echo "  make demo      Run the component demo server"
	@echo "  make demo-url  Print the demo UI, state, and metrics URLs"
	@echo ""
	@echo "Variables:"
	@echo "  PORT=8080      Port used by make demo"
	@echo "  GOCACHE=.cache/go-build"

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

test:
	GOCACHE=$(GOCACHE) go test ./...

demo:
	GOCACHE=$(GOCACHE) PORT=$(PORT) go run ./examples/componentdemo

demo-url:
	@echo "UI:      http://localhost:$(PORT)/"
	@echo "State:   http://localhost:$(PORT)/state"
	@echo "Metrics: http://localhost:$(PORT)/metrics"
