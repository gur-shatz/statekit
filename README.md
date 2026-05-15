# statekit

`statekit` is a small Go library for component-owned runtime state.

The core idea is that application objects should own their own condition,
locking, history, and evaluation logic. A registry only enumerates those objects
and exposes snapshots through JSON or Prometheus-style text.

## Concepts

- `State`: small interface implemented by anything that can report a safe
  snapshot.
- `ManualState`: explicitly set state.
- `AggregateState`: state derived from child states, with children added
  progressively as subsystems initialize.
- `Importance`: whether a state is `important` or `informational` when it
  contributes to an aggregate.
- `PrometheusCollector`: optional interface for factual metrics beyond state.
- `Counter`, `Gauge`, `CounterVec`, `GaugeVec`: built-in Prometheus
  collectors for scalar and labeled values.
- `Registry`: collects states and optional collectors, applies const labels,
  and exposes raw state, display JSON/YAML, or Prometheus handlers.
- `FailRatio`: built-in state object for pass/fail outcome windows.

## Example

```go
reg := statekit.NewRegistry(statekit.WithLabel("component", "issuer"))

app := statekit.NewStateAggregator("issuer")
db := statekit.NewManualState("database")
cache := statekit.NewManualState("cache")

app.Add(db)
app.AddInformational(cache)
reg.Register(app)

db.Fail("connection refused", nil)

http.Handle("/state", reg.JSONHandler())
http.Handle("/state/display.json", reg.StateDisplayJSONHandler())
http.Handle("/state/display.yaml", reg.StateDisplayYAMLHandler())
http.Handle("/metrics", reg.PrometheusHandler())
```

## State Display

The display endpoints wrap the current state tree in a stable document shape
that includes the registry label hierarchy. A fleet-wide visualizer can merge
many component documents by `label_path`.

```json
{
  "kind": "statekit.state.v1",
  "label_path": [
    {"name": "component", "value": "issuer"}
  ],
  "states": []
}
```

## Prometheus Metrics

```go
requests := statekit.NewCounterVec(
	"http_requests_total",
	"Total HTTP requests.",
	"route",
	"status",
)
inflight := statekit.NewGauge("http_inflight_requests", "In-flight HTTP requests.")

reg.RegisterCollector(requests)
reg.RegisterCollector(inflight)

requests.WithLabelValues("/checkout", "200").Inc()
inflight.Set(3)
```

## Fail Ratio

```go
upstream := statekit.NewFailRatio(
	"upstream",
	time.Minute,
	statekit.RatioPolicy{MinSamples: 10, WarnAt: 0.25, FailAt: 0.5},
)

upstream.Pass()
upstream.Fail()
```

Use `statekit.AllFailed(minSamples, status)` when the policy should only fire if
every observed outcome in the window failed.

## Demo

Run the component demo to try manual states, aggregate state, counters, gauges,
failure-ratio state, JSON snapshots, and Prometheus scraping:

```sh
go run ./examples/componentdemo
```

The demo serves a web UI at `http://localhost:8080/`, raw JSON at `/state`,
display documents at `/state/display.json` and `/state/display.yaml`, and
Prometheus text at `/metrics`.

Set `PORT=8081` or another value if port 8080 is already in use.
