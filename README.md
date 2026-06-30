# statekit

`statekit` is a small Go library for component-owned runtime state.

The core idea is that application objects own their own condition, locking,
history, and evaluation logic. `statekit` provides two kinds of objects:

- **States** report the condition of a component (`pass` / `warn` / `fail` /
  `down`) along with a reason, history, and optional structured data.
- **Metrics** report numeric values from inside the component.

A `Registry` enumerates both, applies const labels, and serves HTTP
endpoints for state display, health status, and Prometheus scrapes.

## Example

```go
reg := statekit.NewRegistry(statekit.WithLabel("component", "issuer"))

app := statekit.NewStateAggregator("issuer")
db := statekit.NewManualState("database")
cache := statekit.NewManualState("cache")

app.AddCheck(db)
app.AddInformationalCheck(cache)
reg.Register(app)

requests := statekit.NewCounter("requests_total", "Total requests served.")
inflight := statekit.NewGauge("inflight_requests", "Requests currently being served.")
reg.RegisterCollectors(requests, inflight)

db.Fail("connection refused", nil)
requests.Inc()
inflight.Set(2)

reg.Mount(http.DefaultServeMux, "/")
```

## Built-in collectors

Common instrumentation lives in the `collectors` package:

```go
import (
	"net/http"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/collectors"
)

reg := statekit.NewRegistry()

httpMetrics := collectors.NewHTTPMetrics(collectors.WithHTTPMetricsWindow(5 * time.Minute))
runtimeMetrics := collectors.NewRuntimeMetrics(collectors.WithRecommendedRuntimeMetrics())
reg.RegisterCollectors(httpMetrics, runtimeMetrics)

httpState := statekit.NewStateAggregator("http")
httpState.AddCheck(
	collectors.NewHTTPErrorRatioCheck(httpMetrics, "http errors", 20, 0, 0.05, 0),
	collectors.NewHTTPAverageLatencyCheck(httpMetrics, "http latency", 250*time.Millisecond, 0, 0),
)
reg.Register(httpState)

mux := http.NewServeMux()
mux.HandleFunc("GET /orders/{id}", getOrder)

http.ListenAndServe(":8080", httpMetrics.Middleware(mux))
```

`HTTPMetrics` records global measurements that are available locally:

- `httpMetrics.Requests()`
- `httpMetrics.RequestsPerSecond()`
- `httpMetrics.Errors()`
- `httpMetrics.ErrorsPerSecond()`
- `httpMetrics.ErrorRatio()`
- `httpMetrics.ErrorPercentage()`
- `httpMetrics.AverageLatency()`
- `httpMetrics.ResponseCodes()`
- `httpMetrics.ErrorURLs()`
- `httpMetrics.UnknownURLs()`

These local measurements are evaluated over a rolling window. The default is
five minutes; override it with `collectors.WithHTTPMetricsWindow`. Viewing a
state does not advance or reset the window. Snapshots are cached for one second
by default so frequent local reads stay cheap; use
`collectors.WithHTTPMetricsSnapshotRefresh` to change that interval.

Because the measurements are local, application behavior can use them directly:

```go
if httpMetrics.ErrorRatio() > 0.20 && httpMetrics.AverageLatency() > 200*time.Millisecond {
	enableLoadShedding()
}
```

The same measurements are exported for Prometheus:

- `http_server_requests_total`
- `http_server_errors_total`
- `http_server_requests_per_second`
- `http_server_errors_per_second`
- `http_server_average_latency_seconds`
- `http_server_response_codes`
- `http_server_error_urls`
- `http_server_unknown_urls`

Factories such as `NewHTTPErrorRatioCheck`, `NewHTTPErrorCountCheck`,
`NewHTTPAverageLatencyCheck`, `NewHTTPRequestsPerSecondCheck`, and
`NewHTTPErrorsPerSecondCheck` return regular state objects, so they can be
inspected directly or added to an aggregate state.

Local snapshots and collectors can also be rendered directly without building a
registry:

```go
stateJSON, _ := statekit.SnapshotJSON(httpState)
metricsYAML, _ := statekit.PrometheusCollectorYAML(httpMetrics)
```

`RuntimeMetrics` exports Go's `runtime/metrics` values as Prometheus samples
with a `go_runtime_` prefix. By default it exports every non-bad runtime metric
Go exposes. Use `collectors.WithRuntimeMetricsWhitelist` to keep an explicit set
by Prometheus name or raw `runtime/metrics` name, or
`collectors.WithRecommendedRuntimeMetrics` to use `RecommendedRuntimeMetrics`:

```go
runtimeMetrics := collectors.NewRuntimeMetrics(collectors.WithRecommendedRuntimeMetrics())
```

The recommended set keeps goroutines, GC pauses, GC pause CPU, total mapped
runtime memory, released heap memory, and scheduler latency.

Scalar runtime metrics are also available locally through `Value`, using either
Prometheus or raw `runtime/metrics` names:

```go
memory, ok := runtimeMetrics.Value("go_runtime_memory_classes_total_bytes")
```

Runtime checks can evaluate those local values over time. This flags sustained
growth in total runtime memory over a five minute window:

```go
memoryGrowth := collectors.NewRuntimeIncreasingTrendCheck(
	runtimeMetrics,
	"runtime memory growth",
	"go_runtime_memory_classes_total_bytes",
	5*time.Minute,
	5,
	1024*1024,
	5*1024*1024,
	0,
)
```

For a process-level memory view, `MemoryMetrics` reports both Go runtime memory
and OS process memory where the platform exposes it. `NewMemoryTracker` returns
the metrics collector plus a state initialized with warn and fail byte
thresholds:

```go
memory := collectors.NewMemoryTracker("memory usage", 512*1024*1024, 1024*1024*1024)

reg.Register(memory.State)
reg.RegisterCollectors(memory.Metrics)

mux.Handle("/memory", memory.Metrics.Handler("yaml"))
```

The state checks current OS RSS when available, then falls back to Go runtime
memory obtained from the OS. The metrics include Go heap/sys details and OS RSS
or peak RSS samples when available. The memory handler prints the current usage
summary and raw memory snapshot for backoffice/debug views.

To capture a heap profile when memory crosses into warn or fail, pass a profile
directory:

```go
memory := collectors.NewMemoryTracker(
	"memory usage",
	512*1024*1024,
	1024*1024*1024,
	collectors.WithMemoryHeapProfileDir("/var/log/my-service/profiles"),
)
```

## State

A `State` is anything that implements the small interface:

```go
type State interface {
    Name() string
    Snapshot() Snapshot
}
```

A snapshot carries a current status, a reason, history, and optionally a
nested tree of child snapshots.

### Status levels

Every state reports one of four statuses, in ascending severity:

| Status | Meaning                    |
| ------ | -------------------------- |
| `pass` | Everything is fine.        |
| `warn` | Degraded but operational.  |
| `fail` | Broken or unable to serve. |
| `down` | Not reachable at all.      |

### Built-in states

`statekit` ships four built-in implementations: `ManualState`,
`AggregateState`, `FailRatio`, and `FailMap`.

#### Manual state

Set explicitly by the component:

```go
db := statekit.NewManualState("database")
db.Fail("connection refused", nil)
```

#### Aggregate state

Derives a parent status from a flat set of leaf checks. Checks can be added
progressively as subsystems initialize. Each check contributes its own status to
the aggregate; the parent reports the worst. Aggregates reject other aggregates
as checks, so state trees cannot accidentally recurse.

```go
app := statekit.NewStateAggregator("issuer")
app.AddCheck(db)
app.AddInformationalCheck(cache)
```

`AddInformationalCheck` caps a child's contribution at `warn` even if the child
itself reports `fail` or `down`. Use it for optional subsystems whose
failure should not take the whole component down.

Marking the state itself as informational goes further: an informational state
is limited to `pass` or `warn` and can never report `fail` or `down`. The cap is
applied to its own status, so it shows as `warn` everywhere it appears, not just
inside an aggregate:

```go
cache := statekit.NewManualState("cache", statekit.WithImportance(statekit.Informational))
```

#### Fail ratio

Tracks pass/fail outcomes over a sliding window and evaluates them into a
status:

```go
upstream := statekit.NewFailRatio(
    "upstream",
    time.Minute,
    statekit.RatioPolicy{MinSamples: 10, WarnAt: 0.25, FailAt: 0.5},
)

upstream.Pass()
upstream.Fail()
```

`statekit.AllFailed(minSamples, status)` provides a policy that only fires
when every observed outcome in the window failed.

#### Fail map

Tracks named failures by name. Items reported failing stay in the map until
they are reported ok. The state is `pass` while the map is empty and `fail`
while any item is in it. Useful when walking a collection and you want the
snapshot to say both that something is wrong and exactly which items:

```go
shards := statekit.NewFailMap("shards")

for _, shard := range collection {
    if err := shard.Verify(); err != nil {
        shards.Fail(shard.ID, err.Error(), map[string]any{"offset": shard.Offset})
    } else {
        shards.Pass(shard.ID)
    }
}
```

The reason carries the count of failing items. The data carries the map of
items with each entry's since-time and `secs_in_failure`, so the snapshot
shows how long each item has been bad. Repeated `Fail` calls on the same
name preserve the original since-time.

### Custom states

For richer evaluation logic, implement `State` directly. Composing with
a `ManualState` gives you history and time-in-state for free.

A quorum monitor over a set of upstream servers is a good example: it
reports `warn` as soon as one server is unhealthy and `fail` once more
than a configured threshold are down.

```go
// QuorumState wraps a ManualState and reports fail when more than
// maxFailing of its upstreams are unhealthy.
type QuorumState struct {
    state      *statekit.ManualState
    total      int
    maxFailing int

    mu      sync.Mutex
    failing map[string]struct{}
}

func NewQuorumState(name string, total, maxFailing int) *QuorumState {
    return &QuorumState{
        state:      statekit.NewManualState(name),
        total:      total,
        maxFailing: maxFailing,
        failing:    make(map[string]struct{}),
    }
}

func (q *QuorumState) Name() string                { return q.state.Name() }
func (q *QuorumState) Snapshot() statekit.Snapshot { return q.state.Snapshot() }

func (q *QuorumState) Mark(server string, healthy bool) {
    q.mu.Lock()
    defer q.mu.Unlock()
    if healthy {
        delete(q.failing, server)
    } else {
        q.failing[server] = struct{}{}
    }
    msg := fmt.Sprintf("%d/%d upstreams failing", len(q.failing), q.total)
    switch {
    case len(q.failing) > q.maxFailing:
        q.state.Fail(msg, nil)
    case len(q.failing) > 0:
        q.state.Warn(msg, nil)
    default:
        q.state.Pass("", nil)
    }
}
```

Register it like any other state:

```go
hosts := NewQuorumState("upstreams", 3, 1)
reg.Register(hosts)

hosts.Mark("us-east-1", false)
```

Attach related metrics to built-in state reports with `AddMetric`:

```go
dbLatency := statekit.NewGauge("database_latency_ms", "Current database latency.")
db.AddMetric(dbLatency)
reg.Register(db)
```

The state snapshot keeps metrics in a separate `metrics` field, not in `data`.

### Histogram utilities

`Histogram` is a small local utility for keyed distributions and exact
percentiles:

```go
h := statekit.NewHistogram()
h.Add("200", 42)
h.Add("404", 3)

snap := h.Snapshot()
top := snap.Top(5)
covered := snap.TopPercent(95)
p90 := h.Percentile(90)
```

### Display format

`/state` (served as YAML) wraps the current state tree in a stable
document that includes the registry's label hierarchy and an optional
build version (`statekit.WithVersion(...)`). A fleet-wide visualizer
can merge many component documents by `label_path`.

Every registry also maintains a synthetic `health` state. It is always
the first state in the document, with status equal to the worst of all
registered states (informational states cap themselves at warn), data carrying
the count distribution per status, and reason grouping the non-pass
states like `fail:db,xx warn:zzz`.

`/health` returns the aggregated health state as plain text: `pass`, `warn`,
or `fail`. Internal `down` state is reported as `fail` on this endpoint.

```yaml
kind: statekit.state.v1
version: 1.4.2
label_path:
  - name: service
    value: checkout
  - name: example
    value: componentdemo
states:
  - name: health
    status: warn
    importance: important
    reason: "warn:checkout-api"
    data:
      pass: 1
      warn: 1
  - name: checkout-api
    status: warn
    importance: important
    reason: "payments-upstream: failure ratio crossed warn threshold"
    changed_at: 2026-05-16T00:58:30.966234+03:00
    changed_secs_ago: 4
    history:
      - timestamp: 2026-05-16T00:58:25.258633+03:00
        status: pass
        secs_ago: 9
      - timestamp: 2026-05-16T00:58:30.966234+03:00
        status: warn
        secs_ago: 4
        reason: "payments-upstream: failure ratio crossed warn threshold"
    checks:
      - name: database
        status: pass
        importance: important
        changed_at: 2026-05-16T00:58:25.258633+03:00
        changed_secs_ago: 9
        history:
          - timestamp: 2026-05-16T00:58:25.258633+03:00
            status: pass
            secs_ago: 9
      - name: payments-upstream
        status: warn
        importance: important
        reason: failure ratio crossed warn threshold
        data:
          window: 5m0s
          total: 3
          failures: 1
          passes: 2
          fail_ratio: 0.3333333333333333
        changed_at: 2026-05-16T00:58:25.258648+03:00
        changed_secs_ago: 9
        history:
          - timestamp: 2026-05-16T00:58:25.258634+03:00
            status: pass
            secs_ago: 9
          - timestamp: 2026-05-16T00:58:25.258648+03:00
            status: warn
            secs_ago: 9
            reason: failure ratio crossed warn threshold
            data:
              window: 5m0s
              total: 3
              failures: 1
              passes: 2
              fail_ratio: 0.3333333333333333
```

## Metrics

Metrics live inside the component that produces them. Their values are
useful in two directions: a Prometheus scrape exports them to your
time-series store, and the component itself can read them directly to
drive state evaluation or business logic.

### Built-in collectors

`Counter`, `Gauge`, `CounterVec`, and `GaugeVec` cover scalar and labeled
cases:

```go
requests := statekit.NewCounterVec(
    "http_requests_total",
    "Total HTTP requests.",
    "route",
    "status",
)
inflight := statekit.NewGauge("http_inflight_requests", "In-flight HTTP requests.")

reg.RegisterCollectors(requests, inflight)

requests.WithLabelValues("/checkout", "200").Inc()
inflight.Set(3)
```

### Custom collectors

For anything richer, implement the two-method `PrometheusCollector`
interface and the registry will export it. The collector exposes whatever
local API the component needs. You can easily implement or adapt existing maps,
histograms and variables to prometheus metrics, while keeping them useful locally.

A sliding-window rate counter is a good example: it ages out old events on
read, so its current value is meaningful both as a scrape sample and as a
number the component can act on.

> Note: this is just an illustration, not a production grade metric.

```go
// RateCounter counts events over a sliding window. Count() returns
// the current value for local decisions; Prometheus scrapes the same
// value through the collector interface.
type RateCounter struct {
    name, help string
    window     time.Duration

    mu     sync.Mutex
    events []time.Time
}

func NewRateCounter(name, help string, window time.Duration) *RateCounter {
    return &RateCounter{name: name, help: help, window: window}
}

func (c *RateCounter) Inc() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.events = append(c.events, time.Now())
}

func (c *RateCounter) Count() int {
    c.mu.Lock()
    defer c.mu.Unlock()
    cutoff := time.Now().Add(-c.window)
    drop := 0
    for drop < len(c.events) && c.events[drop].Before(cutoff) {
        drop++
    }
    c.events = c.events[drop:]
    return len(c.events)
}

func (c *RateCounter) DescribePrometheus() []statekit.PrometheusDesc {
    return []statekit.PrometheusDesc{{Name: c.name, Help: c.help, Type: statekit.PrometheusGauge}}
}

func (c *RateCounter) CollectPrometheus() []statekit.PrometheusSample {
    return []statekit.PrometheusSample{{Name: c.name, Value: float64(c.Count())}}
}
```

Wire it up like any other collector:

```go
hits := NewRateCounter("hits_per_minute", "Requests in the last minute.", time.Minute)
reg.RegisterCollectors(hits)

hits.Inc()
if hits.Count() > threshold {
    state.Warn("traffic spike", nil)
}
```

## Demo

Run the component demo to try manual states, aggregate state, counters,
gauges, failure-ratio state, JSON snapshots, and Prometheus scraping:

```sh
go run ./examples/componentdemo
```

The demo serves a web UI at `http://localhost:19080/`, the state display
document (YAML) at `/state`, plain health status at `/health`, and
Prometheus text at `/metrics`.

Set `PORT=29080` or another value if port 19080 is already in use.

## Info pages

Applications can mount small browser-readable info pages beside their statekit
endpoints:

```go
reg := statekit.NewRegistry(statekit.WithLabel("component", "issuer"))
store := storage.NewMemoryStore()

mux := http.NewServeMux()
reg.Mount(mux, "/statekit")
mux.Handle("/api/", http.StripPrefix("/api", storage.NewAPI(store).Handler()))
mux.Handle("/info/", http.StripPrefix("/info", infopages.Handler(infopages.Options{
    Title:       "issuer",
    Registry:    reg,
    Storage:     store,
    RegistryURL: "/statekit",
    APIURL:      "/api",
})))
```

The mounted pages include an overview, current registry configuration, exposed
state/metric endpoints, and storage counts.
