# scraper

`scraper` polls remote `statekit` components on an interval and exposes
the aggregated result through the local `statekit.Registry`. The scraper
is inspired by Prometheus: it works from a configured list of targets,
each running a set of tasks, with cascading defaults for interval,
timeout, and expiration.

## What it produces

A `*Scraper` exposes two things:

- `Scraper.States() []statekit.State` — the top-level states produced
  by the scraper (per-target liveness checks and per-target
  state_aggregation roots). Register each with a `statekit.Registry`.
  The scraper does not wrap them in its own aggregate.
- `Scraper.MetricsCollector() statekit.PrometheusCollector` — a
  collector for samples scraped from target `/metrics` endpoints.
- `scraper.WithMetricsIngestor(...)` — optionally sends successful
  observations to a bounded timeseries aggregator, grouped by `scrape_path`.

Wire-up looks like:

```go
cfg, _ := scraper.LoadConfig("scraper.yaml")
sc, _ := scraper.New(*cfg)

reg := statekit.NewRegistry(statekit.WithLabel("role", "scraper"))
for _, st := range sc.States() {
    reg.Register(st)
}
reg.RegisterCollectors(sc.MetricsCollector())

go sc.Run(ctx)

reg.Mount(http.DefaultServeMux, "/")
```

To retain metrics for the storage console's target drill-down:

```go
metrics := storage.NewMemoryMetricsStore(30*time.Minute, 100)
store := storage.NewMemoryStore(storage.WithMetricsStore(metrics))
sc, _ := scraper.New(*cfg, scraper.WithMetricsIngestor(metrics))
```

## Target shape

Each target has a stable `id` (used for label emission and dedup), a
human `name`, an optional `group_name`, a `base_url`, and one or more
task blocks.

```yaml
targets:
  - id: issuer-prod-east           # stable global identity
    name: issuer                   # display name
    group_name: payments           # logical grouping
    base_url: http://issuer.svc:19080
    labels:
      env: prod
    liveness: [...]                # list of probes
    state_aggregation: { ... }     # optional: mirror remote state tree
    metrics: { ... }               # optional: scrape /metrics
```

Both `target_id` (from `id`) and `group_name` (from `group_name`) are
emitted as labels on every state and metric the target produces. Scraped
metrics also receive `scraped_from`, using the target identifier unless
the sample already carries an upstream `scraped_from`, and `scrape_path`.
The path is nearest-first and ends at the origin, matching state aggregation;
each hop prepends its target identifier to an existing path. When upgrading
from an upstream that has `scraped_from` but no path, the first path-aware
scraper seeds the rightmost element from `scraped_from`.

## Task types

Each target declares one or more tasks. The scraper runs a goroutine
per task with its own ticker.

### `liveness` (list)

A list of HTTP probes. Each entry becomes a child state under the
target.

```yaml
liveness:
  - id: up
    path: /healthz
    importance: important
    expect_status: [200]           # default: any 2xx
    max_latency: 500ms             # fail the attempt if slower
    expect_body_regex: '"status"\s*:\s*"ok"' # optional regex content check
    expect_json: "$.status equals ok" # JSONPath predicate value
    expect_contents: ok             # raw body substring check
    failure_policy:
      fail_after: 3                # consecutive failures before fail/down
      recover_after: 2             # consecutive successes before recovering
    labels:
      probe: http
```

Each check's resulting state is named `<target>.<check-id>`.

Implemented today: `method`, `expect_status`, `max_latency`,
`expect_body_regex`, `expect_json`, `expect_json_path`,
`expect_contents`, and
`failure_policy`.

### `state_aggregation`

Fetches the target's state display document (`/state`, YAML) and
exposes its top-level scraped state through the scraper. Children,
history, importance, reasons, and `data` fields are preserved
verbatim. The top-level state's `scraped_from` is annotated with the
target identifier (existing chains are preserved by prepending).

```yaml
state_aggregation:
  path: /state
  labels:
    subsystem: issuer
```

Content type is detected from the response header. On scrape failure,
the last successful tree is kept and the wrapper state goes `down`
until the next success (or until `expiration` elapses, at which point
even the last-known tree is treated as stale).

The resulting state is named `<target>.state`.

### `metrics`

Fetches Prometheus text from one or more paths, parses the samples,
attaches the merged labels, and re-emits them through the scraper's
metrics collector.

```yaml
metrics:
  paths:
    - /metrics
    - /state/metrics
  labels:
    subsystem: issuer
  drop_scrape_path: false          # set at an export boundary to omit the path
```

Sample lines, `# HELP`, and `# TYPE` are all preserved. On metric-name
collisions across targets, descriptors follow first-wins; samples are
disambiguated by their labels (including the auto-added `target_id`).

## Configuration

```yaml
labels:                            # applied to every state and sample
  scraper: fleet-aggregator
  region: us-east-1

defaults:                          # task-level fallback values
  interval: 15s
  timeout: 5s
  expiration: 1m                   # mark state down if no fresh data
  http_liveness:
    expect_status: [200]
    max_latency: 1s
    failure_policy:
      fail_after: 3
      recover_after: 2

targets:
  - id: issuer-prod-east
    name: issuer
    group_name: payments
    base_url: http://issuer.svc:19080
    labels:
      env: prod
    interval: 30s                  # override scraper default
    liveness:
      - id: up
        path: /healthz
        importance: important
        expect_status: [200]
        max_latency: 500ms
        expect_json:
          - "$.status equals ok"
          - "$.errors equals []"
        expect_contents: ok
        failure_policy:
          fail_after: 3
          recover_after: 2
    state_aggregation:
      path: /state
      labels:
        subsystem: issuer
    metrics:
      paths: [/metrics]
      labels:
        subsystem: issuer
```

### Cascading values

`interval`, `timeout`, and `expiration` resolve from most-specific to
least-specific:

```
task.<field> → target.<field> → defaults.<field> → hard-coded default
```

HTTP liveness defaults under `defaults.http_liveness` apply to every
liveness check unless that check sets the same field directly.

### Label merge

Labels are merged in order from least to most specific, with later
levels overriding on key conflict:

```
scraper.labels → target.labels → task.labels
```

After merging, the scraper appends structural labels:

- `target_id` from the target's `id`
- `group_name` from the target's `group_name`
- `scraped_from` on scraped metrics, preserving an existing non-empty
  upstream value
- `scrape_path` on scraped metrics, prepending the current target identifier
  to an existing upstream path. Set `metrics.drop_scrape_path: true` at a
  boundary that should not re-emit this Statekit aggregation provenance.

Registry-generated state metrics (`state_level` and
`state_time_in_state_seconds`) inherit `scraped_from` and `scrape_path` from
their state tree, including checks. To remove the path from the complete
Prometheus output—both generated state metrics and scraped collectors—apply
`statekit.DropPrometheusLabels("scrape_path")` to the export handler:

```go
mux.Handle("/metrics", reg.PrometheusHandler(
    statekit.DropPrometheusLabels("scrape_path"),
))
```

## Expiration

If no successful scrape happens within `expiration`, the affected state
flips to `down` with reason `"stale (no scrape within expiration)"`.
For `state_aggregation`, the last-known children remain visible under
the stale wrapper so operators can see "last known good plus we lost
contact." Set `expiration: 0` (or omit) to disable.

## Failure policy

`failure_policy` on a liveness check turns probe results into state
transitions:

- `fail_after`: require this many consecutive failed probes before the
  state goes `fail` or `down`. Default: `1`.
- `recover_after`: require this many consecutive successful probes
  before a `fail` or `down` state returns to `pass`. Default: `1`.

Transport, request-construction, and body-read errors are reported as
`down`, because the probe could not reach or read the endpoint. Completed
HTTP responses that violate expectations are reported as `fail`: status
not in
`expect_status` (or not 2xx if empty), elapsed time exceeding
`max_latency`, body not matching `expect_body_regex`, an `expect_json`
assertion failing, `expect_json_path` not resolving to a non-empty value,
or body not containing `expect_contents`.

`expect_json` is the preferred JSON assertion form:

```yaml
expect_json: "$.status equals ok"
```

It may also be a list:

```yaml
expect_json:
  - "$.status equals ok"
  - "$.errors equals []"
```

The expression is parsed as `<jsonpath> <predicate> <value>`. Supported
predicates today are `equals` and `==`. A bare path is treated as an
existence check:

```yaml
expect_json: "$.version"
```

## Demos

- [`examples/scrapedemo`](../examples/scrapedemo) — scraper + the
  existing `componentdemo` (two terminals).
- [`examples/fleetdemo`](../examples/fleetdemo) — two components and a
  scraper in one process (one terminal). Useful for quickly seeing the
  multi-target output shape.

## Limitations and follow-ups

- Per-state labels are currently stashed in `Snapshot.Data["labels"]`
  rather than being a first-class field on `Snapshot`.
- `# HELP` / `# TYPE` for scraped metrics may be absent on the first
  scrape cycle because descriptors are registered eagerly at
  `RegisterCollectors` time.
- Scrapes use the default Go `http.Client`. There is no built-in
  support for TLS configuration, custom headers, or authentication
  yet — wrap your own transport or run the scraper behind a sidecar
  that handles those concerns.
- JSONPath support currently covers the common dot-and-index form, such
  as `$`, `$.status`, and `$.items[0].status`. Full RFC 9535 filter and
  slice syntax is not implemented yet.
- Config is read once at startup. Hot-reload is not implemented.
