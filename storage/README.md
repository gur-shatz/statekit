# storage

`storage` stores `statekit.StateDisplayDocument` values in a query-friendly
shape. The ingestion boundary is the full state document received from a
component or scraper:

```go
store := storage.NewMemoryStore()
_ = store.IngestDocument(ctx, doc, time.Now())
```

## Data layers

Storage is organized in three layers by access pattern:

- **L1** (`FleetSummary`, `TargetSummary` with flat `StateHeader`s): the
  current fleet view without data payloads, polled constantly and replaced in
  place on every ingest. Size is O(fleet), never a function of time.
- **L2** (`TargetDetail`, `StateDetail`): full current detail including
  `data`, fetched per target or per state on drill-down, also replaced in
  place.
- **L3** (per-identity `Transition` rings, the charting store, incidents):
  historical and explicitly bounded. Transitions are keyed by
  `identity + changed_at + status` and fed from `Snapshot.History` and
  observed `changed_at` boundaries, so a scrape of an unchanged state stores
  nothing regardless of `updated_at`, `reason`, or `data` churn.

Bounds are constructor options with deliberate defaults: transition rings of
32 per identity with a 100k global backstop (`WithTransitionRing`), a
24h/1-minute charting window (`WithChartStore`), closed-incident TTL of 24h
and 100 events per incident (`WithIncidentRetention`), and eviction of
sources that go silent for ten of their own ingest intervals
(`WithEvictionFallback` sets the fallback TTL). Worst-case memory is
computable from configuration.

## Metrics timeseries

Metrics aggregation is opt-in. Create a bounded metrics store and connect the
scraper to it explicitly:

```go
metrics := storage.NewMemoryMetricsStore(30*time.Minute, 100)
store := storage.NewMemoryStore(storage.WithMetricsStore(metrics))
sc, _ := scraper.New(cfg, scraper.WithMetricsIngestor(metrics))
```

Samples are keyed by their complete `scrape_path`. Repeated strings are held
once in a dictionary, label sets are encoded as integer postings, and each
series stores parallel `[]int64` Unix-second timestamps and `[]float64`
measurements. The default window is 30 minutes and each metric/key retains at
most 100 label sets; when cardinality exceeds that cap, the label sets with
the smallest latest values are dropped. Override the backend with
`WithMetricsStore(NewMemoryMetricsStore(retention, seriesCap))`.

Timeseries responses preserve OpenMetrics `UNIT` metadata on each metric
family. Each returned label-set series also includes `constant`, which is true
when all values in the requested window are equal (including a one-point
series). The console presents those scalar values separately from changing
charts and formats known base units such as seconds and bytes. Counter charts
show reset-aware deltas between observations, and the metrics drawer can
automatically refresh at an operator-selected interval.

The point payload is approximately 16 bytes per retained observation:

`keys × metrics/key × series/metric × (retention / scrape interval) × 16`

At a 15-second scrape interval, a 30-minute series has 120 points, or 1.9 KiB
of timestamp/value payload. For 100 metrics averaging 10 label sets on one
key, that is about 1.9 MiB plus slice, map, postings, and interned-string
overhead (typically a few additional MiB). At the hard 100-series cap it is
about 19.2 MiB of point payload per key. A one-second interval costs 15 times
as much. Go slice spare capacity can temporarily make the point columns
approach twice their logical size.

When no metrics store is supplied, `GET /metrics/status` returns
`{"enabled":false}`, `/metrics/timeseries` returns 404, and the console hides
the target Metrics buttons.

## Self-observability

Storage exposes its own state and Prometheus collector through
`store.Observability()`:

```go
store := storage.NewMemoryStore(...)
_ = registry.Register(store.Observability())
```

The `storage` state contains `estimated_size_kib`, `items`,
`metrics_aggregator_enabled`, and any persistence backend error. It warns when
the journal or file chart backend reports an error.

The same data is scrapeable as:

- `statekit_storage_estimated_size_kib{area="..."}` — estimated retained
  memory for current state, transitions, charts, metrics, incidents, mutes,
  bookkeeping, document-cache capacity, and the total.
- `statekit_storage_items{kind="..."}` — targets, states, transitions,
  incidents/events, chart entries, metric keys/families/series/points/labels,
  cache entries, and the total.
- `statekit_storage_metrics_aggregator_enabled`
- `statekit_storage_backend_error`

Sizes include point/slice capacity and approximate Go map/string overhead.
They are intended for trend monitoring and capacity alerts, not as an exact
replacement for Go heap metrics. The observer caches the structural scan for
one second so a registry state+metrics scrape computes it once.

The charting store (`ChartStore`) is a separate timeseries component: at
ingest it records, per time bucket, which states were triggering (non-pass),
with display labels captured at write time. It answers the fleet/target
timeline chart and its tooltips by lookup, behind its own small interface so
the backend can later move to disk without touching the state model.

Labels are normalized into `map[string]string` so UIs and future SQL
backends can group by dimensions such as `group_name`, `label:region`,
or `label:service` without parsing serialized YAML.

## File-based storage

Only L3 goes to disk, and only optionally. The layer split states exactly
where disk belongs:

- **L1 never goes to disk.** It is the constantly polled current view,
  O(fleet) and replaced on every ingest; it is rebuilt from the first scrape
  cycle after a restart, so persisting it buys nothing.
- **L2 needs at most snapshots.** It is current-only, so a warm-restart
  snapshot (a periodic dump, not a log) is the most a file backend would
  ever do for it. Like L1, it self-heals from the next scrape. Not
  implemented today.
- **L3 is the layer that pages to disk**, because it is the only layer that
  grows with time — and it has two file backends:

```go
chart, _ := storage.NewFileChartStore(dir+"/chart", time.Minute, 24*60)
journal, _ := storage.OpenJournal(dir + "/journal.ndjson")
store := storage.NewMemoryStore(
    storage.WithChartStore(chart),
    storage.WithJournal(journal),
)
```

**`FileChartStore`** is a write-through wrapper around the in-memory chart:
every non-empty bucket write appends one NDJSON line to a day segment
(`chart-YYYY-MM-DD.ndjson`), reads stay in memory, and opening the store
replays the segments still inside the window and deletes the rest. A healthy
fleet appends nothing and unchanged degraded buckets are deduplicated, so
disk usage tracks how much was wrong inside the window, not uptime.

**`Journal`** persists transitions and incidents as one NDJSON log.
`NewMemoryStore` replays it through the normal ingest paths — ring caps,
transition dedup, and incident TTL all apply — then compacts the file back
to exactly the live state; it recompacts whenever the file outgrows its size
bound (`WithJournalMaxSize`, default 4 MiB). Identities whose newest
transition is older than the replay retention (`WithJournalRetention`,
default 72h) are dropped, so identity churn cannot accumulate across
restarts. After a restart the current layers stay empty until the next
scrape, but timelines, incidents, and the chart come back.

Two seams remain for heavier backends: `ChartStore` and `Store` are
interfaces, so an embedded tsdb or SQL implementation drops in the same way,
and **`material_hash`** is the change signal for a durable L2 flusher — a
target summary's hash covers exactly its material content, so a flusher can
write only what changed since the last flush, without diffing.

The document cache (`WithDocumentCache`) is related but separate, and also
in memory: it keeps the last raw ingested document per source as
zstd-compressed YAML for `CachedDocumentYAML` — a debugging convenience,
not a persistence layer.

## HTTP API

```go
api := storage.NewAPI(store)
http.Handle("/api/", http.StripPrefix("/api", api.Handler()))
```

One endpoint family per layer: `/state/summary` and `/state/targets` (L1),
`/state/targets/{key}` and `/state/states/{identity}` (L2),
`/state/states/{identity}/timeline`, `/state/timeline`,
`/state/timeline/bucket`, and the incident endpoints (L3). Every layer GET
sets a strong `ETag` and honors `If-None-Match` with 304, so an idle fleet
polls as one small summary body plus 304s. The pre-layer endpoints
(`/state/current`, `/state/events`, `/state/groups`) are gone; both in-tree
UIs consume the layers directly: the poll reads L1 conditionally, selecting
a target fetches its L2 document revalidated by its `material_hash` ETag,
and history and charts come from the L3 endpoints.

`GET /metrics/timeseries?key=<scrape_path>&window=30m` returns target metrics
grouped first by metric and then label set. Each target row in the console has
a **Metrics** button that opens a two-row chart drawer; it renders one chart
per metric with label sets as separate lines. Charts use the vendored
MIT-licensed uPlot 1.6.32 library for responsive Canvas rendering, local-time
axes, cursor values, and drag-to-zoom (double-click resets the time range).

The API contract is available at `/openapi.yaml` and is enforced by a
contract test: every served route must be documented, every documented route
must be served, and responses are validated against the spec's schemas.

The embedded browser console can be mounted beside that API:

```go
http.Handle("/storage/", http.StripPrefix("/storage",
    storage.UIHandler(storage.UIOptions{APIBase: "/api"})))
```

For a smaller operations-oriented overview of mounted statekit endpoints,
registry configuration, and storage counts, mount the `infopages` package:

```go
http.Handle("/info/", http.StripPrefix("/info", infopages.Handler(infopages.Options{
    Registry: reg,
    Storage:  store,
    APIURL:   "/api",
})))
```
