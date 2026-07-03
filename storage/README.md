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
