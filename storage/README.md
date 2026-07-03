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
