# storage

`storage` stores `statekit.StateDisplayDocument` values in a UI-friendly
shape. The ingestion boundary is the full state document received from a
component or scraper:

```go
store := storage.NewMemoryStore()
_ = store.IngestDocument(ctx, doc, time.Now())
```

Each document is flattened into:

- `StateNode`: slow-changing metadata such as name, importance, help,
  group name, and normalized labels.
- `CurrentObservation`: current status, reason, changed time, observed
  time, and data.
- `StateEvent`: transition events deduplicated by
  `identity + changed_at + status + reason`.

Labels are normalized into `map[string]string` so UIs and future SQL
backends can group by dimensions such as `group_name`, `label:region`,
or `label:service` without parsing serialized YAML.

The package also exposes a small HTTP API:

```go
api := storage.NewAPI(store)
http.Handle("/api/", http.StripPrefix("/api", api.Handler()))
```

The API contract is available at `/openapi.yaml`.

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
