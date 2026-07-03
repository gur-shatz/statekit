# Storage and API redesign: data layers

Date: 2026-07-03
Status: implemented (phases 1-4, 2026-07-03)
Scope: `storage/` (store, API, projections), the two UI clients (`storage/ui.js`, `console/app.js`), and the ingest path.

## 1. Problem statement

Two symptoms as results accumulate: API responses grow large and slow, and the process accumulates memory. Both trace to the same root causes, confirmed by review:

1. Storage mixes concerns. Identity metadata, current state, timeline events, and data payloads are entangled, so every ingest and every read pays for all of them, and the timeline part grows without bound.
2. The API shape does not match the UI. The UIs are summary-first with drill-down on selection, but every endpoint returns fully hydrated wide documents, refetched in full every 5 seconds.

## 2. The organizing idea: three data layers

The design question is not "how do we normalize storage" but "what are the layers of data we want". Three, by access pattern:

| Layer | Contents | Excludes | Access pattern | Size behavior |
|---|---|---|---|---|
| **L1** | Current: targets and their states (status, reason, changed_at, rollups) | data payloads, timeline | polled constantly, whole-fleet reads | tiny, O(fleet), replaced in place |
| **L2** | Current: full state detail including `data` | timeline | fetched per target on drill-down | O(fleet × data size), replaced in place |
| **L3** | Historical and exhaustive: transitions, time-bucketed rollups, incident history, anything big or old | — | fetched per identity / per window, on demand | append-only, **explicitly bounded**, archivable |

Storage reflects the layers directly. Duplication across L1/L2/L3 is accepted: L1 and L2 are current-only and replaced on every ingest, so their size is a function of fleet size, not of time. Only L3 grows with time, so L3 is the only layer that needs retention policy, and the natural candidate to page to disk later. This is deliberately simpler than content-addressed interning or derive-on-read projections; bounded duplication is cheaper than the machinery to avoid it.

## 3. Review findings (what breaks today, mapped to layers)

### 3.1 Today's store has no layers

`IngestDocument` (storage/storage.go:281) fans each state into four overlapping copies:

- `s.nodes` + `s.current`: metadata and observation, with `Data` inline — an L1/L2 blend.
- `s.targets`: a second fully materialized copy of everything, including another `Data` and `Labels` clone per state (storage/targets.go:123) — L1 and L2 blended again, duplicated wholesale.
- `s.events`: status, reason, times, and the same `Data` clone, kept **forever** — an unbounded L3 with L2-sized entries.

Every read endpoint serves the blend: `/state/current` and `/state/targets` both carry per-state `data` for the whole fleet.

### 3.2 The unbounded event log is the memory leak

The primary growth cause, and a divergence between code and documented intent:

- `storage/README.md` says events dedupe by `identity + changed_at + status + reason`.
- The actual event key (storage/storage.go:545) also hashes **`updated_at`**, and `stateTracker.set` (state_tracker.go:46) bumps `updatedAt` on every update even when status is unchanged.

So every scrape of an actively updating state appends a new `StateEvent`, with a cloned `Data` map, to `s.events` and `s.order`, which are never pruned. At stackdemo's 1 s ingest cadence that is ~86,400 retained events per state per day; at the scraper default of 15 s (scraper/scraper.go:19), a 1,000-state fleet retains ~5.7 M events per day. Dynamic `reason` strings (part of the key) and never-expiring incidents compound it.

In layer terms: L3 entries are minted per observation instead of per transition, carry L2 payloads, and have no bound.

### 3.3 The API/UI mismatch

From tracing both UI clients:

1. Both UIs poll all four read endpoints in full every 5 s, unconditionally (ui.js:843, app.js:807). No ETag / `If-None-Match` anywhere.
2. `/state/current` and `/state/targets` are ~redundant on the wire; each cycle ships most of the store twice.
3. The UIs need L1 constantly, L2 only for the selected target, L3 only for the selected identity and the sparkline. The API's smallest unit is "everything about everything".
4. `material_hash`, `metadata_hash`, `data_hash` are computed on every ingest and read by no client.
5. All server-side filters and `/state/groups` go unused; both UIs filter, sort, and aggregate client-side, and both ship a JS reimplementation of `buildTargetDocuments` ("projection mode").
6. The heaviest client work is replaying the raw 500-event stream into timelines and the 64-bucket sparkline on every refresh and hover (ui.js:450-551, app.js:323-351) — a client-side L3 rollup that belongs server-side.

### 3.4 Transitions are already available upstream

`Snapshot` carries a bounded `History` of real transitions (state.go:203). Storage ignores it and re-derives a timeline by diffing observations — the mechanism that leaks. L3 should be fed from `History` and `changed_at` boundaries: an unchanged state contributes nothing per scrape, and dedup is by construction.

## 4. Proposed storage

One store, three layer-shaped structures, all written per ingest:

```
L1  targets:   key -> TargetSummary        (name, labels, worst_status, status_counts,
                                            affected_states, material_hash, observed_at)
    states:    key -> []StateHeader        (identity, name, parent, status, reason,
                                            importance, changed_at; NO data)

L2  detail:    identity -> StateDetail     (StateHeader + help, label_path, data,
                                            data_hash, updated_at, first/last seen)

L3  timeline:  identity -> ring[Transition]   {changed_at, status, reason}, cap N (default 32)
    charting:  separate timeseries store (see 4.1)
    incidents: as today + retention: TTL for closed, ring cap on events per incident
```

### 4.1 A separate timeseries store for historical charting

Historical charting (the stacked "at any given time, which states were triggering" chart, the sparkline, per-target incident bands) is a different storage problem from the rest of L3 and deserves its own component, with a timeseries-oriented shape rather than the document/map shape of the main store.

The dataset is deliberately very limited:

```
per bucket (e.g. 1m):  timestamp -> [{identity, target_key, label, status}]  for triggering states only
```

Only non-pass states are recorded, so bucket size is proportional to how much is wrong, not to fleet size; a healthy fleet stores near-nothing. A fixed window (e.g. 24h at 1m resolution = 1,440 buckets) with round-robin overwrite gives a hard, precomputable bound. Coarser roll-ups (5m/1h tiers, RRD-style) can extend the horizon later without changing the write path.

`label` is the display name (`target:check`), captured at write time. This makes the store self-sufficient for rendering: a state that was triggering an hour ago may no longer exist in L1/L2, so chart reads must not depend on joins against the current layers.

**It carries its own API**, sufficient for the timeline chart and its tooltips and nothing more:

```
GET /state/timeline?scope=fleet|target:{key}&window=24h&buckets=64
    -> [{t, counts: {warn: n, fail: n, down: n}}]
    Draws the stacked "which states were triggering over time" chart and the
    sparkline directly; one small array, no client-side aggregation.

GET /state/timeline/bucket?scope=...&t=...
    -> [{label, status, identity, target_key}]
    Tooltip/hover detail: the triggering states in one bucket. Fetched lazily
    per hover (or returned inline via ?include=contributors for small windows).
```

Keeping it a separate store behind its own small interface (`Record(bucket, triggering)` / `Range(scope, window)` / `Bucket(scope, t)`) has three benefits:

1. The main store stays current-only; nothing in L1/L2 grows with time.
2. The backend can change independently: in-memory ring first, an embedded timeseries file or tsdb later, without touching the state model or the chart API.
3. It answers every chart query by lookup, replacing both UIs' client-side replay of the raw event stream (today an O(contributors × buckets × events) recomputation per refresh and per hover).

Rules:

1. **L1 and L2 are replaced in place** on each ingest (doc-scope replacement as today, plus a `LastSeenAt` TTL sweep for sources that die silently). They cannot grow with time.
2. **L3 accepts only transitions**, keyed `identity + changed_at + status`, fed from `Snapshot.History` and observed `changed_at` changes. `updated_at` is not part of any event identity. Per-identity ring cap plus a global backstop; both constructor options.
3. **The charting store is written at ingest** (record this bucket's triggering states), so the sparkline/timeline endpoint is a read, not a replay.
4. Worst-case memory is computable from configuration: L1 + L2 are O(fleet), L3 is rings × caps plus the charting window. Disk persistence, when it comes, is an L3 backend (and optionally L2 snapshots); L1 always stays in memory.

The ingest write path stays simple: flatten, update L1 summaries incrementally, replace L2 details, append L3 transitions when the ring head differs. No content-addressed payload store, no derive-on-read.

## 5. Proposed API

One endpoint family per layer:

```
L1  GET /state/summary                 fleet rollup: worst, counts, fleet_hash, observed_at
    GET /state/targets                 []TargetSummary + StateHeaders (no data)

L2  GET /state/targets/{key}           full detail for one target: states + checks + data
    GET /state/states/{identity}       one state: detail + child identities

L3  GET /state/states/{identity}/timeline    transition ring
    GET /state/timeline?...                   bucketed series   (charting store, 4.1)
    GET /state/timeline/bucket?...            tooltip detail    (charting store, 4.1)
    GET /escalations/incidents               (summary: no events[])
    GET /escalations/incidents/{source}/{id} (full, with events)
```

Conditional polling: every GET sets `ETag` (`fleet_hash`, `material_hash`, or `data_hash` as appropriate) and honors `If-None-Match` with 304. Steady-state poll for an idle fleet: one small `/state/summary` body, 304 for everything else. The hashes the store already computes finally earn their keep.

UI mapping: the 5 s tick polls L1; selecting a target fetches its L2 document (keyed by `material_hash` change); the detail pane and sparkline fetch L3 per identity/window. Client-side projection mode, event replay, and re-aggregation are deleted from both UIs.

Retirements (decided, see section 7): `/state/current` is dropped; `/state/events` global feed is dropped (no external consumers; replaced by the L3 endpoints); `/state/groups` is dropped (no callers; re-add if the overview ever adopts grouping).

## 6. Migration plan

1. **Stop the leak (no API change).** Fix the event key to match the README (drop `updated_at`), feed transitions from `changed_at`/`History`, cap the rings, add incident retention. This alone flattens the memory curve.
2. **Restructure storage into L1/L2/L3.** Introduce the charting store (in-memory ring backend first). Existing endpoints keep working by assembling their old wide shapes from the layers during the transition.
3. **API v2.** Add the layer endpoints and ETag support alongside the old ones.
4. **UI migration.** Console first, then the storage UI, then delete client projections and retired endpoints.

## 7. Decisions (resolved 2026-07-03)

1. **L1 includes per-state `StateHeader`s.** The fleet rail and states column render from the single L1 poll; headers carry no data payloads.
2. **No external consumers of `/state/events` or `material_hash` exist.** Events become bounded per-identity rings; the global feed and `/state/groups` are retired; `/state/current` is dropped. `material_hash` semantics stay documented for a possible future durable flusher.
3. **Implementation scope: phases 1-3.** UI migration (phase 4) is a separate follow-up.
4. **L3 depth**: storage transition ring cap defaults to 32 (matching the tracker's own history depth); a constructor option, set deliberately.
5. **L3 on disk** stays future work; the charting store interface is the seam for it.

## 8. Implementation notes

For an implementer working from this document alone.

### 8.1 Core structures

Names are non-binding; shapes and layer assignment are the design. All state lists are **flat with parent pointers** (`ParentIdentity`), not nested check trees: both UIs already index by parent to build trees (app.js:82-91), and one flat shape serves L1 and L2 uniformly.

**L1 — polled constantly, no data payloads.**

```go
// GET /state/summary response. The 5 s poll; fleet_hash is its ETag.
type FleetSummary struct {
    WorstStatus  string         `json:"worst_status"`
    StatusCounts map[string]int `json:"status_counts"` // by state
    Targets      struct {
        Total    int            `json:"total"`
        ByStatus map[string]int `json:"by_status"`     // by target worst_status
    } `json:"targets"`
    FleetHash  string    `json:"fleet_hash"`  // hash of sorted (key, material_hash) pairs
    ObservedAt time.Time `json:"observed_at"`
}

// One rail entry. GET /state/targets returns []TargetSummary plus,
// per decision 7.1, the StateHeaders for each target.
type TargetSummary struct {
    Key          string            `json:"key"`
    Name         string            `json:"name"`
    ScrapePath   string            `json:"scrape_path"`
    Labels       map[string]string `json:"labels,omitempty"`
    WorstStatus  string            `json:"worst_status"`
    StatusCounts map[string]int    `json:"status_counts"`
    AffectedStates []AffectedState `json:"affected_states,omitempty"` // as today
    MaterialHash string            `json:"material_hash"`             // ETag for the L2 detail
    ObservedAt   time.Time         `json:"observed_at"`
    States       []StateHeader     `json:"states"`
}

// The light per-state row: enough for the states column, nothing heavy.
type StateHeader struct {
    Identity       string    `json:"identity"`
    ParentIdentity string    `json:"parent_identity,omitempty"`
    TargetKey      string    `json:"target_key"`
    Name           string    `json:"name"`
    Status         string    `json:"status"`
    Reason         string    `json:"reason,omitempty"`
    Importance     string    `json:"importance"`
    ChangedAt      time.Time `json:"changed_at"`
}
```

**L2 — fetched on drill-down, carries the heavy fields.**

```go
// GET /state/targets/{key} response: the summary plus full state details.
type TargetDetail struct {
    TargetSummary               // States field holds headers; Details below adds the rest
    Details []StateDetail `json:"details"`
}

// GET /state/states/{identity} response (plus child identities).
type StateDetail struct {
    StateHeader
    Help        string                       `json:"help,omitempty"`
    GroupName   string                       `json:"group_name,omitempty"`
    Labels      map[string]string            `json:"labels,omitempty"`
    LabelPath   []statekit.StateDisplayLabel `json:"label_path,omitempty"`
    Data        map[string]any               `json:"data,omitempty"`
    DataHash    string                       `json:"data_hash"` // ETag component
    UpdatedAt   time.Time                    `json:"updated_at,omitempty"`
    ObservedAt  time.Time                    `json:"observed_at"`
    FirstSeenAt time.Time                    `json:"first_seen_at"`
    LastSeenAt  time.Time                    `json:"last_seen_at"`
}
```

**L3 — historical, bounded.**

```go
// Ring entry per identity, cap 32. Dedup key: identity + changed_at + status.
// updated_at is deliberately NOT part of identity or content (section 3.2).
type Transition struct {
    ChangedAt time.Time `json:"changed_at"`
    Status    string    `json:"status"`
    Reason    string    `json:"reason,omitempty"`
}

// GET /state/states/{identity}/timeline response.
type StateTimeline struct {
    Identity    string       `json:"identity"`
    Transitions []Transition `json:"transitions"` // newest first
}

// Charting store entries (section 4.1). Label is the display name
// ("target:check"), captured at write time so reads need no L1/L2 join.
type TriggeringState struct {
    Identity  string `json:"identity"`
    TargetKey string `json:"target_key"`
    Label     string `json:"label"`
    Status    string `json:"status"`
}

// GET /state/timeline response element.
type BucketCounts struct {
    T      time.Time      `json:"t"`
    Counts map[string]int `json:"counts"` // warn/fail/down only
}
```

Incidents (`Incident`, `IncidentEvent`) keep today's shapes (storage/storage.go:149-178), gaining only the retention bounds from 8.3.

**Interfaces.**

```go
// Ingest signatures unchanged; reads reshaped per layer.
type Store interface {
    IngestDocument(ctx context.Context, doc statekit.StateDisplayDocument, observedAt time.Time) error
    IngestEscalations(ctx context.Context, source string, doc statekit.EscalationDisplayDocument, observedAt time.Time) error

    Summary(ctx context.Context) (FleetSummary, error)                    // L1
    Targets(ctx context.Context) ([]TargetSummary, error)                 // L1
    TargetDetail(ctx context.Context, key string) (TargetDetail, error)   // L2
    StateDetail(ctx context.Context, identity string) (StateDetail, error) // L2
    StateTimeline(ctx context.Context, identity string) (StateTimeline, error) // L3

    Incidents(ctx context.Context, filter IncidentFilter) ([]Incident, error)
    AcknowledgeIncident(ctx context.Context, source, id string, at time.Time) error
}

// Separate component; scope is "fleet" or "target:{key}".
type ChartStore interface {
    Record(bucket time.Time, triggering []TriggeringState)
    Range(scope string, from, to time.Time, buckets int) ([]BucketCounts, error)
    Bucket(scope string, t time.Time) ([]TriggeringState, error)
}
```

Internal memory layout follows section 4 directly: `targets map[key]TargetSummary`, `headers map[key][]StateHeader` (L1); `details map[identity]StateDetail` (L2); `timeline map[identity]*ring[Transition]` plus the ChartStore and incidents (L3). L1/L2 maps are replaced per doc-scope on ingest, exactly like today's `docScopeIdentities` mechanism (storage.go:304-316).

### 8.2 Concrete anchors in today's code

1. **Event key bug**: `eventKey` at storage/storage.go:545 hashes `updated_at`; `stateTracker.set` (state_tracker.go:46) bumps it every update. The transition key must be `identity + changed_at + status` only.
2. **`flattenSnapshot` (storage/storage.go:526) currently ignores `snap.History`** (state.go:203). Phase 1 must consume it: each `HistoryEntry` {Timestamp, Status, Reason} is a transition; merge with the observed head; dedupe by the transition key. Note scraped documents cap history at 4 entries by default (`defaultHistoryLimit`, registry_display.go:51).
3. **Delete `s.events`/`s.order`** (unbounded maps, storage.go:191-192) in favor of per-identity rings.
4. **`buildTargetDocuments` (storage/targets.go:5)** becomes the L1 summary + L2 detail writer; stop materializing the wide `TargetDocument.States` copy.
5. **Incidents**: add TTL for closed incidents and a ring cap on events per incident; index by `source+id` to kill the linear scans in `AcknowledgeIncident` (storage.go:483) and `findIncident` (api.go:285).
6. **API handlers** live in storage/api.go; add `If-None-Match`/304 handling there. `fleet_hash` = hash of the sorted `(key, material_hash)` pairs, recomputed incrementally at ingest.
7. **openapi.yaml** must be rewritten for the new surface; storage/README.md updated (its event-dedup claim is currently false).
8. **Ingest cadence context**: scraper default 15 s (scraper/scraper.go:19); stackdemo ingests every 1 s (examples/stackdemo, `ingestFleetState`) — sizing and tests should assume the 1 s case.

### 8.3 Defaults (constructor options)

- Transition ring: 32 per identity; global backstop 100k transitions.
- Charting store: 1 m buckets × 24 h window (1,440 buckets), fleet + per-target scopes.
- Closed-incident TTL: 24 h; incident event ring: 100.
- Node/target TTL sweep: evict identities not seen for 10× their observed ingest interval (fallback 10 min).

### 8.4 Testing and conventions

- Extend storage/storage_test.go style (plain go test in this package today). Must-cover: no event growth across repeated ingests of an unchanged-status document with changing `updated_at`/`reason`/data; ring cap enforcement; doc-scope replacement still prunes; ETag/304 round-trips; charting store bucket bounds and round-robin overwrite.
- Receiver name `this` for new struct methods (repo owner preference).

### 8.5 OpenAPI flow

Today `storage/openapi.yaml` is hand-written, embedded via `go:embed` (api.go:16), and served at `GET /openapi.yaml`. Nothing checks it against the handlers, which is how the README's event-dedup claim (and any spec claim) can silently drift.

Flow for the redesign, spec-first without codegen:

1. **The spec is written before the handlers, per phase.** In phase 3 the v2 `openapi.yaml` (layer endpoints, ETag/304 responses, charting endpoints, retirements) is the first commit and the review artifact for the API shape; handlers implement it.
2. **Go structs stay the single source of types.** No oapi-codegen; the existing json-tagged storage types are the truth and the spec's schemas describe them. Codegen would duplicate every type for a ~10-endpoint API; not worth the machinery (same reasoning as section 2).
3. **A contract test prevents drift.** A test in the storage package loads the embedded spec (kin-openapi), exercises every documented route against a seeded store through the real handler mux, and validates each response (status, headers including ETag, body schema) against the spec. Undocumented routes and unimplemented documented routes both fail the test.
4. Serving stays as-is: embed + `GET /openapi.yaml`.
- Old endpoints (`/state/current`, `/state/events`, `/state/groups`) survive phases 1-3 wired to the new layers (their only clients are the two in-tree UIs); they are deleted in phase 4 together with the UI migration.
