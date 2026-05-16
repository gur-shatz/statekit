# fleetdemo

Runs two `statekit` components and a scraper in a single process. The
scraper polls both components, mirrors their state trees, and re-emits
their metrics under one aggregated registry. Useful for seeing the full
shape of the scraper output (multiple targets, per-target labels,
`group_name`/`target_id`) without launching three terminals.

## Ports

| Component         | Address          |
| ----------------- | ---------------- |
| `issuer-east`     | `localhost:19082` |
| `issuer-west`     | `localhost:19083` |
| scraper           | `localhost:19084` |

(Ports are uncommon to avoid colliding with typical dev servers on
`8080`/`8081`.)

## Run it

```sh
go run ./examples/fleetdemo

# Aggregated state:
curl -s http://localhost:19084/state

# Aggregated metrics (Prometheus text):
curl -s http://localhost:19084/metrics
```

`issuer-east` reports all healthy; `issuer-west` degrades its cache to
`warn`. Importance flow then bubbles up: cache is `informational`, so
the west aggregate is capped at `warn`, and the scraper's top-level
state mirrors that.

## What to look for

In `/state`:

```yaml
states:
  - name: scraper
    status: warn
    reason: "issuer-west.state: worst child = warn"
    checks:
      - name: issuer-east.up      # liveness probe — pass
      - name: issuer-east.state   # remote tree — pass
        checks:
          - name: issuer-east
            checks: [database, cache, queue]
      - name: issuer-west.up      # liveness probe — pass
      - name: issuer-west.state   # remote tree — warn (cache degraded)
        checks:
          - name: issuer-west
            checks: [database, cache, queue]
```

In `/metrics`:

```
issuer_east_requests_total{group_name="issuers",region="us-east-1",target_id="issuer-east",...} 182
issuer_west_requests_total{group_name="issuers",region="us-west-2",target_id="issuer-west",...} 183
```

`group_name` and `target_id` are added automatically by the scraper
from the corresponding fields in `TargetConfig`. Combined with the
per-target `region` label, every sample is uniquely keyed even when
two targets emit the same metric name.
