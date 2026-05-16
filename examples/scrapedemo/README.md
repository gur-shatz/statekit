# scrapedemo

Runs a `statekit` scraper that polls the `componentdemo` and re-serves
its aggregated state and metrics on its own HTTP port.

## Ports

| Process         | Default address    |
| --------------- | ------------------ |
| `componentdemo` | `localhost:19080`  |
| `scrapedemo`    | `localhost:19081`  |

Both ports are uncommon to avoid colliding with typical dev servers.
Either side accepts overrides — `PORT=…` on `componentdemo`, `-addr=:…`
on `scrapedemo`.

## Run it

```sh
# Terminal 1: run the component being scraped.
go run ./examples/componentdemo
# listening on http://localhost:19080

# Terminal 2: run the scraper, which polls localhost:19080 every 3s.
go run ./examples/scrapedemo
# listening on http://localhost:19081

# In another shell, look at the aggregated output:
curl -s http://localhost:19081/state
curl -s http://localhost:19081/metrics
```

The scraper config is at [`scraper.yaml`](scraper.yaml). It declares
one target (`issuer`) running all three task types — a `liveness`
check, a `state_aggregation`, and a `metrics` scrape.

If `componentdemo` is on a different port, update `base_url` in
`scraper.yaml`.

## Try it interactively

While both demos are running, hit `componentdemo`'s web UI at
`http://localhost:19080/` to flip states and update metrics. Then
refetch the scraper's `/state` within a few seconds and
watch the changes propagate through `issuer.state`.

## See also

[`examples/fleetdemo`](../fleetdemo) bundles two components and a
scraper into a single process for a quicker hands-on view.
