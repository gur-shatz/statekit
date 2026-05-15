# statekit component demo

Run the demo server:

```sh
go run ./examples/componentdemo
```

Then open:

- `http://localhost:8080/` for the web UI
- `http://localhost:8080/state` for JSON state snapshots
- `http://localhost:8080/metrics` for Prometheus-style scrape output

The UI controls manual child states, records pass/fail outcomes for a sliding
failure-ratio state, and adjusts demo counters and gauges.

Set `PORT=8081` or another value if port 8080 is already in use.
