# httpcheckdemo

Interactive demo for `collectors.HTTPMetrics` and HTTP check states.

Run it:

```sh
go run ./examples/httpcheckdemo
```

Open `http://localhost:19086/`, then use the buttons to generate good,
slow, or failing requests. The page shows:

- local HTTP measurements from `HTTPMetrics`
- local HTTP checks composed with `statekit.NewStateAggregator`
- the same state tree mirrored back through the built-in scraper at
  `/scraper/state`

Useful endpoints:

- `http://localhost:19086/state`
- `http://localhost:19086/metrics`
- `http://localhost:19086/scraper/state`
- `http://localhost:19086/scraper/metrics`
