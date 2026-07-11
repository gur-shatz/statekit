# stackdemo

Runs a three-layer statekit fleet in one process:

- leaf components with manual state controls
- regional scrapers loaded from YAML config files
- a fleet aggregator loaded from YAML config, with storage API
- support escalation capture from leaves into central incident storage

Start it:

```sh
go run ./examples/stackdemo
```

For local iteration, enable the opt-in quit endpoint:

```sh
go run ./examples/stackdemo -kill-url
curl http://localhost:19110/-/quit
```

Open:

```text
http://localhost:19110/
```

Useful endpoints:

```text
/leaf/checkout-east/          mutable leaf UI
/leaf/billing-east/           mutable leaf UI
/leaf/checkout-west/          mutable leaf UI
/leaf/search-west/            mutable leaf UI
/scraper/east/state           regional scraper state
/scraper/west/state           regional scraper state
/fleet/state                  fleet aggregator state
/console/                     fleet state console dashboard
/api/escalations/incidents    stored escalation incidents
/api/openapi.yaml             storage API contract
/-/quit                       stop the process when -kill-url is enabled
```

The scraper configs are real files:

```text
examples/stackdemo/config/scraper-east.yaml
examples/stackdemo/config/scraper-west.yaml
examples/stackdemo/config/fleet-aggregator.yaml
```

Change a leaf state in the UI, wait a few seconds, then refresh the
fleet state view or the console to see the change propagate through
both scraper layers.

To try escalations, open a leaf component and use **Create Escalation**.
The leaf serves it at `/leaf/<component>/escalations`; the regional
scraper collects and acknowledges it using the same endpoint with
`?ack=<watermark>`, then stores it under `/api/escalations/incidents`.
