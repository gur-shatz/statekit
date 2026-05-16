# stackdemo

Runs a three-layer statekit fleet in one process:

- leaf components with manual state controls
- regional scrapers loaded from YAML config files
- a fleet aggregator loaded from YAML config, with storage API

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
/api/state/current            stored current states
/api/state/groups?by=group_name
/api/state/groups?by=label:region
/api/state/events?limit=20
/api/openapi.yaml
/storage/                     storage console
/-/quit                       stop the process when -kill-url is enabled
```

The scraper configs are real files:

```text
examples/stackdemo/config/scraper-east.yaml
examples/stackdemo/config/scraper-west.yaml
examples/stackdemo/config/fleet-aggregator.yaml
```

Change a leaf state in the UI, wait a few seconds, then refresh the
fleet state or storage API views to see the change propagate through
both scraper layers.
