// Command stackdemo runs a three-layer statekit fleet in one process.
//
// Layers:
//   - mutable leaf components under /leaf/*
//   - regional scrapers under /scraper/*
//   - a fleet aggregator under /fleet, with storage API under /api
//
// The scrapers are loaded from YAML files in examples/stackdemo/config.
package main

import (
	"context"
	_ "embed"
	"flag"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gur-shatz/statekit"
	"github.com/gur-shatz/statekit/scraper"
	"github.com/gur-shatz/statekit/storage"
)

const defaultAddr = ":19110"

//go:embed page.html
var pageHTML string

var pageTemplate = template.Must(template.New("page").Parse(pageHTML))

type leafComponent struct {
	mu     sync.RWMutex
	name   string
	group  string
	region string
	prefix string

	reg      *statekit.Registry
	app      *statekit.AggregateState
	database *statekit.ManualState
	cache    *statekit.ManualState
	queue    *statekit.ManualState

	requests   *statekit.Counter
	queueDepth *statekit.Gauge

	escalations      *statekit.Escalations
	lastEscalationID string
}

func newLeaf(name, group, region, prefix string) *leafComponent {
	reg := statekit.NewRegistry(
		statekit.WithLabel("service", strings.TrimSuffix(strings.TrimSuffix(name, "-east"), "-west")),
		statekit.WithLabel("component", name),
		statekit.WithLabel("group_name", group),
		statekit.WithLabel("region", region),
	)
	app := statekit.NewStateAggregator(name,
		statekit.WithHelp("Application-owned aggregate state for "+name+"."))
	database := statekit.NewManualState("database",
		statekit.WithHelp("Connection to the primary database."))
	cache := statekit.NewManualState("cache",
		statekit.WithImportance(statekit.Informational),
		statekit.WithHelp("Cache health. Degraded cache should warn but not fail the service."))
	queue := statekit.NewManualState("queue",
		statekit.WithHelp("Background queue processor."))
	app.AddCheck(database, queue)
	app.AddInformationalCheck(cache)

	requests := statekit.NewCounter(strings.ReplaceAll(name, "-", "_")+"_requests_total", "Total demo requests.")
	queueDepth := statekit.NewGauge(strings.ReplaceAll(name, "-", "_")+"_queue_depth", "Current queue depth.")
	escalations := statekit.NewEscalations(statekit.WithEscalationPolicy(statekit.EscalationPolicy{
		MaxUnacknowledged: 10,
		TTL:               10 * time.Minute,
	}))
	requests.Add(uint64(100 + len(name)))
	queueDepth.Set(3)

	_ = reg.Register(app)
	_ = reg.RegisterCollectors(requests, queueDepth)
	reg.RegisterEscalations(escalations)

	database.Pass("connected", nil)
	cache.Pass("warm", nil)
	queue.Pass("idle", nil)

	return &leafComponent{
		name:        name,
		group:       group,
		region:      region,
		prefix:      prefix,
		reg:         reg,
		app:         app,
		database:    database,
		cache:       cache,
		queue:       queue,
		requests:    requests,
		queueDepth:  queueDepth,
		escalations: escalations,
	}
}

func (c *leafComponent) mount(mux *http.ServeMux) {
	c.reg.Mount(mux, c.prefix)
	mux.HandleFunc("GET "+c.prefix, c.redirectRoot)
	mux.HandleFunc("GET "+c.prefix+"/{$}", c.handlePage)
	mux.HandleFunc("POST "+c.prefix+"/set", c.handleSet)
	mux.HandleFunc("POST "+c.prefix+"/metric", c.handleMetric)
	mux.HandleFunc("POST "+c.prefix+"/escalate", c.handleEscalate)
}

func (c *leafComponent) redirectRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, c.prefix+"/", http.StatusSeeOther)
}

func (c *leafComponent) handlePage(w http.ResponseWriter, _ *http.Request) {
	c.mu.RLock()
	view := leafView{
		Name:             c.name,
		Group:            c.group,
		Region:           c.region,
		Prefix:           c.prefix,
		Snapshot:         c.app.Snapshot(),
		QueueDepth:       c.queueDepth.Get(),
		LastEscalationID: c.lastEscalationID,
	}
	c.mu.RUnlock()
	if err := pageTemplate.Execute(w, view); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (c *leafComponent) handleEscalate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	title := strings.TrimSpace(r.FormValue("title"))
	if title == "" {
		title = "support escalation from " + c.name
	}
	topic := strings.TrimSpace(r.FormValue("topic"))
	if topic == "" {
		topic = "support"
	}
	severity, err := parseStatus(r.FormValue("severity"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	incident, ok := c.escalations.Start(ctx, statekit.EscalationSpec{
		Title:    title,
		Severity: severity,
		Topics: map[string]any{
			"component": c.name,
			"group":     c.group,
			"region":    c.region,
			"request":   "demo-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		},
	})
	if !ok {
		http.Error(w, "escalation budget exhausted", http.StatusTooManyRequests)
		return
	}
	incident.AddLog(ctx, time.Now(), topic, strings.TrimSpace(r.FormValue("message")), map[string]any{
		"queue_depth": c.queueDepth.Get(),
		"state":       c.app.Snapshot().Status.String(),
	})
	c.mu.Lock()
	c.lastEscalationID = incident.ID()
	c.requests.Inc()
	c.mu.Unlock()
	http.Redirect(w, r, c.prefix+"/", http.StatusSeeOther)
}

func (c *leafComponent) handleSet(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status, err := parseStatus(r.FormValue("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reason := strings.TrimSpace(r.FormValue("reason"))
	if status == statekit.Pass {
		reason = ""
	} else if reason == "" {
		reason = "operator set " + status.String()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch r.FormValue("target") {
	case "database":
		c.database.Set(status, reason, nil)
	case "cache":
		c.cache.Set(status, reason, nil)
	case "queue":
		c.queue.Set(status, reason, nil)
	default:
		http.Error(w, "unknown target", http.StatusBadRequest)
		return
	}
	c.requests.Inc()
	http.Redirect(w, r, c.prefix+"/", http.StatusSeeOther)
}

func (c *leafComponent) handleMetric(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	depth, err := strconv.ParseInt(r.FormValue("queue_depth"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.queueDepth.Set(depth)
	c.requests.Inc()
	c.mu.Unlock()
	http.Redirect(w, r, c.prefix+"/", http.StatusSeeOther)
}

type leafView struct {
	Name             string
	Group            string
	Region           string
	Prefix           string
	Snapshot         statekit.Snapshot
	QueueDepth       int64
	LastEscalationID string
}

type mountedScraper struct {
	name string
	cfg  *scraper.Config
	sc   *scraper.Scraper
	reg  *statekit.Registry
}

func newMountedScraper(name, cfgPath, role string, opts ...scraper.Option) *mountedScraper {
	cfg, err := scraper.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load scraper config %s: %v", cfgPath, err)
	}
	sc, err := scraper.New(*cfg, opts...)
	if err != nil {
		log.Fatalf("new scraper %s: %v", name, err)
	}
	registryLabel := role + "_registry"
	registryRoleLabel := role + "_registry_role"
	reg := statekit.NewRegistry(
		statekit.WithLabel(registryRoleLabel, role),
		statekit.WithLabel(registryLabel, name),
	)
	for _, st := range sc.States() {
		if err := reg.Register(st); err != nil {
			log.Fatalf("register %s state %s: %v", name, st.Name(), err)
		}
	}
	if err := reg.RegisterCollectors(sc.MetricsCollector()); err != nil {
		log.Fatalf("register %s metrics: %v", name, err)
	}
	return &mountedScraper{name: name, cfg: cfg, sc: sc, reg: reg}
}

func (m *mountedScraper) mount(mux *http.ServeMux, prefix string) {
	m.reg.Mount(mux, prefix)
}

func main() {
	listenAddr := flag.String("addr", defaultAddr, "address to listen on")
	killURL := flag.Bool("kill-url", false, "enable GET /-/quit to stop the demo process")
	configDir := flag.String("config-dir", filepath.Join("examples", "stackdemo", "config"), "directory containing scraper config YAML files")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	mux := http.NewServeMux()

	leaves := []*leafComponent{
		newLeaf("checkout-east", "payments", "us-east-1", "/leaf/checkout-east"),
		newLeaf("billing-east", "payments", "us-east-1", "/leaf/billing-east"),
		newLeaf("checkout-west", "payments", "us-west-2", "/leaf/checkout-west"),
		newLeaf("search-west", "discovery", "us-west-2", "/leaf/search-west"),
	}
	for _, leaf := range leaves {
		leaf.mount(mux)
	}

	store := storage.NewMemoryStore(storage.WithDocumentCache(
		storage.NewFreecacheDocumentCache[statekit.StateDisplayDocument](32<<20),
		5*time.Minute,
	))
	east := newMountedScraper("regional-east", filepath.Join(*configDir, "scraper-east.yaml"), "regional", scraper.WithEscalationIngestor(store))
	west := newMountedScraper("regional-west", filepath.Join(*configDir, "scraper-west.yaml"), "regional", scraper.WithEscalationIngestor(store))
	fleet := newMountedScraper("fleet-aggregator", filepath.Join(*configDir, "fleet-aggregator.yaml"), "fleet")
	east.mount(mux, "/scraper/east")
	west.mount(mux, "/scraper/west")
	fleet.mount(mux, "/fleet")

	api := storage.NewAPI(store)
	mux.Handle("/api/", http.StripPrefix("/api", api.Handler()))
	mux.Handle("/storage/", http.StripPrefix("/storage", storage.UIHandler(storage.UIOptions{APIBase: "/api"})))
	if *killURL {
		mux.HandleFunc("GET /-/quit", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("stackdemo shutting down\n"))
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
		})
	}
	mux.HandleFunc("GET /{$}", handleHome)

	server := &http.Server{Addr: *listenAddr, Handler: mux}
	go func() {
		log.Printf("stackdemo listening on http://localhost%s", *listenAddr)
		if *killURL {
			log.Printf("stackdemo kill URL enabled: http://localhost%s/-/quit", *listenAddr)
		}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	go east.sc.Run(ctx)
	go west.sc.Run(ctx)
	go fleet.sc.Run(ctx)
	go ingestFleetState(ctx, store, fleet.reg)

	<-ctx.Done()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = server.Shutdown(shutCtx)
}

func ingestFleetState(ctx context.Context, store storage.Store, reg *statekit.Registry) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_ = store.IngestDocument(ctx, reg.StateDisplay(), time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func handleHome(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(homeHTML))
}

func parseStatus(s string) (statekit.Status, error) {
	return statekit.ParseStatus(strings.ToLower(s))
}

const homeHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>statekit stackdemo</title>
  <style>
    :root { font-family: Inter, ui-sans-serif, system-ui, sans-serif; color: #17202a; background: #f6f7f9; }
    body { margin: 0; }
    main { width: min(1180px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 40px; }
    h1 { margin: 0 0 18px; font-size: 28px; }
    h2 { margin: 22px 0 10px; font-size: 16px; }
    a { color: #1b5fc1; font-weight: 650; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
    .panel { background: #fff; border: 1px solid #d9dee7; border-radius: 8px; padding: 14px; }
    ul { margin: 0; padding-left: 18px; line-height: 1.8; }
    pre { background: #101820; color: #edf2f7; padding: 12px; border-radius: 6px; overflow:auto; }
    form { display: grid; gap: 10px; }
    label { display: grid; gap: 5px; color: #526070; font-size: 13px; font-weight: 650; }
    select, input, button { min-height: 36px; border: 1px solid #c9d1dd; border-radius: 6px; padding: 7px 10px; font: inherit; background: #fff; }
    button { border-color: #1f6feb; background: #1f6feb; color: #fff; font-weight: 720; cursor: pointer; }
    button:disabled { border-color: #c9d1dd; background: #eef1f5; color: #9aa6b5; cursor: default; }
    .buttons { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .opsStatus { margin: 10px 0 0; color: #526070; font-size: 13px; }
    @media (max-width: 820px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <main>
    <h1>statekit stackdemo</h1>
    <div class="grid">
      <section class="panel">
        <h2>Leaf Components</h2>
        <ul>
          <li><a href="/leaf/checkout-east/">checkout-east</a></li>
          <li><a href="/leaf/billing-east/">billing-east</a></li>
          <li><a href="/leaf/checkout-west/">checkout-west</a></li>
          <li><a href="/leaf/search-west/">search-west</a></li>
        </ul>
      </section>
      <section class="panel">
        <h2>Scraper Layers</h2>
        <ul>
          <li><a href="/scraper/east/state">regional east state</a></li>
          <li><a href="/scraper/west/state">regional west state</a></li>
          <li><a href="/fleet/state">fleet state</a></li>
          <li><a href="/fleet/metrics">fleet metrics</a></li>
        </ul>
      </section>
      <section class="panel">
        <h2>Storage API</h2>
        <ul>
          <li><a href="/api/state/current">current states</a></li>
          <li><a href="/api/state/groups?by=group_name">groups by group_name</a></li>
          <li><a href="/api/state/groups?by=label:region">groups by region</a></li>
          <li><a href="/api/state/events?limit=20">recent events</a></li>
          <li><a href="/api/escalations/incidents">all incidents</a></li>
          <li><a href="/api/escalations/incidents?type=deployment">deployment incidents</a></li>
          <li><a href="/api/openapi.yaml">openapi.yaml</a></li>
          <li><a href="/storage/">storage console</a></li>
        </ul>
      </section>
      <section class="panel">
        <h2>Fleet Ops</h2>
        <form id="opsForm">
          <label>Type
            <select id="opsType">
              <option value="deployment">deployment</option>
              <option value="build">build</option>
              <option value="rollback">rollback</option>
            </select>
          </label>
          <label>Title
            <input id="opsTitle" value="deploy v1.2.3 to us-east-1">
          </label>
          <label>Message
            <input id="opsMessage" value="triggered from stackdemo">
          </label>
          <div class="buttons">
            <button type="submit">Start</button>
            <button type="button" id="opsClose" disabled>Close last</button>
          </div>
        </form>
        <p class="opsStatus" id="opsStatus">Markers appear on the storage console timeline.</p>
      </section>
    </div>
    <h2>Config Files</h2>
    <pre>examples/stackdemo/config/scraper-east.yaml
examples/stackdemo/config/scraper-west.yaml
examples/stackdemo/config/fleet-aggregator.yaml</pre>
  </main>
  <script>
    var opsLast = null;
    var opsStatus = document.getElementById("opsStatus");
    var opsClose = document.getElementById("opsClose");

    function postGlobal(body, done) {
      fetch("/api/escalations/global", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }).then(function (res) {
        if (!res.ok) throw new Error(res.status + " " + res.statusText);
        return res.json();
      }).then(done).catch(function (err) {
        opsStatus.textContent = "error: " + err.message;
      });
    }

    document.getElementById("opsForm").addEventListener("submit", function (e) {
      e.preventDefault();
      postGlobal({
        type: document.getElementById("opsType").value,
        title: document.getElementById("opsTitle").value,
        message: document.getElementById("opsMessage").value,
        source: "stackdemo-ops",
      }, function (incident) {
        opsLast = incident;
        opsClose.disabled = false;
        opsStatus.textContent = "started " + incident.type + " " + incident.id;
      });
    });

    opsClose.addEventListener("click", function () {
      if (!opsLast) return;
      postGlobal({
        source: opsLast.source,
        id: opsLast.id,
        status: "closed",
        message: document.getElementById("opsMessage").value,
      }, function (incident) {
        opsStatus.textContent = "closed " + incident.type + " " + incident.id;
        opsLast = null;
        opsClose.disabled = true;
      });
    });
  </script>
</body>
</html>`
