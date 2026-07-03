// statekit storage UI — pure consumer of the storage layer API.
// The refresh tick polls L1 (/state/targets) and the charting store
// (/state/timeline); the selected target's states come from its L2 document
// (/state/targets/{key}, revalidated by its material_hash ETag); the
// per-target transition table merges the L3 timelines of that target's
// states.

const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];
const globalIncidentTypes = new Set(["build", "deployment", "rollback"]);
const chartBucketCount = 96;

const state = {
  status: "",
  selectedTarget: "",
  targets: [],
  incidents: [],
  chart: [], // BucketCounts[] from /state/timeline
  detail: null, // TargetDetail of the selected target
  detailStates: [], // top-level StateDetails with .checks attached
  byId: new Map(), // identity -> StateDetail, selected target only
  transitions: [], // merged L3 transitions of the selected target's states
  incidentFilter: "active",
  timelineWindowMs: 60 * 60 * 1000,
};

const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

// Conditional GET cache: every layer endpoint sets a strong ETag, so the
// steady-state poll is one small L1 body plus 304s.
const etagCache = new Map(); // path -> {etag, data}

async function fetchJSON(path) {
  const cached = etagCache.get(path);
  const headers = { Accept: "application/json" };
  if (cached?.etag) headers["If-None-Match"] = cached.etag;
  const res = await fetch(`${apiBase}${path}`, { headers });
  if (res.status === 304 && cached) return cached.data;
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const data = await res.json();
  const etag = res.headers.get("ETag");
  if (etag) etagCache.set(path, { etag, data });
  return data;
}

function chartPath() {
  return `/state/timeline?scope=fleet&window=${Math.round(state.timelineWindowMs / 1000)}s&buckets=${chartBucketCount}`;
}

async function refresh() {
  $("lastRefresh").textContent = "Refreshing...";
  const [targets, incidents, chart] = await Promise.all([
    fetchJSON("/state/targets"),
    fetchJSON("/escalations/incidents"),
    fetchJSON(chartPath()),
  ]);
  state.targets = normalizeTargets(targets);
  state.incidents = incidents || [];
  state.chart = chart || [];
  if (!state.selectedTarget || !state.targets.some((t) => t.key === state.selectedTarget)) {
    state.selectedTarget = state.targets[0]?.key || "";
  }
  await loadSelectedTarget();
  render();
}

function normalizeTargets(items) {
  return (items || []).map((item) => ({
    key: item.key,
    name: item.name,
    scrapePath: item.scrape_path || item.name,
    labels: item.labels || {},
    counts: {
      pass: item.status_counts?.pass || 0,
      warn: item.status_counts?.warn || 0,
      fail: item.status_counts?.fail || 0,
      down: item.status_counts?.down || 0,
    },
    worstStatus: item.worst_status || "pass",
    observedAt: item.observed_at || "",
    affectedStates: item.affected_states || [],
    materialHash: item.material_hash || "",
  })).sort((a, b) => {
    const byStatus = rank(b.worstStatus) - rank(a.worstStatus);
    if (byStatus) return byStatus;
    return a.name.localeCompare(b.name);
  });
}

// loadSelectedTarget fetches the selected target's L2 detail (revalidated by
// its material_hash ETag) and the L3 timelines of its states.
async function loadSelectedTarget() {
  if (!state.selectedTarget) {
    state.detail = null;
    state.detailStates = [];
    state.byId = new Map();
    state.transitions = [];
    return;
  }
  const detail = await fetchJSON(`/state/targets/${encodeURIComponent(state.selectedTarget)}`);
  state.detail = detail;
  const details = detail.details || [];
  state.byId = new Map(details.map((d) => [d.identity, d]));
  const checksByParent = new Map();
  const roots = [];
  details.forEach((d) => {
    if (d.parent_identity && state.byId.has(d.parent_identity)) {
      const list = checksByParent.get(d.parent_identity) || [];
      list.push(d);
      checksByParent.set(d.parent_identity, list);
    } else {
      roots.push(d);
    }
  });
  roots.forEach((d) => { d.checks = (checksByParent.get(d.identity) || []).slice().sort(compareStates); });
  state.detailStates = roots.sort(compareStates);

  const timelines = await Promise.all(details.map((d) =>
    fetchJSON(`/state/states/${encodeURIComponent(d.identity)}/timeline`).catch(() => null)));
  state.transitions = timelines
    .filter(Boolean)
    .flatMap((timeline) => (timeline.transitions || []).map((transition) => ({
      ...transition,
      identity: timeline.identity,
      name: state.byId.get(timeline.identity)?.name || timeline.identity.slice(0, 10),
    })))
    .sort((a, b) => Date.parse(b.changed_at || 0) - Date.parse(a.changed_at || 0));
}

function rank(status) {
  return statusOrder.indexOf(status);
}

function compareStates(a, b) {
  const byStatus = rank(b.status) - rank(a.status);
  if (byStatus) return byStatus;
  return (a.name || "").localeCompare(b.name || "");
}

function filteredTargets() {
  if (!state.status) return state.targets;
  if (state.status === "issues") return state.targets.filter((t) => t.worstStatus !== "pass");
  return state.targets.filter((t) => t.worstStatus === state.status);
}

function selectedTarget() {
  return state.targets.find((t) => t.key === state.selectedTarget) || state.targets[0] || null;
}

function render() {
  renderTotals();
  renderTargets();
  renderFleetChart();
  renderIncidents();
  renderStates();
  renderTransitions();
  $("lastRefresh").textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

function renderTotals() {
  const counts = { pass: 0, warn: 0, fail: 0, down: 0 };
  state.targets.forEach((target) => {
    counts[target.worstStatus] = (counts[target.worstStatus] || 0) + 1;
  });
  const worst = statusOrder.reduce((acc, s) => counts[s] ? s : acc, "pass");
  $("totals").innerHTML = [
    metric("Worst", `<span class="pill ${worst}">${worst}</span>`),
    metric("Targets", state.targets.length),
    metric("OK", counts.pass || 0),
    metric("Warn", counts.warn || 0),
    metric("Fail/Down", (counts.fail || 0) + (counts.down || 0)),
  ].join("");
}

function metric(label, value) {
  return `<div class="metric"><span>${esc(label)}</span><strong>${value}</strong></div>`;
}

function renderTargets() {
  const rows = filteredTargets().map((target) => {
    const selected = target.key === state.selectedTarget;
    const affected = target.affectedStates.length
      ? target.affectedStates.map((item) => affectedChip(item)).join("")
      : `<span class="subtle">all states OK</span>`;
    return `<div class="row clickable ${selected ? "selected" : ""}" data-target="${esc(target.key)}">
      <div><span class="pill ${esc(target.worstStatus)}">${esc(target.worstStatus)}</span></div>
      <div class="truncate">
        <div class="stateName">${esc(target.name)}</div>
        <div class="subtle truncate">${esc(target.scrapePath)}</div>
      </div>
      <div>
        <div class="chips">${summaryCounts(target)}</div>
        <div class="chips issueList">${affected}</div>
      </div>
    </div>`;
  }).join("");
  $("targets").innerHTML = `<div class="row header">
    <div>Status</div><div>Target</div><div>States</div>
  </div>${rows || `<div class="empty">No targets</div>`}`;
  $("targets").querySelectorAll("[data-target]").forEach((row) => {
    row.addEventListener("click", () => {
      selectTarget(row.dataset.target);
    });
  });
}

function selectTarget(key) {
  state.selectedTarget = key;
  loadSelectedTarget().then(render).catch(showError);
}

function affectedChip(item) {
  const status = item.status || "pass";
  return `<span class="chip ${esc(status)} truncate">${esc(item.name)}: ${esc(status)}</span>`;
}

function summaryCounts(target) {
  const labels = { pass: "ok", warn: "warn", fail: "fail", down: "down" };
  return statusOrder
    .filter((status) => target.counts[status])
    .map((status) => `<span class="countChip ${status}">${esc(target.counts[status])} ${labels[status]}</span>`)
    .join("");
}

function renderStates() {
  const target = selectedTarget();
  $("statesTitle").textContent = target ? `States: ${target.name}` : "States";
  if (!target) {
    $("states").innerHTML = `<div class="empty">No target selected</div>`;
    return;
  }
  $("states").innerHTML = state.detailStates.map((item) => stateCard(target, item)).join("") ||
    `<div class="empty">No states for this target</div>`;
}

function stateCard(target, item) {
  const reason = item.reason ? `<div class="reason truncate">${esc(item.reason)}</div>` : "";
  const labels = stateLabels(item);
  const data = stateData(item);
  return `<article class="stateCard clickable" data-identity="${esc(item.identity)}">
    <div class="stateCardTop">
      <span class="pill ${esc(item.status)}">${esc(item.status)}</span>
      <div class="stateCardTitle">
        <div class="stateName truncate">${esc(item.name)}</div>
        <div class="subtle truncate">${esc(item.group_name || "")}</div>
      </div>
    </div>
    ${reason}
    ${labels ? `<div class="labels stateLabels">${labels}</div>` : ""}
    ${data ? `<div class="dataBlock"><div class="dataTitle">data</div><pre class="dataYaml">${highlightYAML(data)}</pre></div>` : ""}
    <div class="cardMeta">
      <span>changed ${esc(formatAge(secondsSince(item.changed_at)))} ago</span>
      <span>observed ${esc(formatTime(item.observed_at))}</span>
    </div>
    ${item.checks?.length ? `<div class="checks">${item.checks.map(checkRow).join("")}</div>` : ""}
  </article>`;
}

function stateLabels(item) {
  const hidden = new Set([
    "fleet_registry",
    "fleet_registry_role",
    "regional_registry",
    "regional_registry_role",
    "scraper",
    "target_id",
  ]);
  return Object.entries(item.labels || {})
    .filter(([k, v]) => v !== "" && !hidden.has(k))
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `<span class="label truncate">${esc(k)}=${esc(v)}</span>`)
    .join("");
}

// stateData renders the state's data payload as YAML, so arbitrarily nested
// values display without flattening. Labels ride in their own chip row.
function stateData(item) {
  const entries = Object.entries(item.data || {})
    .filter(([k, v]) => k !== "labels" && v !== null && v !== undefined && v !== "")
    .sort(([a], [b]) => a.localeCompare(b));
  if (!entries.length) return "";
  return toYAML(Object.fromEntries(entries));
}

function checkRow(item) {
  return `<div class="checkRow clickable" data-identity="${esc(item.identity)}">
    <div><span class="pill ${esc(item.status)}">${esc(item.status)}</span></div>
    <div class="truncate">${esc(item.name)}</div>
    <div class="truncate">${esc(item.reason || "")}</div>
    <div>${esc(formatAge(secondsSince(item.changed_at)))}</div>
  </div>`;
}

function renderTransitions() {
  const target = selectedTarget();
  $("timelineTitle").textContent = target ? `Timeline: ${target.name}` : "Timeline";
  if (!target) {
    $("events").innerHTML = `<div class="empty">No target selected</div>`;
    return;
  }
  const rows = state.transitions.map((transition) => `<div class="row">
      <div class="truncate">${formatTime(transition.changed_at)}</div>
      <div><span class="pill ${esc(transition.status)}">${esc(transition.status)}</span></div>
      <div class="truncate">${esc(transition.name)}</div>
      <div class="truncate">${esc(transition.reason || "")}</div>
    </div>`).join("");
  $("events").innerHTML = `<div class="row header">
    <div>Changed</div><div>Status</div><div>State</div><div>Reason</div>
  </div>${rows || `<div class="empty">No transitions for this target</div>`}`;
}

// renderFleetChart draws the charting store's bucketed triggering-state
// counts as stacked bars: one lookup, no client-side event replay.
function renderFleetChart() {
  const container = $("fleetChart");
  const buckets = state.chart.map((b) => ({
    t: b.t || "",
    warn: b.counts?.warn || 0,
    fail: (b.counts?.fail || 0) + (b.counts?.down || 0),
  }));
  const max = Math.max(1, ...buckets.map((b) => b.warn + b.fail));
  const bars = buckets.map((b) => {
    const failH = (b.fail / max) * 100;
    const warnH = (b.warn / max) * 100;
    const title = `${formatTime(b.t)} · ${b.warn} warn · ${b.fail} fail`;
    return `<div class="chartCol" title="${esc(title)}">
      <div class="chartCell warn" style="height:${warnH}%;"></div>
      <div class="chartCell fail" style="height:${failH}%;"></div>
    </div>`;
  }).join("");
  const axis = [buckets[0]?.t, buckets[Math.floor(buckets.length / 2)]?.t, new Date().toISOString()]
    .map((t) => `<span>${esc(formatTime(t))}</span>`).join("");
  container.innerHTML = `<div class="chartBars">${bars}</div><div class="chartAxis">${axis}</div>`;
}

function isGlobalIncident(incident) {
  return globalIncidentTypes.has(incident.type || "");
}

function incidentTypeName(incident) {
  return incident.type || "incident";
}

function cssToken(value) {
  return String(value || "").replace(/[^a-z0-9_-]/gi, "");
}

function filteredIncidents() {
  const filter = state.incidentFilter;
  return state.incidents.filter((incident) => {
    if (filter === "active") return incident.status !== "closed";
    if (filter === "global") return isGlobalIncident(incident);
    if (!filter) return true;
    return incident.status === filter;
  });
}

function renderIncidents() {
  const rows = filteredIncidents().map((incident) => {
    const typeName = incidentTypeName(incident);
    const severity = incident.severity || "";
    const ack = incident.status === "acknowledged"
      ? `<span class="subtle">acked</span>`
      : `<button type="button" class="ackButton" data-source="${esc(incident.source)}" data-id="${esc(incident.id)}">Ack</button>`;
    return `<div class="row">
      <div><span class="typeChip type-${cssToken(typeName)}">${esc(typeName)}</span></div>
      <div>${severity ? `<span class="pill ${cssToken(severity)}">${esc(severity)}</span>` : ""}</div>
      <div><span class="incidentStatus status-${cssToken(incident.status)}">${esc(incident.status)}</span></div>
      <div class="truncate" title="${esc(incident.title)}">
        <div class="stateName truncate">${esc(incident.title)}</div>
        <div class="subtle truncate">${esc(incident.id)}</div>
      </div>
      <div class="truncate subtle">${esc(incident.source)}</div>
      <div class="subtle">${esc(formatTime(incident.created_at))}</div>
      <div class="subtle">${esc(formatAge(secondsSince(incident.last_updated_at)))} ago</div>
      <div>${ack}</div>
    </div>`;
  }).join("");
  $("incidentsTitle").textContent = `Incidents (${filteredIncidents().length})`;
  $("incidents").innerHTML = `<div class="row header">
    <div>Type</div><div>Severity</div><div>Status</div><div>Title</div><div>Source</div><div>Started</div><div>Updated</div><div></div>
  </div>${rows || `<div class="empty">No incidents</div>`}`;
  $("incidents").querySelectorAll(".ackButton").forEach((button) => {
    button.addEventListener("click", () => {
      acknowledgeIncident(button.dataset.source, button.dataset.id).catch(showError);
    });
  });
}

async function acknowledgeIncident(source, id) {
  const params = new URLSearchParams({ source, id });
  const res = await fetch(`${apiBase}/escalations/ack?${params}`, { method: "POST" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  await refresh();
}

function formatTime(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleTimeString();
}

function formatAge(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "";
  if (seconds < 60) return `${Math.max(0, Math.floor(seconds))}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function secondsSince(value) {
  if (!value) return "";
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) return "";
  return Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
}

function yamlScalar(value) {
  if (value === null || value === undefined) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : JSON.stringify(String(value));
  const str = String(value);
  if (str === "") return '""';
  if (/[\n"]/.test(str)) return JSON.stringify(str);
  if (/^[\s]|[\s]$|^[-?:,[\]{}#&*!|>'"%@`]|[:#]\s|^(?:true|false|null|yes|no|on|off|~)$|^[-+]?\d/i.test(str)) {
    return JSON.stringify(str);
  }
  return str;
}

function yamlKey(key) {
  return /^[A-Za-z0-9_.-]+$/.test(key) ? key : JSON.stringify(key);
}

function isEmptyContainer(value) {
  return Array.isArray(value) ? value.length === 0 : Object.keys(value).length === 0;
}

// highlightYAML tokenizes toYAML output into colored spans. It only needs to
// understand the YAML this UI generates: one node per line, plain or
// JSON-quoted keys, scalar values.
function highlightYAML(yaml) {
  return yaml.split("\n").map((line) => {
    const m = line.match(/^(\s*)((?:- )*)((?:"(?:[^"\\]|\\.)*"|[A-Za-z0-9_.-]+):)?( ?)(.*)$/);
    if (!m) return esc(line);
    const [, indent, dashes, key, space, rest] = m;
    return esc(indent)
      + (dashes ? `<span class="yDash">${esc(dashes)}</span>` : "")
      + (key ? `<span class="yKey">${esc(key)}</span>` : "")
      + space
      + yamlValueHTML(rest);
  }).join("\n");
}

function yamlValueHTML(value) {
  if (value === "") return "";
  const cls =
    /^-?\d+(\.\d+)?$/.test(value) ? "yNum"
    : (value === "true" || value === "false") ? "yBool"
    : (value === "null" || value === "[]" || value === "{}") ? "yNull"
    : "yStr";
  return `<span class="${cls}">${esc(value)}</span>`;
}

// toYAML renders a plain JSON value as a YAML document fragment indented under
// the given level. Strings that could be misread as YAML are quoted.
function toYAML(value, indent = 0) {
  const pad = "  ".repeat(indent);
  if (value === null || typeof value !== "object") return yamlScalar(value);

  if (Array.isArray(value)) {
    if (!value.length) return "[]";
    return value.map((item) => {
      if (item !== null && typeof item === "object" && !isEmptyContainer(item)) {
        const lines = toYAML(item, indent + 1).split("\n");
        const head = lines[0].slice((indent + 1) * 2);
        const rest = lines.slice(1).join("\n");
        return `${pad}- ${head}${rest ? `\n${rest}` : ""}`;
      }
      return `${pad}- ${toYAML(item, 0)}`;
    }).join("\n");
  }

  const keys = Object.keys(value);
  if (!keys.length) return "{}";
  return keys.map((key) => {
    const child = value[key];
    if (child !== null && typeof child === "object" && !isEmptyContainer(child)) {
      return `${pad}${yamlKey(key)}:\n${toYAML(child, indent + 1)}`;
    }
    if (child !== null && typeof child === "object") {
      return `${pad}${yamlKey(key)}: ${Array.isArray(child) ? "[]" : "{}"}`;
    }
    return `${pad}${yamlKey(key)}: ${yamlScalar(child)}`;
  }).join("\n");
}

let drawerYaml = "";

function openStateDrawer(identity) {
  const record = state.byId.get(identity) || null;
  $("stateDrawerTitle").textContent = record?.name || identity;
  $("stateDrawerSub").textContent = record?.identity || identity;
  if (record) {
    const {
      status, reason, changed_at, updated_at, observed_at, data, data_hash, checks,
      ...metadata
    } = record;
    const observation = { status, reason, changed_at, updated_at, observed_at, data, data_hash };
    $("stateDrawerObservation").innerHTML = highlightYAML(toYAML(observation));
    $("stateDrawerMetadata").innerHTML = highlightYAML(toYAML(metadata));
    drawerYaml = toYAML({ ...observation, ...metadata });
  } else {
    $("stateDrawerObservation").textContent = `# state ${identity} not found`;
    $("stateDrawerMetadata").textContent = "";
    drawerYaml = "";
  }
  $("stateDrawerMeta").open = false;
  $("stateDrawerCopy").textContent = "Copy";
  const drawer = $("stateDrawer");
  drawer.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
}

function closeStateDrawer() {
  const drawer = $("stateDrawer");
  drawer.classList.remove("open");
  drawer.setAttribute("aria-hidden", "true");
}

async function copyStateYaml() {
  if (!navigator.clipboard || !drawerYaml) return;
  await navigator.clipboard.writeText(drawerYaml);
  $("stateDrawerCopy").textContent = "Copied";
}

function showError(err) {
  $("lastRefresh").textContent = err.message;
}

$("statusFilter").addEventListener("change", (e) => {
  state.status = e.target.value;
  render();
});
$("incidentFilter").addEventListener("change", (e) => {
  state.incidentFilter = e.target.value;
  render();
});
$("timelineWindow").addEventListener("change", (e) => {
  state.timelineWindowMs = Number(e.target.value) || 60 * 60 * 1000;
  refresh().catch(showError);
});
$("refresh").addEventListener("click", () => refresh().catch(showError));

$("states").addEventListener("click", (e) => {
  const target = e.target.closest("[data-identity]");
  if (target) openStateDrawer(target.dataset.identity);
});
$("stateDrawerClose").addEventListener("click", closeStateDrawer);
$("stateDrawerBackdrop").addEventListener("click", closeStateDrawer);
$("stateDrawerCopy").addEventListener("click", () => copyStateYaml().catch(showError));
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeStateDrawer();
});

refresh().catch(showError);
setInterval(() => refresh().catch(showError), 5000);
