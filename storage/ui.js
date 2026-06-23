const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];
const timelineNowMarkerID = "now-marker";
const globalIncidentTypes = new Set(["build", "deployment", "rollback"]);

const state = {
  status: "",
  projectionMode: "server",
  selectedTarget: "",
  current: [],
  events: [],
  targets: [],
  incidents: [],
  incidentFilter: "active",
  timelineWindowMs: 60 * 60 * 1000,
  systemTimeline: null,
};

const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function fetchJSON(path) {
  const res = await fetch(`${apiBase}${path}`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function refresh() {
  $("lastRefresh").textContent = "Refreshing...";
  const [current, events, serverTargets, incidents] = await Promise.all([
    fetchJSON("/state/current"),
    fetchJSON("/state/events?limit=500"),
    fetchJSON("/state/targets"),
    fetchJSON("/escalations/incidents"),
  ]);
  state.current = current;
  state.events = events;
  state.incidents = incidents || [];
  state.targets = state.projectionMode === "server" ? normalizeServerTargets(serverTargets) : buildTargets(current);
  if (!state.selectedTarget && state.targets.length) state.selectedTarget = state.targets[0].key;
  if (state.selectedTarget && !state.targets.some((t) => t.key === state.selectedTarget)) {
    state.selectedTarget = state.targets[0]?.key || "";
  }
  render();
}

function buildTargets(items) {
  const byID = new Map(items.map((item) => [item.identity, item]));
  const groups = new Map();

  items.forEach((item) => {
    const target = targetFor(item);
    if (!target.name) return;
    const group = groups.get(target.key) || {
      key: target.key,
      name: target.name,
      scrapePath: target.scrapePath,
      labels: {},
      states: [],
      checksByParent: new Map(),
      counts: { pass: 0, warn: 0, fail: 0, down: 0 },
      worstStatus: "pass",
      observedAt: "",
    };
    groups.set(target.key, group);

    const parent = byID.get(item.parent_identity);
    if (parent && targetFor(parent).key === target.key) {
      const checks = group.checksByParent.get(item.parent_identity) || [];
      checks.push(item);
      group.checksByParent.set(item.parent_identity, checks);
      return;
    }

    group.states.push(item);
    group.labels = { ...group.labels, ...(item.labels || {}) };
    const status = item.observation?.status || "pass";
    group.counts[status] = (group.counts[status] || 0) + 1;
    if (rank(status) > rank(group.worstStatus)) group.worstStatus = status;
    if (!group.observedAt || Date.parse(item.observation?.observed_at || 0) > Date.parse(group.observedAt || 0)) {
      group.observedAt = item.observation?.observed_at || "";
    }
  });

  return [...groups.values()]
    .map((target) => {
      target.states.sort(compareStates);
      target.affectedStates = target.states.filter((item) => item.observation?.status !== "pass");
      return target;
    })
    .sort((a, b) => {
      const byStatus = rank(b.worstStatus) - rank(a.worstStatus);
      if (byStatus) return byStatus;
      return a.name.localeCompare(b.name);
    });
}

function normalizeServerTargets(items) {
  return (items || []).map((item) => ({
    key: item.key,
    name: item.name,
    scrapePath: item.scrape_path || item.name,
    labels: item.labels || {},
    states: (item.states || []).map(normalizeServerState),
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

function normalizeServerState(item) {
  return {
    identity: item.identity,
    name: item.name,
    group_name: item.group_name || "",
    labels: item.labels || {},
    observation: {
      status: item.status || "pass",
      reason: item.reason || "",
      changed_at: item.changed_at || "",
      changed_secs_ago: secondsSince(item.changed_at),
      observed_at: item.observed_at || "",
      data: item.data || {},
    },
    checks: (item.checks || []).map(normalizeServerCheck),
  };
}

function normalizeServerCheck(item) {
  return {
    identity: item.identity,
    name: item.name,
    labels: item.labels || {},
    observation: {
      status: item.status || "pass",
      reason: item.reason || "",
      changed_at: item.changed_at || "",
      changed_secs_ago: secondsSince(item.changed_at),
      observed_at: item.observed_at || "",
    },
  };
}

function targetFor(item) {
  const name = item.scraped_from || item.labels?.target_id || "";
  const scrapePath = item.scrape_path || name;
  return { name, scrapePath, key: `${name}\n${scrapePath}` };
}

function rank(status) {
  return statusOrder.indexOf(status);
}

function compareStates(a, b) {
  const byStatus = rank(b.observation?.status) - rank(a.observation?.status);
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
  renderSystemTimeline();
  renderIncidents();
  renderStates();
  renderEvents();
  $("lastRefresh").textContent = `Updated ${new Date().toLocaleTimeString()} from ${state.projectionMode}`;
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
      state.selectedTarget = row.dataset.target;
      render();
    });
  });
}

function affectedChip(item) {
  const status = item.observation?.status || item.status || "pass";
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
  const rows = target.states;
  $("states").innerHTML = rows.map((item) => stateCard(target, item)).join("") || `<div class="empty">No states for this target</div>`;
}

function stateCard(target, item) {
  const obs = item.observation || {};
  const checks = checksForState(target, item).slice().sort(compareStates);
  const reason = obs.reason ? `<div class="reason truncate">${esc(obs.reason)}</div>` : "";
  const labels = stateLabels(item);
  const data = stateData(item);
  return `<article class="stateCard clickable" data-identity="${esc(item.identity)}">
    <div class="stateCardTop">
      <span class="pill ${esc(obs.status)}">${esc(obs.status)}</span>
      <div class="stateCardTitle">
        <div class="stateName truncate">${esc(item.name)}</div>
        <div class="subtle truncate">${esc(item.group_name || "")}</div>
      </div>
    </div>
    ${reason}
    ${labels ? `<div class="labels stateLabels">${labels}</div>` : ""}
    ${data ? `<div class="dataBlock"><div class="dataTitle">data</div><div class="dataGrid">${data}</div></div>` : ""}
    <div class="cardMeta">
      <span>changed ${esc(formatAge(obs.changed_secs_ago))} ago</span>
      <span>observed ${esc(formatTime(obs.observed_at))}</span>
    </div>
    ${checks.length ? `<div class="checks">${checks.map(checkRow).join("")}</div>` : ""}
  </article>`;
}

function checksForState(target, item) {
  if (item.checks) return item.checks;
  return target.checksByParent?.get(item.identity) || [];
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

function stateData(item) {
  const data = item.observation?.data || {};
  return Object.entries(data)
    .filter(([k, v]) => k !== "labels" && v !== null && v !== undefined && v !== "")
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => {
      const value = formatDataValue(v);
      return `<div class="dataPair"><span class="dataKey truncate">${esc(k)}</span><span class="dataVal truncate" title="${esc(value)}">${esc(value)}</span></div>`;
    })
    .join("");
}

function formatDataValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function checkRow(item) {
  const obs = item.observation || {};
  return `<div class="checkRow clickable" data-identity="${esc(item.identity)}">
    <div><span class="pill ${esc(obs.status)}">${esc(obs.status)}</span></div>
    <div class="truncate">${esc(item.name)}</div>
    <div class="truncate">${esc(obs.reason || "")}</div>
    <div>${esc(formatAge(obs.changed_secs_ago))}</div>
  </div>`;
}

function renderEvents() {
  const target = selectedTarget();
  $("timelineTitle").textContent = target ? `Timeline: ${target.name}` : "Timeline";
  if (!target) {
    $("events").innerHTML = `<div class="empty">No target selected</div>`;
    return;
  }
  const ids = new Set([
    ...target.states.map((item) => item.identity),
    ...target.states.flatMap((item) => checksForState(target, item)).map((item) => item.identity),
  ]);
  const byIdentity = new Map(state.current.map((item) => [item.identity, item]));
  const rows = state.events
    .filter((event) => ids.has(event.identity))
    .slice()
    .reverse()
    .map((event) => {
      const item = byIdentity.get(event.identity);
      const name = item?.name || event.identity.slice(0, 10);
      return `<div class="row">
        <div class="truncate">${formatTime(event.observed_at)}</div>
        <div><span class="pill ${esc(event.status)}">${esc(event.status)}</span></div>
        <div class="truncate">${esc(name)}</div>
        <div class="truncate">${esc(event.reason || "")}</div>
      </div>`;
    }).join("");
  $("events").innerHTML = `<div class="row header">
    <div>Observed</div><div>Status</div><div>State</div><div>Reason</div>
  </div>${rows || `<div class="empty">No events for this target</div>`}`;
}

function renderSystemTimeline() {
  const container = $("systemTimelineViz");
  if (!container) return;
  const now = new Date();
  const rangeStart = new Date(now.getTime() - state.timelineWindowMs);
  const intervals = buildTargetIntervals()
    .filter((interval) => intervalOverlapsWindow(interval, rangeStart, now));
  if (!window.vis?.Timeline || !window.vis?.DataSet) {
    container.innerHTML = `<div class="empty">Timeline library unavailable</div>`;
    return;
  }
  const items = intervals.map((interval, idx) => ({
    id: `system-${interval.targetKey}-${idx}`,
    content: esc(`${interval.status} ${interval.targetName}`),
    title: esc([interval.targetName, interval.status, interval.reasons.join("; ")].filter(Boolean).join(": ")),
    start: interval.start,
    end: interval.end || new Date(),
    type: "range",
    className: `timeline-${interval.status}`,
  }));
  items.push(...incidentTimelineItems(rangeStart, now));
  if (!items.length) {
    if (state.systemTimeline) {
      state.systemTimeline.destroy();
      state.systemTimeline = null;
    }
    container.innerHTML = `<div class="empty">No recent issue transitions or incidents</div>`;
    return;
  }

  const options = {
    stack: true,
    stackSubgroups: true,
    selectable: true,
    zoomMin: 1000,
    zoomMax: 1000 * 60 * 60 * 24 * 14,
    margin: { item: { horizontal: 8, vertical: 8 }, axis: 8 },
    orientation: "top",
    start: rangeStart,
    end: now,
  };

  if (!state.systemTimeline) {
    container.innerHTML = "";
    state.systemTimeline = new window.vis.Timeline(
      container,
      new window.vis.DataSet(items),
      options,
    );
  } else {
    state.systemTimeline.setItems(new window.vis.DataSet(items));
    state.systemTimeline.setOptions(options);
  }
  setNowMarker(state.systemTimeline, now);
  state.systemTimeline.setWindow(rangeStart, now, { animation: false });
}

function setNowMarker(timeline, now) {
  try {
    timeline.setCustomTime(now, timelineNowMarkerID);
  } catch {
    timeline.addCustomTime(now, timelineNowMarkerID);
  }
  if (typeof timeline.setCustomTimeTitle === "function") {
    timeline.setCustomTimeTitle("now", timelineNowMarkerID);
  }
}

function intervalOverlapsWindow(interval, rangeStart, rangeEnd) {
  const start = Date.parse(interval.start);
  const end = interval.end ? Date.parse(interval.end) : rangeEnd.getTime();
  if (Number.isNaN(start) || Number.isNaN(end)) return false;
  return start <= rangeEnd.getTime() && end >= rangeStart.getTime();
}

function buildTargetIntervals() {
  const byIdentity = new Map(state.current.map((item) => [item.identity, item]));
  const targetStates = new Map(state.targets.map((target) => [target.key, {
    target,
    open: null,
    intervals: [],
    statusByIdentity: new Map(),
  }]));

  state.events
    .slice()
    .sort((a, b) => Date.parse(a.observed_at || a.changed_at || 0) - Date.parse(b.observed_at || b.changed_at || 0))
    .forEach((event) => {
      const current = byIdentity.get(event.identity);
      if (!current) return;
      const targetKey = targetFor(current).key;
      const bucket = targetStates.get(targetKey);
      if (!bucket) return;
      const status = event.status || "pass";
      const when = event.observed_at || event.changed_at;
      if (!when) return;

      if (status === "pass") {
        bucket.statusByIdentity.delete(event.identity);
      } else {
        bucket.statusByIdentity.set(event.identity, {
          status,
          reason: event.reason || current.observation?.reason || current.name,
        });
      }

      const worstStatus = worstStatusFor(bucket.statusByIdentity);
      if (worstStatus === "pass") {
        closeInterval(bucket, when);
        return;
      }

      const reasons = reasonsFor(bucket.statusByIdentity);
      if (!bucket.open) {
        bucket.open = {
          targetKey,
          targetName: bucket.target.name,
          status: worstStatus,
          reasons,
          start: when,
          end: "",
        };
        return;
      }
      if (bucket.open.status !== worstStatus) {
        closeInterval(bucket, when);
        bucket.open = {
          targetKey,
          targetName: bucket.target.name,
          status: worstStatus,
          reasons,
          start: when,
          end: "",
        };
        return;
      }
      bucket.open.reasons = reasons;
    });

  state.current.forEach((item) => {
    const status = item.observation?.status || "pass";
    if (status === "pass") return;
    const targetKey = targetFor(item).key;
    const bucket = targetStates.get(targetKey);
    if (!bucket || bucket.statusByIdentity.has(item.identity)) return;

    bucket.statusByIdentity.set(item.identity, {
      status,
      reason: item.observation?.reason || item.name,
    });
    const start = item.observation?.changed_at || item.observation?.observed_at;
    if (!start) return;
    if (!bucket.open) {
      bucket.open = {
        targetKey,
        targetName: bucket.target.name,
        status,
        reasons: reasonsFor(bucket.statusByIdentity),
        start,
        end: "",
      };
      return;
    }
    if (Date.parse(start) < Date.parse(bucket.open.start)) {
      bucket.open.start = start;
    }
    bucket.open.status = worstStatusFor(bucket.statusByIdentity);
    bucket.open.reasons = reasonsFor(bucket.statusByIdentity);
  });

  const intervals = [];
  targetStates.forEach((bucket) => {
    if (bucket.open) intervals.push(bucket.open);
    intervals.push(...bucket.intervals);
  });
  return intervals;
}

function closeInterval(bucket, end) {
  if (!bucket.open) return;
  bucket.open.end = end;
  bucket.intervals.push(bucket.open);
  bucket.open = null;
}

function worstStatusFor(statuses) {
  let worst = "pass";
  statuses.forEach(({ status }) => {
    if (rank(status) > rank(worst)) worst = status;
  });
  return worst;
}

function reasonsFor(statuses) {
  return [...statuses.values()]
    .filter(({ reason }) => reason)
    .map(({ status, reason }) => `${status}: ${reason}`)
    .slice(0, 4);
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

function incidentSpan(incident, now) {
  const start = incident.created_at;
  const end = incident.status === "closed" ? (incident.last_updated_at || incident.created_at) : now;
  return { start, end };
}

function latestIncidentEvent(incident) {
  const events = incident.events || [];
  return events.length ? events[events.length - 1] : null;
}

function incidentTooltip(incident, span) {
  const typeName = incidentTypeName(incident);
  const latest = latestIncidentEvent(incident);
  const lines = [
    `${typeName}: ${incident.title}`,
    `Status: ${incident.status || "unknown"}`,
    `Source: ${incident.source || "unknown"}`,
    `Started: ${formatDateTime(span.start)}`,
    `Updated: ${formatDateTime(incident.last_updated_at)}`,
  ];
  if (latest) {
    lines.push(`Latest event: ${[latest.topic, latest.message].filter(Boolean).join(" - ")}`);
  }
  return lines.filter(Boolean).join("\n");
}

function incidentTimelineItems(rangeStart, now) {
  return state.incidents
    .map((incident) => ({ incident, span: incidentSpan(incident, now) }))
    .filter(({ span }) => intervalOverlapsWindow(span, rangeStart, now))
    .map(({ incident, span }) => {
      const typeName = incidentTypeName(incident);
      const global = isGlobalIncident(incident);
      const label = `${typeName}: ${incident.title}`;
      return {
        id: `incident-${incident.identity}`,
        content: esc(label),
        title: esc(incidentTooltip(incident, span)),
        start: span.start,
        end: span.end,
        type: global ? "background" : "range",
        className: global
          ? `timeline-global timeline-global-${cssToken(typeName)}`
          : `timeline-incident timeline-${cssToken(incident.severity || "warn")}`,
      };
    });
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

function formatDateTime(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
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

function currentRecord(identity) {
  return state.current.find((item) => item.identity === identity) || null;
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
  const record = currentRecord(identity);
  $("stateDrawerTitle").textContent = record?.name || identity;
  $("stateDrawerSub").textContent = record?.identity || identity;
  if (record) {
    const { observation, ...metadata } = record;
    $("stateDrawerObservation").textContent = observation ? toYAML(observation) : "# no observation recorded";
    $("stateDrawerMetadata").textContent = toYAML(metadata);
    drawerYaml = toYAML(record);
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
$("projectionMode").addEventListener("change", (e) => {
  state.projectionMode = e.target.value;
  refresh().catch(showError);
});
$("timelineWindow").addEventListener("change", (e) => {
  state.timelineWindowMs = Number(e.target.value) || 60 * 60 * 1000;
  renderSystemTimeline();
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
