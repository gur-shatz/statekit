const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];

const state = {
  status: "",
  selectedTarget: "",
  current: [],
  events: [],
  targets: [],
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
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function refresh() {
  $("lastRefresh").textContent = "Refreshing...";
  const [current, events] = await Promise.all([
    fetchJSON("/state/current"),
    fetchJSON("/state/events?limit=100"),
  ]);
  state.current = current;
  state.events = events;
  state.targets = buildTargets(current);
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
  renderStates();
  renderEvents();
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
      state.selectedTarget = row.dataset.target;
      render();
    });
  });
}

function affectedChip(item) {
  const status = item.observation?.status || "pass";
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
  const checks = (target.checksByParent.get(item.identity) || []).slice().sort(compareStates);
  const reason = obs.reason ? `<div class="reason truncate">${esc(obs.reason)}</div>` : "";
  const labels = stateLabels(item);
  return `<article class="stateCard">
    <div class="stateCardTop">
      <span class="pill ${esc(obs.status)}">${esc(obs.status)}</span>
      <div class="stateCardTitle">
        <div class="stateName truncate">${esc(item.name)}</div>
        <div class="subtle truncate">${esc(item.group_name || "")}</div>
      </div>
    </div>
    ${reason}
    ${labels ? `<div class="labels stateLabels">${labels}</div>` : ""}
    <div class="cardMeta">
      <span>changed ${esc(formatAge(obs.changed_secs_ago))} ago</span>
      <span>observed ${esc(formatTime(obs.observed_at))}</span>
    </div>
    ${checks.length ? `<div class="checks">${checks.map(checkRow).join("")}</div>` : ""}
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

function checkRow(item) {
  const obs = item.observation || {};
  return `<div class="checkRow">
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
    ...[...target.checksByParent.values()].flat().map((item) => item.identity),
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

function showError(err) {
  $("lastRefresh").textContent = err.message;
}

$("statusFilter").addEventListener("change", (e) => {
  state.status = e.target.value;
  render();
});
$("refresh").addEventListener("click", () => refresh().catch(showError));

refresh().catch(showError);
setInterval(() => refresh().catch(showError), 5000);
