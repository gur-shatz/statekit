const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];
const timelineNowMarkerID = "now-marker";

const state = {
  status: "",
  projectionMode: "server",
  selectedTarget: "",
  current: [],
  events: [],
  targets: [],
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
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function refresh() {
  $("lastRefresh").textContent = "Refreshing...";
  const [current, events, serverTargets] = await Promise.all([
    fetchJSON("/state/current"),
    fetchJSON("/state/events?limit=500"),
    fetchJSON("/state/targets"),
  ]);
  state.current = current;
  state.events = events;
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
  if (!intervals.length) {
    if (state.systemTimeline) {
      state.systemTimeline.destroy();
      state.systemTimeline = null;
    }
    container.innerHTML = `<div class="empty">No recent issue transitions</div>`;
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

function showError(err) {
  $("lastRefresh").textContent = err.message;
}

$("statusFilter").addEventListener("change", (e) => {
  state.status = e.target.value;
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

refresh().catch(showError);
setInterval(() => refresh().catch(showError), 5000);
