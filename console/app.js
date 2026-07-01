// statekit fleet state console — client logic.
// Pure consumer of the storage JSON API exposed at window.STATEKIT_API_BASE.

const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];
const globalIncidentTypes = new Set(["build", "deployment", "rollback"]);
const NB = 64; // sparkline buckets

const state = {
  view: "fleet", // "fleet" | "overview"
  projectionMode: "server",
  selectedTarget: "",
  selectedIdentity: "",
  incidentFilter: "active",
  windowMs: 60 * 60 * 1000,
  hoverIdx: null,
  updatedAt: "",
  current: [],
  events: [],
  targets: [],
  incidents: [],
  byId: new Map(),
  childrenByParent: new Map(),
  eventsByIdentity: new Map(),
};

const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function statusClass(status) {
  return statusOrder.includes(status) ? status : "pass";
}
function rank(status) {
  const i = statusOrder.indexOf(status);
  return i < 0 ? 0 : i;
}
function pill(status, xs) {
  const s = statusClass(status);
  return `<span class="pill ${xs ? "xs " : ""}${s}"><span class="dot"></span>${s.toUpperCase()}</span>`;
}

async function fetchJSON(path) {
  const res = await fetch(`${apiBase}${path}`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// ---------- data load ----------

async function refresh() {
  const [current, events, serverTargets, incidents] = await Promise.all([
    fetchJSON("/state/current"),
    fetchJSON("/state/events?limit=500"),
    fetchJSON("/state/targets"),
    fetchJSON("/escalations/incidents"),
  ]);
  state.current = current || [];
  state.events = events || [];
  state.incidents = incidents || [];
  state.targets = state.projectionMode === "server"
    ? normalizeServerTargets(serverTargets)
    : buildTargets(state.current);

  indexCurrent();
  indexEvents();

  if (!state.selectedTarget || !state.targets.some((t) => t.key === state.selectedTarget)) {
    state.selectedTarget = state.targets[0]?.key || "";
  }
  ensureSelection();
  state.updatedAt = new Date().toLocaleTimeString();
  render();
}

function indexCurrent() {
  state.byId = new Map();
  state.childrenByParent = new Map();
  state.current.forEach((item) => {
    state.byId.set(item.identity, item);
    const parent = item.parent_identity || "";
    if (!state.childrenByParent.has(parent)) state.childrenByParent.set(parent, []);
    state.childrenByParent.get(parent).push(item);
  });
}

function indexEvents() {
  state.eventsByIdentity = new Map();
  state.events.forEach((event) => {
    const t = Date.parse(event.observed_at || event.changed_at || "");
    if (Number.isNaN(t)) return;
    const list = state.eventsByIdentity.get(event.identity) || [];
    list.push({ t, status: event.status || "pass", reason: event.reason || "" });
    state.eventsByIdentity.set(event.identity, list);
  });
  state.eventsByIdentity.forEach((list) => list.sort((a, b) => a.t - b.t));
}

function normalizeServerTargets(items) {
  return (items || []).map((item) => ({
    key: item.key,
    name: item.name,
    scrapePath: item.scrape_path || item.name,
    labels: item.labels || {},
    states: (item.states || []).map(normalizeServerState),
    counts: countsOf(item.status_counts),
    worstStatus: item.worst_status || "pass",
    observedAt: item.observed_at || "",
  })).map(attachAffected).sort(compareTargets);
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
    observation: {
      status: item.status || "pass",
      reason: item.reason || "",
      changed_secs_ago: secondsSince(item.changed_at),
    },
  };
}

// buildTargets derives targets client-side straight from the flattened current
// states, mirroring the server projection shape.
function buildTargets(items) {
  const byID = new Map(items.map((item) => [item.identity, item]));
  const groups = new Map();
  items.forEach((item) => {
    const name = item.scraped_from || item.labels?.target_id || "";
    if (!name) return;
    const scrapePath = item.scrape_path || name;
    const key = `${name}\n${scrapePath}`;
    const group = groups.get(key) || {
      key, name, scrapePath, labels: {}, states: [], childrenByParent: new Map(),
      counts: { pass: 0, warn: 0, fail: 0, down: 0 }, worstStatus: "pass", observedAt: "",
    };
    groups.set(key, group);

    const parent = byID.get(item.parent_identity);
    if (parent && (parent.scraped_from || "") === name) {
      const kids = group.childrenByParent.get(item.parent_identity) || [];
      kids.push(item);
      group.childrenByParent.set(item.parent_identity, kids);
      return;
    }
    group.states.push(item);
    group.labels = { ...group.labels, ...(item.labels || {}) };
    const status = item.observation?.status || "pass";
    group.counts[status] = (group.counts[status] || 0) + 1;
    if (rank(status) > rank(group.worstStatus)) group.worstStatus = status;
  });

  return [...groups.values()].map((group) => {
    group.states.forEach((s) => { s.checks = group.childrenByParent.get(s.identity) || []; });
    group.states.sort((a, b) => compareStatus(a.observation, b.observation) || a.name.localeCompare(b.name));
    return attachAffected(group);
  }).sort(compareTargets);
}

function countsOf(sc) {
  return { pass: sc?.pass || 0, warn: sc?.warn || 0, fail: sc?.fail || 0, down: sc?.down || 0 };
}
function attachAffected(target) {
  target.affectedStates = target.states.filter((s) => (s.observation?.status || "pass") !== "pass");
  return target;
}
function compareTargets(a, b) {
  return (rank(b.worstStatus) - rank(a.worstStatus)) || a.name.localeCompare(b.name);
}
function compareStatus(a, b) {
  return rank(b?.status) - rank(a?.status);
}

// ---------- selection ----------

function selectedTarget() {
  return state.targets.find((t) => t.key === state.selectedTarget) || state.targets[0] || null;
}

function ensureSelection() {
  const target = selectedTarget();
  if (!target) { state.selectedIdentity = ""; return; }
  const ids = new Set(target.states.map((s) => s.identity));
  if (state.selectedIdentity && ids.has(state.selectedIdentity)) return;
  if (state.selectedIdentity && state.byId.has(state.selectedIdentity)) {
    // keep a deeper node only if it still belongs to this target
    if (targetKeyOf(state.selectedIdentity) === target.key) return;
  }
  const worst = target.states.slice().sort((a, b) => compareStatus(a.observation, b.observation))[0];
  state.selectedIdentity = worst?.identity || "";
}

function targetKeyOf(identity) {
  let node = state.byId.get(identity);
  while (node && node.parent_identity && state.byId.has(node.parent_identity)) {
    node = state.byId.get(node.parent_identity);
  }
  if (!node) return "";
  const name = node.scraped_from || node.labels?.target_id || "";
  return `${name}\n${node.scrape_path || name}`;
}

function selectTarget(key) {
  state.selectedTarget = key;
  state.selectedIdentity = "";
  ensureSelection();
  render();
}
function selectIdentity(identity) {
  state.selectedIdentity = identity;
  render();
}

// ---------- render ----------

function render() {
  renderHeader();
  renderSparkline();
  $("viewFleet").classList.toggle("hidden", state.view !== "fleet");
  $("viewOverview").classList.toggle("hidden", state.view !== "overview");
  $("tabFleet").classList.toggle("active", state.view === "fleet");
  $("tabOverview").classList.toggle("active", state.view === "overview");
  if (state.view === "fleet") renderFleet();
  else renderOverview();
}

function fleetCounts() {
  const counts = { pass: 0, warn: 0, fail: 0, down: 0 };
  state.targets.forEach((t) => { counts[t.worstStatus] = (counts[t.worstStatus] || 0) + 1; });
  const worst = statusOrder.reduce((acc, s) => (counts[s] ? s : acc), "pass");
  return { counts, worst };
}

function renderHeader() {
  const { worst } = fleetCounts();
  const badge = $("statusBadge");
  badge.className = `statusBadge ${statusClass(worst)}`;
  const label = { pass: "OPERATIONAL", warn: "DEGRADED", fail: "CRITICAL", down: "DOWN" }[statusClass(worst)];
  $("statusBadgeText").textContent = label;
  badge.querySelector(".dot").classList.add("pulse-dot");
  updateClock();
}

function updateClock() {
  const z = new Date().toISOString().slice(11, 19);
  const ctx = `${state.targets.length} targets · ${state.projectionMode}`;
  $("clock").innerHTML = `<b>${z}Z</b> · updated <b>${esc(state.updatedAt || "—")}</b> · ${esc(ctx)}`;
}

// ---------- sparkline ----------

// contributors returns one entry per target — the same unit the fleet rail and
// overview counts use. A target is degraded when any node scraped from it is,
// so the chart plots the count of warn/fail *targets* over time (not raw event
// counts, and not leaf states, which would disagree with the counts elsewhere).
// contributors returns one entry per failing unit the chart plots. It keeps the
// leaf states that belong to a target: the real checks (database, cache, .up)
// and each target's own "health" rollup. It drops two kinds of node the same
// failure would otherwise echo through: component-root aggregates (they own
// sub-checks, so they have children) and the top-level fleet health (no target,
// not surfaced anywhere else in the UI).
function contributors() {
  return state.current
    .filter((node) => {
      const kids = state.childrenByParent.get(node.identity);
      const isLeaf = !kids || kids.length === 0;
      const target = node.scraped_from || node.labels?.target_id || "";
      return isLeaf && target !== "";
    })
    .map((node) => ({
      identity: node.identity,
      name: node.name,
      target: node.scraped_from || node.labels?.target_id,
      status: node.observation?.status || "pass",
    }));
}

function statusAt(identity, tAbs) {
  const list = state.eventsByIdentity.get(identity);
  if (!list || !list.length) {
    const node = state.byId.get(identity);
    const changed = Date.parse(node?.observation?.changed_at || "");
    if (!Number.isNaN(changed) && changed <= tAbs) return node?.observation?.status || "pass";
    return "pass";
  }
  let status = "pass";
  for (const e of list) {
    if (e.t <= tAbs) status = e.status;
    else break;
  }
  return status;
}

// worstInInterval returns the worst status a state held at any point during
// [lo, hi]: the status it carried entering the bucket, raised by any transition
// that landed inside it. Sampling the whole interval (rather than a single
// center point) keeps brief transitions from being aliased away between samples.
function worstInInterval(identity, lo, hi) {
  let worst = statusAt(identity, lo);
  const list = state.eventsByIdentity.get(identity);
  if (list) {
    for (const e of list) {
      if (e.t > lo && e.t <= hi && rank(e.status) > rank(worst)) worst = e.status;
    }
  }
  return worst;
}

function buildBuckets(now) {
  const W = state.windowMs;
  const step = W / NB;
  const contrib = contributors();
  const buckets = Array.from({ length: NB }, () => ({ warn: 0, fail: 0, active: [] }));
  for (let i = 0; i < NB; i++) {
    const lo = now - (W - i * step);
    const hi = now - (W - (i + 1) * step);
    contrib.forEach((c) => {
      const st = worstInInterval(c.identity, lo, hi);
      if (st === "pass") return;
      if (st === "fail" || st === "down") buckets[i].fail++;
      else buckets[i].warn++;
      buckets[i].active.push({ target: c.target, name: c.name, st });
    });
  }
  return buckets;
}

function renderSparkline() {
  const now = Date.now();
  const W = state.windowMs;
  const buckets = buildBuckets(now);
  const contrib = contributors();
  const errWarn = contrib.filter((c) => c.status === "warn").length;
  const errFail = contrib.filter((c) => c.status === "fail" || c.status === "down").length;
  const errorMax = Math.max(1, ...buckets.map((b) => b.warn + b.fail));
  const cellH = 100 / errorMax;

  $("sparkSummary").innerHTML =
    `<span class="warn">${errWarn} warn</span> · <span class="fail">${errFail} fail</span> · peak ${errorMax}`;

  $("sparkBars").innerHTML = buckets.map((b, i) => {
    const tot = b.warn + b.fail;
    let cells = "";
    for (let f = 0; f < b.fail; f++) cells += `<div class="sparkCell" style="height:${cellH}%;background:#ff7b72;"></div>`;
    for (let w = 0; w < b.warn; w++) cells += `<div class="sparkCell" style="height:${cellH}%;background:#e3b341;"></div>`;
    const base = `<div class="base" style="background:${tot ? "transparent" : "rgba(255,255,255,.05)"}"></div>`;
    return `<div class="sparkCol" data-idx="${i}">${cells}${base}</div>`;
  }).join("");

  const axis = [];
  for (let i = 0; i < 7; i++) axis.push(hhmm(now - (W - i * (W / 6)) - W + W)); // even ticks across window
  $("sparkAxis").innerHTML = Array.from({ length: 7 }, (_, i) =>
    `<span>${hhmm(now - W + i * (W / 6))}</span>`).join("");

  renderSparkTip(buckets, now);
}

function renderSparkTip(buckets, now) {
  const tip = $("sparkTip");
  const hi = state.hoverIdx;
  if (hi == null || !buckets[hi]) { tip.classList.add("hidden"); return; }
  const bk = buckets[hi];
  const W = state.windowMs;
  const centerAbs = now - (W - (hi + 0.5) * (W / NB));
  const leftPct = (hi + 0.5) / NB * 100;
  const tf = leftPct < 16 ? "translateX(-10px)" : (leftPct > 84 ? "translateX(calc(-100% + 10px))" : "translateX(-50%)");
  const clear = bk.warn + bk.fail === 0;
  const rowsRaw = bk.active.slice().sort((a, b) => rank(b.st) - rank(a.st));
  const rows = rowsRaw.slice(0, 6).map((r) => {
    const color = r.st === "warn" ? "#e3b341" : "#ff7b72";
    const label = r.target && r.target !== r.name ? `<span class="t">${esc(r.target)}:</span>${esc(r.name)}` : esc(r.name);
    return `<div class="rowline"><span class="dot" style="background:${color};box-shadow:0 0 5px ${color};"></span><span class="truncate">${label}</span></div>`;
  }).join("");
  const more = rowsRaw.length > 6 ? `<div class="more">+${rowsRaw.length - 6} more</div>` : "";
  const body = clear
    ? `<div class="clear"><span class="dot"></span>all clear · no active issues</div>`
    : `<div class="counts"><span style="color:#e3b341;">${bk.warn} warn</span><span style="color:#ff7b72;">${bk.fail} fail</span></div>${rows}${more}`;
  tip.style.left = `${leftPct}%`;
  tip.style.transform = tf;
  tip.innerHTML = `<div class="box"><div class="top"><span class="time">${clock(centerAbs)}Z</span><span class="rel">${relFromMs(centerAbs, now)}</span></div>${body}</div>`;
  tip.classList.remove("hidden");
}

// ---------- fleet view ----------

function renderFleet() {
  renderRail();
  renderStates();
  renderDetail();
}

function renderRail() {
  const total = state.targets.length;
  $("targetsRollup").textContent = total ? `${total} targets` : "";
  $("targets").innerHTML = state.targets.map((target) => {
    const sel = target.key === state.selectedTarget;
    const chips = statusOrder.filter((s) => target.counts[s]).map((s) =>
      `<span class="countChip ${s}">${target.counts[s]} ${s === "pass" ? "OK" : s.toUpperCase()}</span>`).join("");
    const issues = target.affectedStates.length
      ? target.affectedStates.map((s) => {
          const st = s.observation?.status || "pass";
          return `<span class="issueChip ${statusClass(st)}" data-identity="${esc(s.identity)}" title="${esc(s.name)}: ${esc(st)}">${esc(s.name)}: ${esc(st)}</span>`;
        }).join("")
      : `<span class="allOk">all states OK</span>`;
    return `<div class="trow ${sel ? "sel " + statusClass(target.worstStatus) : ""}" data-target="${esc(target.key)}">
      <div class="head">
        ${pill(target.worstStatus)}
        <div style="min-width:0;">
          <div class="name truncate">${esc(target.name)}</div>
          <div class="host truncate">${esc(target.scrapePath)}</div>
        </div>
      </div>
      <div class="chips">${chips}</div>
      <div class="chips">${issues}</div>
    </div>`;
  }).join("") || `<div class="empty">No targets</div>`;
}

function renderStates() {
  const target = selectedTarget();
  $("statesTitle").textContent = "States";
  if (!target) {
    $("statesRollup").textContent = "";
    $("states").innerHTML = `<div class="empty">No target selected</div>`;
    return;
  }
  const c = target.counts;
  const total = c.pass + c.warn + c.fail + c.down;
  $("statesRollup").textContent = `${total} · ` +
    [`${c.pass} ok`, c.warn ? `${c.warn} warn` : "", (c.fail + c.down) ? `${c.fail + c.down} fail` : ""].filter(Boolean).join(" · ");

  $("states").innerHTML = target.states.map((s) => stateCard(s)).join("") ||
    `<div class="empty">No states for this target</div>`;
}

function stateCard(s) {
  const obs = s.observation || {};
  const st = statusClass(obs.status);
  const sel = s.identity === state.selectedIdentity;
  const cls = ["scard", st !== "pass" ? st : "", sel ? "sel" : ""].filter(Boolean).join(" ");
  const reason = obs.reason ? `<div class="reason truncate" title="${esc(obs.reason)}">${esc(obs.reason)}</div>` : "";
  const checks = (s.checks || []).slice().sort((a, b) => compareStatus(a.observation, b.observation));
  const checkRows = checks.map((c) => {
    const co = c.observation || {};
    return `<div class="checkRow" data-identity="${esc(c.identity)}">
      ${pill(co.status, true)}
      <div style="min-width:0;">
        <div class="cname">${esc(c.name)}</div>
        ${co.reason ? `<div class="creason">${esc(co.reason)}</div>` : ""}
      </div>
      <span class="dur">${esc(formatAge(co.changed_secs_ago))}</span>
    </div>`;
  }).join("");
  return `<article class="${cls}" data-state="${esc(s.identity)}">
    <div class="top">${pill(obs.status, true)}<span class="sname">${esc(s.name)}</span></div>
    ${reason}
    <div class="meta">changed ${esc(formatAge(obs.changed_secs_ago))} ago<span class="sep"> · </span>observed ${esc(formatTime(obs.observed_at))}</div>
    ${checkRows ? `<div class="checks">${checkRows}</div>` : ""}
  </article>`;
}

function renderDetail() {
  const node = state.byId.get(state.selectedIdentity);
  const crumbEl = $("detailCrumbs");
  const bodyEl = $("detail");
  if (!node) {
    crumbEl.innerHTML = "";
    bodyEl.innerHTML = `<div class="empty">Select a state to inspect</div>`;
    return;
  }

  // breadcrumb: target › ...ancestors › node
  const chain = [];
  let n = node;
  while (n) {
    chain.unshift(n);
    n = n.parent_identity ? state.byId.get(n.parent_identity) : null;
  }
  const target = selectedTarget();
  const crumbs = [];
  if (target && (!chain.length || chain[0].name !== target.name)) {
    crumbs.push({ label: target.name, target: target.key });
  }
  chain.forEach((c) => crumbs.push({ label: c.name, identity: c.identity }));
  crumbEl.innerHTML = crumbs.map((c, i) => {
    const last = i === crumbs.length - 1;
    const attr = c.target ? `data-target="${esc(c.target)}"` : `data-identity="${esc(c.identity)}"`;
    return `<span class="crumb ${last ? "last" : ""}" ${last ? "" : attr}>${esc(c.label)}</span>` +
      (last ? "" : `<span class="crumbSep">/</span>`);
  }).join("");

  const obs = node.observation || {};
  const st = statusClass(obs.status);
  const info = (node.importance || "").toLowerCase() === "informational";
  const children = (state.childrenByParent.get(node.identity) || []).slice()
    .sort((a, b) => compareStatus(a.observation, b.observation));
  const history = (state.eventsByIdentity.get(node.identity) || []).slice().reverse();

  const parts = [];
  parts.push(`<div class="detailTitle">${pill(obs.status)}<h2>${esc(node.name)}</h2>${info ? `<span class="infoBadge">INFORMATIONAL</span>` : ""}</div>`);
  parts.push(`<div class="detailMeta">
    <span>updated <b>${esc(formatAge(secsAgo(obs.updated_secs_ago, obs.observed_at)))} ago</b></span>
    <span>changed <b>${esc(formatAge(obs.changed_secs_ago))} ago</b></span>
  </div>`);
  if (node.help) parts.push(`<div class="detailHelp">${esc(node.help)}</div>`);
  if (obs.reason) {
    const rc = st !== "pass" ? st : "neutral";
    parts.push(`<div class="detailReason ${rc}">${esc(obs.reason)}</div>`);
  }

  const dataRows = flattenData(obs.data || {});
  if (dataRows.length) {
    parts.push(`<div class="detailSection">
      <div class="subKicker" style="margin-bottom:9px;">DATA</div>
      <div class="dataBlock">${dataRows.map(dataRowHTML).join("")}</div>
    </div>`);
  }

  if (children.length) {
    const cc = { pass: 0, warn: 0, fail: 0, down: 0 };
    children.forEach((c) => { cc[c.observation?.status || "pass"]++; });
    const roll = `${children.length} checks · ` +
      [`${cc.pass} pass`, cc.warn ? `${cc.warn} warn` : "", (cc.fail + cc.down) ? `${cc.fail + cc.down} fail` : ""].filter(Boolean).join(", ");
    parts.push(`<div class="detailSection">
      <div class="head"><div class="subKicker">SUB-CHECKS</div><div class="metaMono">${esc(roll)}</div></div>
      <div class="childList">${children.map(childRowHTML).join("")}</div>
    </div>`);
  }

  if (history.length) {
    parts.push(`<div class="detailSection">
      <div class="subKicker" style="margin-bottom:11px;">STATE HISTORY <span style="color:#3a4452;">· transition timeline</span></div>
      <div>${history.map(historyRowHTML).join("")}</div>
    </div>`);
  }

  bodyEl.innerHTML = parts.join("");
}

function childRowHTML(c) {
  const co = c.observation || {};
  const st = statusClass(co.status);
  const kids = (state.childrenByParent.get(c.identity) || []).length;
  const chevron = kids > 0 ? `${kids} ›` : "›";
  return `<div class="childRow ${st !== "pass" ? st : ""}" data-identity="${esc(c.identity)}">
    <div style="min-width:0;flex:1;">
      <div class="top">${pill(co.status, true)}<span class="cname">${esc(c.name)}</span></div>
      ${co.reason ? `<div class="creason">${esc(co.reason)}</div>` : ""}
    </div>
    <span class="chev">${esc(chevron)}</span>
  </div>`;
}

function historyRowHTML(e) {
  const st = statusClass(e.status);
  return `<div class="histRow">
    <div class="histTime"><div class="rel">${esc(relFromMs(e.t, Date.now()))}</div><div class="abs">${clock(e.t)}Z</div></div>
    <div class="histRail"><span class="dot ${st}"></span></div>
    <div class="histText">${pill(e.status, true)}${e.reason ? `<div class="histReason">${esc(e.reason)}</div>` : ""}</div>
  </div>`;
}

// flattenData renders a nested data object into indented key/value rows.
function flattenData(obj) {
  const rows = [];
  const walk = (k, v, ind) => {
    if (Array.isArray(v)) {
      rows.push({ k, v: `[${v.length}]`, ind, header: true });
      v.forEach((it, i) => {
        if (it && typeof it === "object") {
          rows.push({ k: `– ${i}`, v: "", ind: ind + 1, header: true });
          Object.entries(it).forEach(([k2, v2]) => walk(k2, v2, ind + 2));
        } else rows.push({ k: `– ${i}`, v: String(it), ind: ind + 1 });
      });
    } else if (v && typeof v === "object") {
      rows.push({ k, v: "", ind, header: true });
      Object.entries(v).forEach(([k2, v2]) => walk(k2, v2, ind + 1));
    } else rows.push({ k, v: String(v), ind });
  };
  Object.entries(obj).forEach(([k, v]) => walk(k, v, 0));
  return rows;
}

function dataRowHTML(r) {
  const hasVal = !r.header && r.v !== "";
  const color = /MiB|GiB|KiB/.test(r.v) ? "#79c0ff" : (r.v === "true" ? "#56d364" : (r.v === "false" ? "#ff7b72" : "#c4cedb"));
  return `<div class="dataRow" style="padding-left:${r.ind * 15}px;">
    <span class="dataKey ${r.header ? "header" : ""}">${esc(r.k)}</span>
    ${hasVal ? `<span class="dataVal" style="color:${color};">${esc(r.v)}</span>` : ""}
  </div>`;
}

// ---------- overview view ----------

function renderOverview() {
  renderCounts();
  renderIncidents();
}

function renderCounts() {
  const { counts, worst } = fleetCounts();
  const total = state.targets.length;
  const degraded = counts.warn + counts.fail + counts.down;
  const worstLabel = { pass: "OK", warn: "WARN", fail: "FAIL", down: "DOWN" }[statusClass(worst)];
  const worstColor = { pass: "#56d364", warn: "#e3b341", fail: "#ff7b72", down: "#ff7b72" }[statusClass(worst)];
  const sub = degraded ? `${degraded} of ${total} targets degraded` : `all ${total} targets healthy`;

  const segs = statusOrder.filter((s) => counts[s]).map((s, i, arr) => {
    const color = { pass: "#3fb950", warn: "#e3b341", fail: "#ff7b72", down: "#ff7b72" }[s];
    return `<div style="flex:${counts[s]};background:${color};${i < arr.length - 1 ? "border-right:1px solid #0e121a;" : ""}"></div>`;
  }).join("");

  $("counts").innerHTML = `
    <div class="worst">
      <div class="kicker">WORST STATE</div>
      <div class="worstLine"><span class="worstLabel" style="color:${worstColor};">${worstLabel}</span><span class="worstSub">${esc(sub)}</span></div>
      <div class="distBar">${segs}</div>
    </div>
    ${countCell("TARGETS", total, "neutral")}
    ${countCell("OK", counts.pass, "pass")}
    ${countCell("WARN", counts.warn, "warn")}
    ${countCell("FAIL / DOWN", counts.fail + counts.down, counts.fail + counts.down ? "fail" : "zero")}
  `;
}

function countCell(label, value, cls) {
  return `<div class="countCell"><div class="kicker">${esc(label)}</div><div class="n ${cls}">${value}</div></div>`;
}

function isGlobalIncident(inc) {
  return globalIncidentTypes.has(inc.type || "");
}

function filteredIncidents() {
  const filter = state.incidentFilter;
  return state.incidents.filter((inc) => {
    if (filter === "active") return inc.status !== "closed";
    if (filter === "global") return isGlobalIncident(inc);
    if (!filter) return true;
    return inc.status === filter;
  });
}

function renderIncidents() {
  const list = filteredIncidents();
  $("incidentsCount").textContent = `${list.length} ${state.incidentFilter || "total"}`;
  const el = $("incidents");
  if (!list.length) {
    el.innerHTML = `<div class="incidentEmpty">
      <div class="icon">✓</div>
      <div><div class="t">No ${esc(state.incidentFilter || "")} incidents</div>
      <div class="s">${state.incidents.length} total escalations tracked · switch the filter to review history</div></div>
    </div>`;
    return;
  }
  const rows = list.map((inc) => {
    const sev = severityClass(inc);
    const latest = (inc.events || [])[(inc.events || []).length - 1];
    const reason = latest ? [latest.topic, latest.message].filter(Boolean).join(" · ") : (inc.type || "");
    const ack = inc.status === "acknowledged"
      ? `<span class="metaMono">acked</span>`
      : `<button type="button" class="ackBtn" data-source="${esc(inc.source)}" data-id="${esc(inc.id)}">Ack</button>`;
    return `<div class="incRow">
      <span class="sev ${sev.cls}">${esc(sev.label)}</span>
      <div style="min-width:0;"><div class="title" title="${esc(inc.title)}">${esc(inc.title)}</div><div class="id">${esc(inc.id)}</div></div>
      <span class="cell dim" title="${esc(inc.source)}">${esc(inc.source)}</span>
      <span class="cell" title="${esc(reason)}">${esc(reason)}</span>
      <span class="cell dim">${esc(formatTime(inc.created_at))}</span>
      <span class="cell dim">${esc(formatAge(secondsSince(inc.last_updated_at)))} ago</span>
      <span class="incStatus">${esc(inc.status)}</span>
      ${ack}
    </div>`;
  }).join("");
  el.innerHTML = `<div class="incTable">
    <div class="incHead"><span>SEVERITY</span><span>TITLE</span><span>SOURCE</span><span>REASON</span><span>STARTED</span><span>UPDATED</span><span>STATUS</span><span></span></div>
    ${rows}
  </div>`;
}

function severityClass(inc) {
  const sev = (inc.severity || "").toLowerCase();
  if (["fail", "down", "high"].includes(sev)) return { cls: "high", label: sev || "HIGH" };
  if (["warn", "med", "medium"].includes(sev)) return { cls: "med", label: sev || "MED" };
  if (["pass", "low", "info", "informational"].includes(sev)) return { cls: "low", label: sev || "LOW" };
  return { cls: "med", label: sev || (inc.type || "incident") };
}

async function acknowledgeIncident(source, id) {
  const params = new URLSearchParams({ source, id });
  const res = await fetch(`${apiBase}/escalations/ack?${params}`, { method: "POST" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  await refresh();
}

// ---------- time helpers ----------

function pad(n) { return String(n).padStart(2, "0"); }
function clock(ms) { const d = new Date(ms); return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`; }
function hhmm(ms) { const d = new Date(ms); return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`; }
function relFromMs(ms, now) {
  const secs = Math.max(0, Math.floor((now - ms) / 1000));
  return formatAge(secs) + " ago";
}
function formatTime(value) {
  if (!value) return "";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleTimeString();
}
function formatAge(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${Math.max(0, Math.floor(seconds))}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d`;
}
function secondsSince(value) {
  if (!value) return NaN;
  const t = Date.parse(value);
  return Number.isNaN(t) ? NaN : Math.max(0, Math.floor((Date.now() - t) / 1000));
}
function secsAgo(explicit, fallbackTime) {
  if (Number.isFinite(Number(explicit)) && Number(explicit) > 0) return Number(explicit);
  return secondsSince(fallbackTime);
}

// ---------- events / wiring ----------

function showError(err) {
  const bar = $("errbar");
  bar.textContent = `Failed to load: ${err.message}`;
  bar.classList.remove("hidden");
}
function clearError() { $("errbar").classList.add("hidden"); }

$("tabFleet").addEventListener("click", () => { state.view = "fleet"; render(); });
$("tabOverview").addEventListener("click", () => { state.view = "overview"; render(); });
$("refresh").addEventListener("click", () => refresh().then(clearError).catch(showError));
$("projectionMode").addEventListener("change", (e) => { state.projectionMode = e.target.value; refresh().then(clearError).catch(showError); });
$("incidentFilter").addEventListener("change", (e) => { state.incidentFilter = e.target.value; render(); });
$("timelineWindow").addEventListener("change", (e) => { state.windowMs = Number(e.target.value) || 60 * 60 * 1000; renderSparkline(); });

$("targets").addEventListener("click", (e) => {
  const issue = e.target.closest(".issueChip");
  if (issue) { e.stopPropagation(); selectIdentity(issue.dataset.identity); return; }
  const row = e.target.closest("[data-target]");
  if (row) selectTarget(row.dataset.target);
});
$("states").addEventListener("click", (e) => {
  const check = e.target.closest(".checkRow");
  if (check) { e.stopPropagation(); selectIdentity(check.dataset.identity); return; }
  const card = e.target.closest("[data-state]");
  if (card) selectIdentity(card.dataset.state);
});
$("detailCrumbs").addEventListener("click", (e) => {
  const t = e.target.closest("[data-target]");
  if (t) { selectTarget(t.dataset.target); return; }
  const id = e.target.closest("[data-identity]");
  if (id) selectIdentity(id.dataset.identity);
});
$("detail").addEventListener("click", (e) => {
  const child = e.target.closest("[data-identity]");
  if (child) selectIdentity(child.dataset.identity);
});
$("incidents").addEventListener("click", (e) => {
  const btn = e.target.closest(".ackBtn");
  if (btn) acknowledgeIncident(btn.dataset.source, btn.dataset.id).then(clearError).catch(showError);
});

const sparkBars = $("sparkBars");
sparkBars.addEventListener("mouseover", (e) => {
  const col = e.target.closest(".sparkCol");
  if (!col) return;
  const idx = Number(col.dataset.idx);
  if (idx !== state.hoverIdx) { state.hoverIdx = idx; renderSparkline(); }
});
sparkBars.addEventListener("mouseleave", () => { state.hoverIdx = null; renderSparkline(); });

setInterval(updateClock, 1000);
refresh().then(clearError).catch(showError);
setInterval(() => refresh().then(clearError).catch(showError), 5000);
