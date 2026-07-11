// statekit fleet state console — client logic.
// Pure consumer of the storage layer API exposed at window.STATEKIT_API_BASE:
// the 5s tick polls L1 (/state/targets) and the charting store
// (/state/timeline); selecting a target fetches its L2 document
// (/state/targets/{key}, revalidated by its material_hash ETag); the detail
// pane fetches the L3 transition ring per identity; sparkline hovers fetch
// per-bucket contributors lazily.

const apiBase = window.STATEKIT_API_BASE || "/api";
const statusOrder = ["pass", "warn", "fail", "down"];
const globalIncidentTypes = new Set(["build", "deployment", "rollback"]);
const NB = 64; // sparkline buckets

const state = {
  view: "fleet", // "fleet" | "overview"
  selectedTarget: "",
  selectedIdentity: "",
  incidentFilter: "active",
  windowMs: 60 * 60 * 1000,
  hoverIdx: null,
  updatedAt: "",
  targets: [],
  incidents: [],
  chart: [], // BucketCounts[] from /state/timeline
  detail: null, // TargetDetail of the selected target
  byId: new Map(), // identity -> StateDetail, selected target only
  childrenByParent: new Map(),
  history: [], // transitions of the selected identity, newest first
  bucketTips: new Map(), // bucket time -> contributors from /state/timeline/bucket
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

// ---------- data load ----------

function chartPath() {
  return `/state/timeline?scope=fleet&window=${Math.round(state.windowMs / 1000)}s&buckets=${NB}`;
}

async function refresh() {
  const [targets, incidents, chart] = await Promise.all([
    fetchJSON("/state/targets"),
    fetchJSON("/escalations/incidents"),
    fetchJSON(chartPath()),
  ]);
  state.targets = normalizeTargets(targets);
  state.incidents = incidents || [];
  state.chart = chart || [];
  state.bucketTips = new Map();

  if (!state.selectedTarget || !state.targets.some((t) => t.key === state.selectedTarget)) {
    state.selectedTarget = state.targets[0]?.key || "";
    state.selectedIdentity = "";
  }
  await loadSelection();
  state.updatedAt = new Date().toLocaleTimeString();
  render();
}

// normalizeTargets shapes L1 TargetSummary rows for rendering: the flat
// StateHeaders become top-level states with their checks attached, and each
// affected state is resolved back to its header identity.
function normalizeTargets(items) {
  return (items || []).map((item) => {
    const headers = item.states || [];
    const inTarget = new Set(headers.map((h) => h.identity));
    const checksByParent = new Map();
    const roots = [];
    headers.forEach((h) => {
      if (h.parent_identity && inTarget.has(h.parent_identity)) {
        const list = checksByParent.get(h.parent_identity) || [];
        list.push(h);
        checksByParent.set(h.parent_identity, list);
      } else {
        roots.push(h);
      }
    });
    const states = roots.map((h) => ({ ...h, checks: checksByParent.get(h.identity) || [] }));
    return {
      key: item.key,
      name: item.name,
      scrapePath: item.scrape_path || item.name,
      labels: item.labels || {},
      counts: countsOf(item.status_counts),
      worstStatus: item.worst_status || "pass",
      observedAt: item.observed_at || "",
      states,
      allStates: headers,
      affectedStates: (item.affected_states || []).map((a) => ({
        ...a,
        identity: roots.find((r) => r.name === a.name)?.identity || "",
      })),
    };
  }).sort(compareTargets);
}

// loadSelection fetches the selected target's L2 detail and the selected
// identity's L3 timeline. The detail request carries the last material_hash
// as If-None-Match, so an unchanged target costs a 304.
async function loadSelection() {
  const target = selectedTarget();
  if (!target) {
    state.detail = null;
    state.byId = new Map();
    state.childrenByParent = new Map();
    state.history = [];
    state.selectedIdentity = "";
    return;
  }
  const detail = await fetchJSON(`/state/targets/${encodeURIComponent(target.key)}`);
  state.detail = detail;
  state.byId = new Map((detail.details || []).map((d) => [d.identity, d]));
  state.childrenByParent = new Map();
  (detail.details || []).forEach((d) => {
    const parent = d.parent_identity || "";
    if (!state.childrenByParent.has(parent)) state.childrenByParent.set(parent, []);
    state.childrenByParent.get(parent).push(d);
  });
  ensureSelection();
  await loadHistory();
}

async function loadHistory() {
  if (!state.selectedIdentity) {
    state.history = [];
    return;
  }
  const timeline = await fetchJSON(`/state/states/${encodeURIComponent(state.selectedIdentity)}/timeline`);
  state.history = timeline?.transitions || [];
}

function countsOf(sc) {
  return { pass: sc?.pass || 0, warn: sc?.warn || 0, fail: sc?.fail || 0, down: sc?.down || 0 };
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
  if (state.selectedIdentity && state.byId.has(state.selectedIdentity)) return;
  const worst = target.states.slice().sort(compareStatus)[0];
  state.selectedIdentity = worst?.identity || "";
}

function selectTarget(key, identity) {
  state.selectedTarget = key;
  state.selectedIdentity = identity || "";
  loadSelection().then(render).then(clearError).catch(showError);
}
function selectIdentity(identity) {
  state.selectedIdentity = identity;
  loadHistory().then(render).then(clearError).catch(showError);
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
  $("clock").innerHTML = `<b>${z}Z</b> · updated <b>${esc(state.updatedAt || "—")}</b> · ${state.targets.length} targets`;
}

// ---------- sparkline ----------

// The chart plots the charting store's bucketed triggering-state counts
// directly; fail and down are stacked together as "fail".
function chartBuckets() {
  return state.chart.map((b) => ({
    t: Date.parse(b.t || ""),
    warn: b.counts?.warn || 0,
    fail: (b.counts?.fail || 0) + (b.counts?.down || 0),
  }));
}

function renderSparkline() {
  const now = Date.now();
  const W = state.windowMs;
  const buckets = chartBuckets();
  let errWarn = 0;
  let errFail = 0;
  state.targets.forEach((t) => t.allStates.forEach((h) => {
    if (h.status === "warn") errWarn++;
    else if (h.status === "fail" || h.status === "down") errFail++;
  }));
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

  $("sparkAxis").innerHTML = Array.from({ length: 7 }, (_, i) =>
    `<span>${hhmm(now - W + i * (W / 6))}</span>`).join("");

  renderSparkTip(buckets, now);
}

// loadBucketTip lazily fetches the triggering states behind one hovered
// bucket from the charting store's bucket endpoint.
async function loadBucketTip(idx) {
  const bucket = state.chart[idx];
  if (!bucket?.t) return [];
  if (state.bucketTips.has(bucket.t)) return state.bucketTips.get(bucket.t);
  const center = Date.parse(bucket.t) + state.windowMs / NB / 2;
  const list = await fetchJSON(`/state/timeline/bucket?scope=fleet&t=${encodeURIComponent(new Date(center).toISOString())}`);
  state.bucketTips.set(bucket.t, list || []);
  return list || [];
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

  const contributors = state.bucketTips.get(state.chart[hi]?.t);
  if (!clear && contributors === undefined) {
    loadBucketTip(hi).then(() => {
      if (state.hoverIdx === hi) renderSparkline();
    }).catch(() => {});
  }
  const rowsRaw = (contributors || []).slice().sort((a, b) => rank(b.status) - rank(a.status));
  const rows = rowsRaw.slice(0, 6).map((r) => {
    const color = r.status === "warn" ? "#e3b341" : "#ff7b72";
    return `<div class="rowline"><span class="dot" style="background:${color};box-shadow:0 0 5px ${color};"></span><span class="truncate">${esc(r.label)}</span></div>`;
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
          const st = statusClass(s.status);
          return `<span class="issueChip ${st}" data-identity="${esc(s.identity)}" title="${esc(s.name)}: ${esc(st)}">${esc(s.name)}: ${esc(st)}</span>`;
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

  $("states").innerHTML = target.states.map((s) => stateCard(target, s)).join("") ||
    `<div class="empty">No states for this target</div>`;
}

function stateCard(target, s) {
  const st = statusClass(s.status);
  const sel = s.identity === state.selectedIdentity;
  const cls = ["scard", st !== "pass" ? st : "", sel ? "sel" : ""].filter(Boolean).join(" ");
  const reason = s.reason ? `<div class="reason truncate" title="${esc(s.reason)}">${esc(s.reason)}</div>` : "";
  const checks = (s.checks || []).slice().sort(compareStatus);
  const checkRows = checks.map((c) => `<div class="checkRow" data-identity="${esc(c.identity)}">
      ${pill(c.status, true)}
      <div style="min-width:0;">
        <div class="cname">${esc(c.name)}</div>
        ${c.reason ? `<div class="creason">${esc(c.reason)}</div>` : ""}
      </div>
      <span class="dur">${esc(formatAge(secondsSince(c.changed_at)))}</span>
    </div>`).join("");
  return `<article class="${cls}" data-state="${esc(s.identity)}">
    <div class="top">${pill(s.status, true)}<span class="sname">${esc(s.name)}</span></div>
    ${reason}
    <div class="meta">changed ${esc(formatAge(secondsSince(s.changed_at)))} ago<span class="sep"> · </span>observed ${esc(formatTime(target.observedAt))}</div>
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

  const st = statusClass(node.status);
  const info = (node.importance || "").toLowerCase() === "informational";
  const children = (state.childrenByParent.get(node.identity) || []).slice().sort(compareStatus);
  const history = state.history;

  const parts = [];
  parts.push(`<div class="detailTitle">${pill(node.status)}<h2>${esc(node.name)}</h2>${info ? `<span class="infoBadge">INFORMATIONAL</span>` : ""}</div>`);
  parts.push(muteControlsHTML(node));
  parts.push(`<div class="detailMeta">
    <span>updated <b>${esc(formatAge(secondsSince(node.updated_at || node.observed_at)))} ago</b></span>
    <span>changed <b>${esc(formatAge(secondsSince(node.changed_at)))} ago</b></span>
  </div>`);
  if (node.help) parts.push(`<div class="detailHelp">${esc(node.help)}</div>`);
  if (node.reason) {
    const rc = st !== "pass" ? st : "neutral";
    parts.push(`<div class="detailReason ${rc}">${esc(node.reason)}</div>`);
  }

  const labels = stateLabels(node);
  if (labels.length) {
    parts.push(`<div class="detailSection">
      <div class="subKicker" style="margin-bottom:9px;">LABELS</div>
      <div class="detailLabels">${labels.map(([k, v]) => `<span class="labelChip">${esc(k)}=${esc(v)}</span>`).join("")}</div>
    </div>`);
  }

  const yaml = stateDataYAML(node);
  if (yaml) {
    parts.push(`<div class="detailSection">
      <div class="subKicker" style="margin-bottom:9px;">DATA</div>
      <div class="dataBlock"><pre class="dataYaml">${highlightYAML(yaml)}</pre></div>
    </div>`);
  }

  if (children.length) {
    const cc = { pass: 0, warn: 0, fail: 0, down: 0 };
    children.forEach((c) => { cc[statusClass(c.status)]++; });
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

function muteControlsHTML(node) {
  const active = node.mute;
  const original = node.original_status ? `<span class="metaMono">from ${esc(node.original_status.toUpperCase())}</span>` : "";
  const expires = active?.expires_at ? `<span class="metaMono">until ${esc(formatTime(active.expires_at))}</span>` : "";
  return `<div class="muteBar" data-mute-identity="${esc(node.identity)}">
    <div class="muteStatus">${active ? `MUTED ${original} ${expires}` : "MUTE"}</div>
    <select class="select sm muteStatusSelect" aria-label="Muted status">
      ${statusOrder.map((s) => `<option value="${s}" ${s === "pass" ? "selected" : ""}>${s.toUpperCase()}</option>`).join("")}
    </select>
    <select class="select sm muteDurationSelect" aria-label="Mute duration">
      <option value="30m">30m</option>
      <option value="1h" selected>1h</option>
      <option value="4h">4h</option>
      <option value="24h">24h</option>
      <option value="7d">7d</option>
      <option value="30d">30d</option>
      <option value="365d">365d</option>
    </select>
    <button type="button" class="muteApply">Apply</button>
    ${active ? `<button type="button" class="muteClear">Clear</button>` : ""}
  </div>`;
}

function childRowHTML(c) {
  const st = statusClass(c.status);
  const kids = (state.childrenByParent.get(c.identity) || []).length;
  const chevron = kids > 0 ? `${kids} ›` : "›";
  return `<div class="childRow ${st !== "pass" ? st : ""}" data-identity="${esc(c.identity)}">
    <div style="min-width:0;flex:1;">
      <div class="top">${pill(c.status, true)}<span class="cname">${esc(c.name)}</span></div>
      ${c.reason ? `<div class="creason">${esc(c.reason)}</div>` : ""}
    </div>
    <span class="chev">${esc(chevron)}</span>
  </div>`;
}

function historyRowHTML(transition) {
  const t = Date.parse(transition.changed_at || "");
  return `<div class="histRow">
    <div class="histTime"><div class="rel">${esc(relFromMs(t, Date.now()))}</div><div class="abs">${clock(t)}Z</div></div>
    <div class="histRail"><span class="dot ${statusClass(transition.status)}"></span></div>
    <div class="histText">${pill(transition.status, true)}${transition.reason ? `<div class="histReason">${esc(transition.reason)}</div>` : ""}</div>
  </div>`;
}

// hiddenLabels are fleet plumbing, not operator-facing tags.
const hiddenLabels = new Set([
  "fleet_registry",
  "fleet_registry_role",
  "regional_registry",
  "regional_registry_role",
  "scraper",
  "target_id",
]);

function stateLabels(node) {
  return Object.entries(node.labels || {})
    .filter(([k, v]) => v !== "" && !hiddenLabels.has(k))
    .sort(([a], [b]) => a.localeCompare(b));
}

// stateDataYAML renders the state's data payload as YAML, so arbitrarily
// nested values display without flattening. Labels ride in their own section.
function stateDataYAML(node) {
  const entries = Object.entries(node.data || {})
    .filter(([k, v]) => k !== "labels" && v !== null && v !== undefined && v !== "")
    .sort(([a], [b]) => a.localeCompare(b));
  if (!entries.length) return "";
  return toYAML(Object.fromEntries(entries));
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
    const ack = inc.status === "acknowledged"
      ? `<span class="metaMono">acked</span>`
      : `<button type="button" class="ackBtn" data-source="${esc(inc.source)}" data-id="${esc(inc.id)}">Ack</button>`;
    return `<div class="incRow">
      <span class="sev ${sev.cls}">${esc(sev.label)}</span>
      <div style="min-width:0;"><div class="title" title="${esc(inc.title)}">${esc(inc.title)}</div><div class="id">${esc(inc.id)}</div></div>
      <span class="cell dim" title="${esc(inc.source)}">${esc(inc.source)}</span>
      <span class="cell" title="${esc(inc.type || "")}">${esc(inc.type || "")}</span>
      <span class="cell dim">${esc(formatTime(inc.created_at))}</span>
      <span class="cell dim">${esc(formatAge(secondsSince(inc.last_updated_at)))} ago</span>
      <span class="incStatus">${esc(inc.status)}</span>
      ${ack}
    </div>`;
  }).join("");
  el.innerHTML = `<div class="incTable">
    <div class="incHead"><span>SEVERITY</span><span>TITLE</span><span>SOURCE</span><span>TYPE</span><span>STARTED</span><span>UPDATED</span><span>STATUS</span><span></span></div>
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

async function muteState(identity, status, duration) {
  const body = {
    identity,
    status,
    duration,
    reason: status === "pass" ? "muted: forced pass" : `muted: capped at ${status}`,
  };
  const res = await fetch(`${apiBase}/state/mutes`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  etagCache.clear();
  await refresh();
}

async function clearMute(identity) {
  const res = await fetch(`${apiBase}/state/mutes/${encodeURIComponent(identity)}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  etagCache.clear();
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
$("incidentFilter").addEventListener("change", (e) => { state.incidentFilter = e.target.value; render(); });
$("timelineWindow").addEventListener("change", (e) => {
  state.windowMs = Number(e.target.value) || 60 * 60 * 1000;
  refresh().then(clearError).catch(showError);
});

$("targets").addEventListener("click", (e) => {
  const row = e.target.closest("[data-target]");
  const issue = e.target.closest(".issueChip");
  if (issue && row) { e.stopPropagation(); selectTarget(row.dataset.target, issue.dataset.identity); return; }
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
  const apply = e.target.closest(".muteApply");
  if (apply) {
    const bar = apply.closest("[data-mute-identity]");
    muteState(
      bar.dataset.muteIdentity,
      bar.querySelector(".muteStatusSelect").value,
      bar.querySelector(".muteDurationSelect").value,
    ).then(clearError).catch(showError);
    return;
  }
  const clear = e.target.closest(".muteClear");
  if (clear) {
    const bar = clear.closest("[data-mute-identity]");
    clearMute(bar.dataset.muteIdentity).then(clearError).catch(showError);
    return;
  }
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
