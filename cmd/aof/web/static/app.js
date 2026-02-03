async function getJSON(url) {
  const r = await fetch(url, { headers: { "Accept": "application/json" } });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

async function postJSON(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(payload),
  });
  const txt = await r.text();
  if (!r.ok) throw new Error(txt);
  try { return JSON.parse(txt); } catch { return txt; }
}

function setPill(ok, msg) {
  const pill = document.getElementById("pill");
  pill.classList.toggle("ok", ok);
  pill.classList.toggle("bad", !ok);
  pill.textContent = msg;
}

function splitWatchlist(text) {
  return text.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
}

function fillConfig(cfg) {
  document.getElementById("dbPath").textContent = cfg.db_path || "-";
  document.getElementById("wlCount").textContent = String((cfg.watchlist || []).length);

  document.getElementById("rtInterval").value = cfg.realtime?.interval_seconds ?? 10;
  document.getElementById("onlyTradeHours").checked = !!cfg.realtime?.only_during_trading_hours;

  document.getElementById("indEnabled").checked = !!cfg.industry?.enabled;
  document.getElementById("indInterval").value = cfg.industry?.interval_seconds ?? 10;

  document.getElementById("conEnabled").checked = !!cfg.concept?.enabled;
  document.getElementById("conInterval").value = cfg.concept?.interval_seconds ?? 60;
  document.getElementById("conAll").checked = !!cfg.concept?.collect_all;
  document.getElementById("conTop").value = cfg.concept?.top_size ?? 100;

  document.getElementById("aggEnabled").checked = !!cfg.market_agg?.enabled;
  document.getElementById("aggInterval").value = cfg.market_agg?.interval_seconds ?? 60;
  document.getElementById("aggConc").value = cfg.market_agg?.concurrency ?? 4;

  document.getElementById("watchlist").value = (cfg.watchlist || []).join("\n");
}

function fmtMoney(v) {
  if (v === null || v === undefined) return "-";
  if (!Number.isFinite(v)) return "-";
  const abs = Math.abs(v);
  if (abs >= 1e8) return (v / 1e8).toFixed(2) + "亿";
  if (abs >= 1e4) return (v / 1e4).toFixed(2) + "万";
  return String(Math.round(v));
}

function setText(id, v) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = v;
}

function fillRealtime(snap, cfg) {
  const ts = snap?.ts_utc ? new Date(snap.ts_utc).toISOString() : "-";
  setText("rtTs", ts);

  const nb = snap?.northbound;
  setText("nbSh", nb ? fmtMoney(nb.SH?.NetBuyAmt ?? nb.sh?.netBuyAmt ?? nb.sh?.net_buy_amt) : "-");
  setText("nbSz", nb ? fmtMoney(nb.SZ?.NetBuyAmt ?? nb.sz?.netBuyAmt ?? nb.sz?.net_buy_amt) : "-");

  const agg = snap?.agg_by_key || {};
  const indKey = "industry_sum:" + ((cfg?.industry?.fid) || "f62");
  const allKey = "allstocks_sum:" + ((cfg?.market_agg?.fid) || "f62");
  setText("aggIndustry", agg[indKey] !== undefined ? fmtMoney(agg[indKey]) : "-");
  setText("aggAll", agg[allKey] !== undefined ? fmtMoney(agg[allKey]) : "-");

  const tbody = document.querySelector("#tblWatch tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  const byCode = new Map();
  (snap?.fundflow || []).forEach(r => { byCode.set(r.Code || r.code, r); });
  const wl = (cfg?.watchlist || []);
  wl.forEach(sym => {
    const code = sym.split(".")[0];
    const r = byCode.get(code);
    const tr = document.createElement("tr");
    const td = (t, cls) => {
      const x = document.createElement("td");
      if (cls) x.className = cls;
      x.textContent = t;
      return x;
    };
    tr.appendChild(td(code));
    tr.appendChild(td(r ? (r.Name || r.name || "-") : "-"));
    tr.appendChild(td(fmtMoney(r ? (r.NetMain ?? r.net_main) : NaN), "num"));
    tr.appendChild(td(fmtMoney(r ? (r.NetXL ?? r.net_xl) : NaN), "num"));
    tr.appendChild(td(fmtMoney(r ? (r.NetL ?? r.net_l) : NaN), "num"));
    tr.appendChild(td(fmtMoney(r ? (r.NetM ?? r.net_m) : NaN), "num"));
    tr.appendChild(td(fmtMoney(r ? (r.NetS ?? r.net_s) : NaN), "num"));
    tbody.appendChild(tr);
  });
}

function setRoute(route) {
  document.querySelectorAll(".view").forEach(v => {
    v.hidden = v.dataset.view !== route;
  });
  document.querySelectorAll(".tab").forEach(t => {
    t.classList.toggle("active", t.dataset.route === route);
  });
}

function getRoute() {
  const h = (location.hash || "#/home").replace(/^#\//, "");
  if (h === "history" || h === "settings" || h === "home") return h;
  return "home";
}

let state = {
  cfg: null,
  timers: [],
};

function clearTimers() {
  state.timers.forEach(id => clearInterval(id));
  state.timers = [];
}

async function refreshConfig() {
  try {
    const cfg = await getJSON("/api/config");
    state.cfg = cfg;
    fillConfig(cfg);
    setText("dbPath", cfg.db_path || "-");
    setText("wlCount", String((cfg.watchlist || []).length));
    return cfg;
  } catch (e) {
    console.error(e);
    throw e;
  }
}

function wire() {
  window.addEventListener("hashchange", () => bootRoute());

  document.getElementById("formRealtime").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      realtime_interval_seconds: Number(document.getElementById("rtInterval").value),
      only_during_trading_hours: document.getElementById("onlyTradeHours").checked,
    };
    try {
      setPill(true, "saving...");
      const cfg = await postJSON("/api/config", payload);
      state.cfg = cfg;
      fillConfig(cfg);
      setPill(true, "saved");
      setTimeout(() => setPill(true, "connected"), 700);
    } catch (e) {
      console.error(e);
      setPill(false, "save failed");
    }
  });

  document.getElementById("formBoards").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      industry_enabled: document.getElementById("indEnabled").checked,
      industry_interval_seconds: Number(document.getElementById("indInterval").value),
      concept_enabled: document.getElementById("conEnabled").checked,
      concept_interval_seconds: Number(document.getElementById("conInterval").value),
      concept_collect_all: document.getElementById("conAll").checked,
      concept_top_size: Number(document.getElementById("conTop").value),
    };
    try {
      setPill(true, "saving...");
      const cfg = await postJSON("/api/config", payload);
      state.cfg = cfg;
      fillConfig(cfg);
      setPill(true, "saved");
      setTimeout(() => setPill(true, "connected"), 700);
    } catch (e) {
      console.error(e);
      setPill(false, "save failed");
    }
  });

  document.getElementById("formAgg").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      market_agg_enabled: document.getElementById("aggEnabled").checked,
      market_agg_interval_seconds: Number(document.getElementById("aggInterval").value),
      market_agg_concurrency: Number(document.getElementById("aggConc").value),
    };
    try {
      setPill(true, "saving...");
      const cfg = await postJSON("/api/config", payload);
      state.cfg = cfg;
      fillConfig(cfg);
      setPill(true, "saved");
      setTimeout(() => setPill(true, "connected"), 700);
    } catch (e) {
      console.error(e);
      setPill(false, "save failed");
    }
  });

  document.getElementById("formWatch").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const wl = splitWatchlist(document.getElementById("watchlist").value);
    const payload = { watchlist: wl };
    try {
      setPill(true, "saving...");
      const cfg = await postJSON("/api/config", payload);
      state.cfg = cfg;
      fillConfig(cfg);
      setPill(true, "saved");
      setTimeout(() => setPill(true, "connected"), 700);
    } catch (e) {
      console.error(e);
      setPill(false, "save failed");
    }
  });

  document.getElementById("formHistory").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    await loadHistory();
  });
}

async function refreshRealtimeOnce() {
  try {
    const snap = await getJSON("/api/realtime");
    if (state.cfg) fillRealtime(snap, state.cfg);
    setPill(true, "connected");
  } catch (e) {
    console.error(e);
    setPill(false, "rt error");
  }
}

async function loadHistory() {
  try {
    setPill(true, "loading...");
    const source = document.getElementById("histSource").value;
    const kind = document.getElementById("histKind").value;
    const limit = Number(document.getElementById("histLimit").value || "200");
    const fid = (state.cfg?.market_agg?.fid) || "f62";

    const url = `/api/history/market_agg?source=${encodeURIComponent(source)}&fid=${encodeURIComponent(fid)}&kind=${encodeURIComponent(kind)}&limit=${encodeURIComponent(String(limit))}`;
    const rows = await getJSON(url);

    const head = document.getElementById("histHead");
    const tbody = document.querySelector("#tblHist tbody");
    head.innerHTML = "";
    tbody.innerHTML = "";

    const th = (t) => { const x = document.createElement("th"); x.textContent = t; return x; };
    if (kind === "rt") {
      head.appendChild(th("ts_utc"));
      head.appendChild(th("value"));
      rows.forEach(r => {
        const tr = document.createElement("tr");
        const td1 = document.createElement("td"); td1.textContent = r.ts_utc || r.TSUTC || "-";
        const td2 = document.createElement("td"); td2.textContent = fmtMoney(r.value ?? r.Value);
        td2.className = "num";
        tr.appendChild(td1); tr.appendChild(td2);
        tbody.appendChild(tr);
      });
    } else {
      head.appendChild(th("trade_date"));
      head.appendChild(th("value"));
      rows.forEach(r => {
        const tr = document.createElement("tr");
        const td1 = document.createElement("td"); td1.textContent = r.trade_date || r.TradeDate || "-";
        const td2 = document.createElement("td"); td2.textContent = fmtMoney(r.value ?? r.Value);
        td2.className = "num";
        tr.appendChild(td1); tr.appendChild(td2);
        tbody.appendChild(tr);
      });
    }
    setPill(true, "loaded");
    setTimeout(() => setPill(true, "connected"), 700);
  } catch (e) {
    console.error(e);
    setPill(false, "history error");
  }
}

async function bootRoute() {
  clearTimers();
  const route = getRoute();
  setRoute(route);

  try {
    await refreshConfig();
  } catch {
    setPill(false, "config error");
  }

  if (route === "home") {
    await refreshRealtimeOnce();
    state.timers.push(setInterval(refreshRealtimeOnce, 2000));
  } else if (route === "history") {
    await loadHistory();
  } else if (route === "settings") {
    // Keep config in sync while user edits in another terminal/editor.
    state.timers.push(setInterval(async () => { try { await refreshConfig(); } catch {} }, 5000));
  }
}

wire();
bootRoute();
