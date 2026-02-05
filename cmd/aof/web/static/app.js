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
  if (!pill) return;
  pill.classList.toggle("ok", ok);
  pill.classList.toggle("bad", !ok);
  pill.textContent = msg;
}

function initThemeToggle() {
  const key = "aof_theme";
  const btn = document.getElementById("themeToggle");
  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  let theme = localStorage.getItem(key) || (prefersDark ? "dark" : "light");
  if (theme !== "dark" && theme !== "light") theme = "dark";
  const apply = (t) => {
    document.documentElement.setAttribute("data-theme", t);
    localStorage.setItem(key, t);
    if (btn) btn.textContent = (t === "dark" ? "浅色" : "深色");
  };
  apply(theme);
  if (btn) {
    btn.addEventListener("click", () => {
      const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
      apply(next);
    });
  }
}

function splitWatchlist(text) {
  return text.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
}

function fillConfig(cfg) {
  document.getElementById("dbPath").textContent = cfg.db_path || "-";
  document.getElementById("wlCount").textContent = String((cfg.watchlist || []).length);

  document.getElementById("rtInterval").value = cfg.realtime?.interval_seconds ?? 20;
  document.getElementById("onlyTradeHours").checked = !!cfg.realtime?.only_during_trading_hours;
  document.getElementById("topSize").value = cfg.toplist?.size ?? 10;

  document.getElementById("indEnabled").checked = !!cfg.industry?.enabled;
  document.getElementById("indInterval").value = cfg.industry?.interval_seconds ?? 10;

  document.getElementById("conEnabled").checked = !!cfg.concept?.enabled;
  document.getElementById("conInterval").value = cfg.concept?.interval_seconds ?? 60;
  document.getElementById("conAll").checked = !!cfg.concept?.collect_all;
  document.getElementById("conTop").value = cfg.concept?.top_size ?? 100;

  document.getElementById("aggEnabled").checked = !!cfg.market_agg?.enabled;
  document.getElementById("aggInterval").value = cfg.market_agg?.interval_seconds ?? 120;
  document.getElementById("aggConc").value = cfg.market_agg?.concurrency ?? 4;

  if (cfg.board_trend) {
    state.boardTrendCfg.batchSize = cfg.board_trend.batch_size ?? state.boardTrendCfg.batchSize;
    state.boardTrendCfg.concurrency = cfg.board_trend.concurrency ?? state.boardTrendCfg.concurrency;
    state.boardTrendCfg.gapMs = cfg.board_trend.gap_ms ?? state.boardTrendCfg.gapMs;
    state.boardTrendCfg.afterCloseMode = cfg.board_trend.after_close_mode ?? state.boardTrendCfg.afterCloseMode;
    state.boardTrendCfg.afterCloseIntervalSeconds = cfg.board_trend.after_close_interval_seconds ?? state.boardTrendCfg.afterCloseIntervalSeconds;
    fillTrendCfgForm();
  }

  document.getElementById("watchlist").value = (cfg.watchlist || []).join("\n");
}

function fillTrendCfgForm() {
  document.getElementById("trendBatch").value = String(state.boardTrendCfg.batchSize);
  document.getElementById("trendConc").value = String(state.boardTrendCfg.concurrency);
  document.getElementById("trendGap").value = String(state.boardTrendCfg.gapMs);
  document.getElementById("trendAfterMode").value = state.boardTrendCfg.afterCloseMode;
  document.getElementById("trendAfterInterval").value = String(state.boardTrendCfg.afterCloseIntervalSeconds);
}

function fmtMoney(v) {
  if (v === null || v === undefined) return "-";
  if (!Number.isFinite(v)) return "-";
  const abs = Math.abs(v);
  if (abs >= 1e8) return (v / 1e8).toFixed(2) + "亿";
  if (abs >= 1e4) return (v / 1e4).toFixed(2) + "万";
  return String(Math.round(v));
}

function fmtYi(v) {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return (n / 1e8).toFixed(2);
}

function fmtBJTime(isoLike) {
  if (!isoLike) return "-";
  const d = new Date(isoLike);
  if (Number.isNaN(d.getTime())) return "-";
  // Use a stable YYYY-MM-DD HH:mm:ss format.
  const parts = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(d);
  const m = {};
  parts.forEach(p => { if (p.type !== "literal") m[p.type] = p.value; });
  return `${m.year}-${m.month}-${m.day} ${m.hour}:${m.minute}:${m.second}`;
}

function getBJTimeParts() {
  const parts = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date());
  const m = {};
  parts.forEach(p => { if (p.type !== "literal") m[p.type] = p.value; });
  return m;
}

function getBJWeekday() {
  const wd = new Intl.DateTimeFormat("en-US", { timeZone: "Asia/Shanghai", weekday: "short" }).format(new Date());
  return wd; // Mon, Tue, Wed, Thu, Fri, Sat, Sun
}

function isWeekendBJ() {
  const wd = getBJWeekday();
  return wd === "Sat" || wd === "Sun";
}

function getBJMinutes() {
  const t = getBJTimeParts();
  const h = Number(t.hour || 0);
  const m = Number(t.minute || 0);
  return h * 60 + m;
}

function isBefore0930BJ() {
  return getBJMinutes() < 9 * 60 + 30;
}

function isAfterCloseBJ() {
  const hm = getBJMinutes();
  return hm >= 15 * 60;
}

function colorVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name);
  return (v && v.trim()) ? v.trim() : fallback;
}

function isLightTheme() {
  return document.documentElement.getAttribute("data-theme") === "light";
}

function drawLineChart(canvas, labels, values) {
  if (!canvas) return;

  const parentW = canvas.clientWidth || 600;
  const parentH = canvas.clientHeight || 240;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(parentW * dpr);
  canvas.height = Math.floor(parentH * dpr);

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  const bg = isLightTheme() ? "#ffffff" : "#0b1018";
  const stroke = colorVar("--stroke", "#223042");
  const muted = colorVar("--muted", "#a8b4c6");
  const accent = colorVar("--accent", "#46d6a3");

  ctx.clearRect(0, 0, parentW, parentH);
  ctx.fillStyle = isLightTheme() ? "#ffffff" : "#0b1018";
  ctx.fillRect(0, 0, parentW, parentH);
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, parentW - 1, parentH - 1);

  const n = Math.min(labels.length, values.length);
  if (n < 2) {
    ctx.fillStyle = muted;
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
    ctx.fillText("no data", 12, 20);
    return;
  }

  const padL = 56, padR = 14, padT = 14, padB = 32;
  const w = parentW - padL - padR;
  const h = parentH - padT - padB;

  let minY = Infinity, maxY = -Infinity;
  for (let i = 0; i < n; i++) {
    const y = Number(values[i]);
    if (!Number.isFinite(y)) continue;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  if (!Number.isFinite(minY) || !Number.isFinite(maxY)) return;
  if (minY === maxY) {
    minY -= 1;
    maxY += 1;
  } else {
    const pad = (maxY - minY) * 0.06;
    minY -= pad;
    maxY += pad;
  }

  const xAt = (i) => padL + (i / (n - 1)) * w;
  const yAt = (v) => padT + (1 - (v - minY) / (maxY - minY)) * h;

  // Grid + y-axis labels
  ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
  ctx.fillStyle = muted;
  ctx.strokeStyle = "rgba(168,180,198,.18)";
  ctx.lineWidth = 1;
  const yTicks = 4;
  for (let t = 0; t <= yTicks; t++) {
    const p = t / yTicks;
    const y = padT + p * h;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + w, y);
    ctx.stroke();
    const val = maxY - p * (maxY - minY);
    const txt = fmtMoney(val);
    ctx.fillText(txt, 10, y + 4);
  }

  // X-axis labels
  ctx.strokeStyle = stroke;
  ctx.beginPath();
  ctx.moveTo(padL, padT + h);
  ctx.lineTo(padL + w, padT + h);
  ctx.stroke();

  const xTicks = Math.min(6, n);
  for (let t = 0; t < xTicks; t++) {
    const idx = Math.round((t / (xTicks - 1)) * (n - 1));
    const x = xAt(idx);
    const lab = String(labels[idx] ?? "");
    ctx.fillStyle = muted;
    ctx.textAlign = t === 0 ? "left" : (t === xTicks - 1 ? "right" : "center");
    ctx.fillText(lab, x, parentH - 10);
  }
  ctx.textAlign = "left";

  // Line path
  ctx.strokeStyle = accent;
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (let i = 0; i < n; i++) {
    const v = Number(values[i]);
    const x = xAt(i);
    const y = yAt(v);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // Points
  ctx.fillStyle = bg;
  ctx.strokeStyle = accent;
  ctx.lineWidth = 2;
  for (let i = 0; i < n; i += Math.max(1, Math.floor(n / 80))) {
    const v = Number(values[i]);
    const x = xAt(i);
    const y = yAt(v);
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  }
}

function normalizeBoardTrend(points) {
  const labels = [];
  const values = [];
  if (!Array.isArray(points) || points.length === 0) return { labels, values };
  let base = null;
  for (const p of points) {
    const ts = String(p.ts || p.TS || "");
    let hm = null;
    let label = "";
    if (ts.length >= 16) {
      hm = Number(ts.slice(11, 13)) * 60 + Number(ts.slice(14, 16));
      label = ts.slice(11, 16);
    } else if (ts.length >= 5) {
      hm = Number(ts.slice(0, 2)) * 60 + Number(ts.slice(3, 5));
      label = ts.slice(0, 5);
    }
    if (hm === null || Number.isNaN(hm)) continue;
    if (hm < 9 * 60 + 30 || hm > 15 * 60) continue;
    const v = Number(p.price ?? p.Price);
    if (!Number.isFinite(v)) continue;
    if (base === null) base = v;
    labels.push(label);
    values.push(v - base);
  }
  return { labels, values };
}

function trendToSeries(points, startMin, endMin, labelMode) {
  const labels = [];
  const values = [];
  (points || []).forEach(p => {
    const ts = String(p.ts || p.TS || "");
    let hm = null;
    let label = "";
    if (ts.length >= 19) {
      hm = Number(ts.slice(11, 13)) * 60 + Number(ts.slice(14, 16));
      label = (labelMode === "sec") ? ts.slice(5, 19) : ts.slice(11, 16);
    } else if (ts.length >= 16) {
      hm = Number(ts.slice(11, 13)) * 60 + Number(ts.slice(14, 16));
      label = ts.slice(11, 16);
    } else if (ts.length >= 5) {
      hm = Number(ts.slice(0, 2)) * 60 + Number(ts.slice(3, 5));
      label = ts.slice(0, 5);
    }
    if (hm === null || Number.isNaN(hm)) return;
    if (hm < startMin || hm > endMin) return;
    const v = Number(p.price ?? p.Price ?? p.value ?? p.Value);
    if (!Number.isFinite(v)) return;
    labels.push(label);
    values.push(v);
  });
  return { labels, values };
}

function renderHistoryChart(meta, rows) {
  const panel = document.getElementById("histChartPanel");
  const canvas = document.getElementById("histChart");
  if (!panel || !canvas) return;
  const kind = meta?.kind || "daily";
  panel.hidden = false;
  const labels = [];
  const values = [];
  (rows || []).forEach(r => {
    const v = Number(r.value ?? r.Value);
    if (!Number.isFinite(v)) return;
    if (kind === "rt") {
      const t = fmtBJTime(r.ts_utc || r.TSUTC || "");
      labels.push(t === "-" ? "-" : t.slice(5, 16));
    } else {
      labels.push(r.trade_date || r.TradeDate || "-");
    }
    values.push(v);
  });
  drawLineChart(canvas, labels, values);
}

function setText(id, v) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = v;
}

function fillRealtime(snap, cfg) {
  const ts = snap?.ts_utc ? fmtBJTime(snap.ts_utc) : "-";
  setText("rtTs", ts);

  const nb = snap?.northbound;
  if (!nb) {
    setText("nbShQuotaPct", "-");
    setText("nbShQuota", "-");
    setText("nbShTurnover", "-");
    setText("nbSzQuotaPct", "-");
    setText("nbSzQuota", "-");
    setText("nbSzTurnover", "-");
  } else {
    const shRemain = nb.SH?.DayAmtRemain ?? nb.sh?.dayAmtRemain ?? nb.sh?.day_amt_remain;
    const shThreshold = nb.SH?.DayAmtThreshold ?? nb.sh?.dayAmtThreshold ?? nb.sh?.day_amt_threshold;
    const shTurnover = nb.SH?.BuySellAmt ?? nb.sh?.buySellAmt ?? nb.sh?.buy_sell_amt;
    const szRemain = nb.SZ?.DayAmtRemain ?? nb.sz?.dayAmtRemain ?? nb.sz?.day_amt_remain;
    const szThreshold = nb.SZ?.DayAmtThreshold ?? nb.sz?.dayAmtThreshold ?? nb.sz?.day_amt_threshold;
    const szTurnover = nb.SZ?.BuySellAmt ?? nb.sz?.buySellAmt ?? nb.sz?.buy_sell_amt;

    const fmtQuota = (remain, threshold) => {
      const r = Number(remain);
      const t = Number(threshold);
      if (!Number.isFinite(r) || !Number.isFinite(t) || t <= 0) return { pct: "-", remain: "-" };
      const ratio = r / t;
      if (ratio >= 0.3) return { pct: (ratio * 100).toFixed(2), remain: "充足" };
      return { pct: (ratio * 100).toFixed(2), remain: fmtYi(r) };
    };

    const shq = fmtQuota(shRemain, shThreshold);
    const szq = fmtQuota(szRemain, szThreshold);
    setText("nbShQuotaPct", shq.pct);
    setText("nbSzQuotaPct", szq.pct);
    setText("nbShQuota", shq.remain);
    setText("nbSzQuota", szq.remain);
    setText("nbShTurnover", Number(shTurnover) > 0 ? fmtYi(Number(shTurnover)) : "-");
    setText("nbSzTurnover", Number(szTurnover) > 0 ? fmtYi(Number(szTurnover)) : "-");
  }

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
  const route = h.split("?")[0];
  if (route === "history" || route === "history-industry" || route === "history-concept" || route === "settings" || route === "home" || route === "industry" || route === "concept" || route === "trend") return route;
  return "home";
}

function getRouteQuery() {
  const h = (location.hash || "#/home").replace(/^#\//, "");
  const idx = h.indexOf("?");
  if (idx < 0) return {};
  const qs = h.slice(idx + 1);
  const out = {};
  qs.split("&").forEach(pair => {
    if (!pair) return;
    const parts = pair.split("=");
    const k = decodeURIComponent(parts[0] || "");
    const v = decodeURIComponent(parts[1] || "");
    if (k) out[k] = v;
  });
  return out;
}

let state = {
  cfg: null,
  timers: [],
  historyMeta: null,
  historyRows: null,
  homeChartRows: null,
  homeIndex: null,
  trendPoints: null,
  dailyObserver: null,
  boardCharts: {
    industry: new Map(),
    concept: new Map(),
  },
  boardList: {
    industry: [],
    concept: [],
  },
  boardTrendRunId: {
    industry: 0,
    concept: 0,
  },
  boardDailyCharts: {
    industry: new Map(),
    concept: new Map(),
  },
  boardDailyList: {
    industry: [],
    concept: [],
  },
  boardDailyRunId: {
    industry: 0,
    concept: 0,
  },
  batchTimers: {
    industry: null,
    concept: null,
  },
  boardTrendCfg: {
    batchSize: 20,
    concurrency: 2,
    gapMs: 400,
    afterCloseMode: "once",
    afterCloseIntervalSeconds: 300,
  },
  marketClosed: false,
};

function setLoadStatus(type, text, show) {
  const id = type === "industry" ? "histIndLoadStatus" : "histConLoadStatus";
  const el = document.getElementById(id);
  if (!el) return;
  if (show === false) {
    el.hidden = true;
    return;
  }
  el.textContent = text || "";
  el.hidden = false;
}

function clearTimers() {
  state.timers.forEach(id => clearInterval(id));
  state.timers = [];
  ["industry", "concept"].forEach(tp => {
    const id = state.batchTimers?.[tp];
    if (id) clearInterval(id);
    if (state.batchTimers) state.batchTimers[tp] = null;
  });
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

async function refreshBuildInfo() {
  try {
    const v = await getJSON("/api/version");
    if (v && v.last_commit_time) {
      setText("buildInfo", `Code by Yuhao@jiansutech.com at ${v.last_commit_time}`);
    }
  } catch (e) {
    console.error(e);
  }
}

function wire() {
  initThemeToggle();
  fillTrendCfgForm();
  window.addEventListener("hashchange", () => bootRoute());
  window.addEventListener("resize", () => {
    const route = getRoute();
    if (route === "history") {
      if (state.historyMeta && state.historyRows) {
        renderHistoryChart(state.historyMeta, state.historyRows);
      }
      return;
    }
    if (route === "home" && state.homeChartRows) {
      renderIndustryChartHome(state.homeChartRows);
    }
    if (route === "home" && state.homeIndex) {
      renderIndexChartHome("homeSHChart", "homeSHHint", state.homeIndex.shPts);
      renderIndexChartHome("homeSZChart", "homeSZHint", state.homeIndex.szPts);
    }
    if (route === "home") {
      renderBoardChartsFromCache("industry");
      renderBoardChartsFromCache("concept");
    }
    if (route === "history-industry") {
      renderBoardDailyFromCache("industry");
    }
    if (route === "history-concept") {
      renderBoardDailyFromCache("concept");
    }
    if (route === "trend" && state.trendPoints) {
      renderTrendChart(state.trendPoints);
    }
  });


  document.getElementById("formRealtime").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      realtime_interval_seconds: Number(document.getElementById("rtInterval").value),
      only_during_trading_hours: document.getElementById("onlyTradeHours").checked,
      toplist_size: Number(document.getElementById("topSize").value),
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

  document.getElementById("formBoardTrend").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const batchSize = Number(document.getElementById("trendBatch").value);
    const concurrency = Number(document.getElementById("trendConc").value);
    const gapMs = Number(document.getElementById("trendGap").value);
    const afterMode = document.getElementById("trendAfterMode").value;
    const afterInterval = Number(document.getElementById("trendAfterInterval").value);
    state.boardTrendCfg.batchSize = Math.min(100, Math.max(5, Number.isFinite(batchSize) ? batchSize : 20));
    state.boardTrendCfg.concurrency = Math.min(6, Math.max(1, Number.isFinite(concurrency) ? concurrency : 2));
    state.boardTrendCfg.gapMs = Math.min(5000, Math.max(100, Number.isFinite(gapMs) ? gapMs : 400));
    state.boardTrendCfg.afterCloseMode = (afterMode === "interval") ? "interval" : "once";
    state.boardTrendCfg.afterCloseIntervalSeconds = Math.min(1800, Math.max(60, Number.isFinite(afterInterval) ? afterInterval : 300));
    fillTrendCfgForm();
    try {
      setPill(true, "saving...");
      const payload = {
        board_trend_batch_size: state.boardTrendCfg.batchSize,
        board_trend_concurrency: state.boardTrendCfg.concurrency,
        board_trend_gap_ms: state.boardTrendCfg.gapMs,
        board_trend_after_close_mode: state.boardTrendCfg.afterCloseMode,
        board_trend_after_close_interval_seconds: state.boardTrendCfg.afterCloseIntervalSeconds,
      };
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
  document.getElementById("histPeriod")?.addEventListener("change", () => {
    const v = Number(document.getElementById("histPeriod").value || "90");
    const limit = document.getElementById("histLimit");
    if (limit) limit.value = String(v);
  });

  document.getElementById("formHistInd")?.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    await loadBoardDailyView("industry");
  });
  document.getElementById("histIndSort")?.addEventListener("change", () => sortBoardDailyGrid("industry"));
  document.getElementById("histIndFilter")?.addEventListener("change", () => sortBoardDailyGrid("industry"));
  document.getElementById("formHistCon")?.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    await loadBoardDailyView("concept");
  });
  document.getElementById("histConSort")?.addEventListener("change", () => sortBoardDailyGrid("concept"));
  document.getElementById("histConFilter")?.addEventListener("change", () => sortBoardDailyGrid("concept"));

  document.getElementById("formTrend")?.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const code = String(document.getElementById("trendBoard")?.value || "").trim();
    await loadTrendView(code);
  });

  document.getElementById("histIndBatch")?.addEventListener("click", async () => {
    await startBoardDailyBatch("industry");
  });
  document.getElementById("histConBatch")?.addEventListener("click", async () => {
    await startBoardDailyBatch("concept");
  });
}

async function refreshRealtimeOnce() {
  try {
    const snap = await getJSON("/api/realtime");
    if (state.cfg) fillRealtime(snap, state.cfg);
    setPill(true, "connected");
    if (!state.marketClosed && isAfterCloseBJ()) {
      state.marketClosed = true;
      clearTimers();
      setPill(true, "market closed");
      startMissingTrendRefresh();
    }
  } catch (e) {
    console.error(e);
    setPill(false, "rt error");
  }
}

async function loadBoardsFor(type, _listId, hintId) {
  const fid = (type === "concept" ? state.cfg?.concept?.fid : state.cfg?.industry?.fid) || "f62";
  const url = `/api/boards?type=${encodeURIComponent(type)}&fid=${encodeURIComponent(fid)}&limit=500`;
  const data = await getJSON(url);
  const boards = Array.isArray(data) ? data : (data.rows || []);
  state.boardList[type] = boards;
  const boardHint = document.getElementById(hintId);
  if (boardHint) {
    const show = !Array.isArray(data) && !!data.from_live;
    boardHint.hidden = !show;
    if (show) {
      boardHint.textContent = "市场休市或未抓到快照，已即时拉取板块数据。";
    }
  }
  buildBoardGrid(type, boards);
  refreshBoardTrends(type, boards);
}

function startMissingTrendRefresh() {
  // Only refresh missing charts after close.
  const tick = async () => {
    if (!state.marketClosed) return;
    await Promise.all([
      refreshBoardTrends("industry", state.boardList.industry, true),
      refreshBoardTrends("concept", state.boardList.concept, true),
    ]);
  };
  tick();
  if (state.boardTrendCfg.afterCloseMode === "interval") {
    const interval = state.boardTrendCfg.afterCloseIntervalSeconds * 1000;
    state.timers.push(setInterval(tick, interval));
  }
}

function buildBoardGrid(type, boards) {
  const gridId = type === "industry" ? "boardGridInd" : "boardGridCon";
  const grid = document.getElementById(gridId);
  if (!grid) return;
  grid.innerHTML = "";
  const map = new Map();
  (boards || []).forEach(b => {
    const code = b.code || b.Code || "";
    if (!code) return;
    const card = document.createElement("div");
    card.className = "boardCard";
    card.dataset.code = code;

    const title = document.createElement("div");
    title.className = "boardCardTitle";
    title.textContent = `${b.name || "-"} ${code}`;

    const meta = document.createElement("div");
    meta.className = "boardCardMeta";
    const val = document.createElement("div");
    val.textContent = fmtMoney(Number(b.value ?? b.Value));
    const pct = document.createElement("div");
    const pctNum = Number(b.pct ?? b.Pct);
    pct.textContent = Number.isFinite(pctNum) ? pctNum.toFixed(2) + "%" : "-";
    pct.className = Number.isFinite(pctNum) ? (pctNum > 0 ? "up" : (pctNum < 0 ? "down" : "")) : "";
    meta.appendChild(val);
    meta.appendChild(pct);

    const canvas = document.createElement("canvas");
    canvas.className = "boardCardCanvas";
    canvas.height = 250;

    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(canvas);
    grid.appendChild(card);

    map.set(code, { canvas, title, meta, name: b.name || "-", points: null });
  });
  state.boardCharts[type] = map;
}

function renderBoardChartsFromCache(type) {
  const cache = state.boardCharts[type];
  if (!cache || cache.size === 0) return;
  cache.forEach((v) => {
    if (v.points && v.points.length >= 2) {
      const norm = normalizeBoardTrend(v.points);
      if (norm.labels.length >= 2) {
        drawLineChart(v.canvas, norm.labels, norm.values);
      }
    }
  });
}

function buildBoardDailyGrid(type, boards) {
  const gridId = type === "industry" ? "boardGridHistInd" : "boardGridHistCon";
  const grid = document.getElementById(gridId);
  if (!grid) return;
  grid.innerHTML = "";
  const map = new Map();
  ensureDailyObserver();
  (boards || []).forEach(b => {
    const code = b.code || b.Code || "";
    if (!code) return;
    const card = document.createElement("div");
    card.className = "boardCard";
    card.dataset.code = code;

    const title = document.createElement("div");
    title.className = "boardCardTitle";
    title.textContent = `${b.name || "-"} ${code}`;

    const meta = document.createElement("div");
    meta.className = "boardCardMeta";
    const val = document.createElement("div");
    val.textContent = "-";
    const date = document.createElement("div");
    date.textContent = "-";
    meta.appendChild(val);
    meta.appendChild(date);

    const canvas = document.createElement("canvas");
    canvas.className = "boardCardCanvas";
    canvas.height = 250;

    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(canvas);
    grid.appendChild(card);

    state.dailyObserver.observe(card);
    map.set(code, { card, canvas, title, meta, name: b.name || "-", points: null, valEl: val, dateEl: date, inView: false, rendered: false });
  });
  state.boardDailyCharts[type] = map;
}

function ensureDailyObserver() {
  if (state.dailyObserver) return state.dailyObserver;
  state.dailyObserver = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      const card = e.target;
      if (!card || !card.dataset) return;
      const code = card.dataset.code || "";
      if (!code) return;
      ["industry", "concept"].forEach(tp => {
        const cache = state.boardDailyCharts[tp];
        const entry = cache?.get(code);
        if (!entry) return;
        entry.inView = !!e.isIntersecting;
        if (entry.inView && entry.points && entry.points.length >= 2 && !entry.rendered) {
          drawBoardDailyEntry(entry);
        }
      });
    });
  }, { root: null, rootMargin: "120px", threshold: 0.01 });
  return state.dailyObserver;
}

function drawBoardDailyEntry(entry) {
  if (!entry || !entry.canvas || !entry.points || entry.points.length < 2) return;
  const labels = entry.points.map(p => String(p.trade_date || p.TradeDate || "").slice(5));
  const values = entry.points.map(p => Number(p.value ?? p.Value));
  drawLineChart(entry.canvas, labels, values);
  entry.rendered = true;
}

function getDailySortIds(type) {
  return type === "industry"
    ? { sort: "histIndSort", filter: "histIndFilter", grid: "boardGridHistInd" }
    : { sort: "histConSort", filter: "histConFilter", grid: "boardGridHistCon" };
}

function lastPointValue(entry) {
  const pts = entry?.points || [];
  if (!pts.length) return null;
  const last = pts[pts.length - 1];
  const v = Number(last.value ?? last.Value);
  return Number.isFinite(v) ? v : null;
}

function sortBoardDailyGrid(type) {
  const ids = getDailySortIds(type);
  const sortMode = String(document.getElementById(ids.sort)?.value || "value_desc");
  const filterMode = String(document.getElementById(ids.filter)?.value || "all");
  const grid = document.getElementById(ids.grid);
  const cache = state.boardDailyCharts[type];
  if (!grid || !cache || cache.size === 0) return;

  const entries = [];
  cache.forEach((entry, code) => {
    const v = lastPointValue(entry);
    // Keep unloaded entries at bottom.
    const name = String(entry?.name || "").toLowerCase();
    if (filterMode === "in" && (v === null || v <= 0)) return;
    if (filterMode === "out" && (v === null || v >= 0)) return;
    entries.push({ code, entry, v, name });
  });

  const cmp = (a, b) => {
    // Always push null values to the end.
    const an = (a.v === null), bn = (b.v === null);
    if (an !== bn) return an ? 1 : -1;
    if (sortMode === "value_asc") return (a.v ?? 0) - (b.v ?? 0);
    if (sortMode === "abs_desc") return Math.abs(b.v ?? 0) - Math.abs(a.v ?? 0);
    if (sortMode === "name_asc") return a.name.localeCompare(b.name);
    // value_desc default
    return (b.v ?? 0) - (a.v ?? 0);
  };
  entries.sort(cmp);

  // Reorder cards in the grid by moving existing nodes.
  grid.innerHTML = "";
  entries.forEach(({ entry }) => {
    if (entry?.card) grid.appendChild(entry.card);
  });
}

function renderBoardDailyFromCache(type) {
  const cache = state.boardDailyCharts[type];
  if (!cache || cache.size === 0) return;
  cache.forEach((v) => {
    if (v.points && v.points.length >= 2) {
      if (!v.rendered && v.inView) {
        drawBoardDailyEntry(v);
      }
    }
  });
}

async function fetchBoardDaily(boardCode, type, fid, limit, refresh) {
  const url = `/api/board/daily?board=${encodeURIComponent(boardCode)}&type=${encodeURIComponent(type)}&fid=${encodeURIComponent(fid)}&limit=${encodeURIComponent(String(limit))}` + (refresh ? "&refresh=1" : "");
  return await getJSON(url);
}

async function refreshBoardDaily(type, boards, limit, refresh, onlyMissing = false) {
  const cache = state.boardDailyCharts[type];
  if (!cache || cache.size === 0) return;
  let list = (boards || []).map(b => b.code || b.Code).filter(Boolean);
  if (onlyMissing) {
    list = list.filter(code => {
      const entry = cache.get(code);
      return !(entry && entry.points && entry.points.length >= 2);
    });
  }
  if (list.length === 0) return;
  const runId = ++state.boardDailyRunId[type];
  const batchSize = state.boardTrendCfg.batchSize;
  const concurrency = Math.max(1, Math.min(6, state.boardTrendCfg.concurrency || 2));
  let offset = 0;
  let done = 0;
  const total = list.length;
  setLoadStatus(type, `加载中... 0/${total}`, true);
  while (offset < list.length) {
    if (state.boardDailyRunId[type] !== runId) return;
    const batch = list.slice(offset, offset + batchSize);
    offset += batchSize;
    let idx = 0;
    const run = async () => {
      while (idx < batch.length) {
        const code = batch[idx++];
        try {
          const data = await fetchBoardDaily(code, type, (state.cfg?.[type]?.fid) || "f62", limit, refresh);
          const points = data?.points || [];
          const entry = cache.get(code);
          if (entry && points.length >= 2) {
            entry.points = points;
            entry.rendered = false;
            if (entry.inView) {
              drawBoardDailyEntry(entry);
            }
            const last = points[points.length - 1];
            if (entry.valEl) {
              const lv = Number(last.value ?? last.Value);
              entry.valEl.textContent = Number.isFinite(lv) ? (lv > 0 ? "+" : "") + fmtMoney(lv) : "-";
            }
            if (entry.dateEl) entry.dateEl.textContent = String(last.trade_date || last.TradeDate || "-");
          }
        } catch (e) {
          console.error(e);
        } finally {
          done++;
          if (done % 10 === 0 || done === total) {
            setLoadStatus(type, `加载中... ${done}/${total}`, true);
          }
        }
      }
    };
    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(run());
    await Promise.all(workers);
    sortBoardDailyGrid(type);
    if (offset < list.length) {
      await new Promise(r => setTimeout(r, state.boardTrendCfg.gapMs));
    }
  }
  sortBoardDailyGrid(type);
  setLoadStatus(type, "", false);
}

async function loadBoardDailyView(type) {
  const limitId = type === "industry" ? "histIndLimit" : "histConLimit";
  const boardsId = type === "industry" ? "histIndBoards" : "histConBoards";
  const refreshId = type === "industry" ? "histIndRefresh" : "histConRefresh";
  const limit = Number(document.getElementById(limitId)?.value || "120");
  const boardLimit = Number(document.getElementById(boardsId)?.value || "80");
  const refresh = document.getElementById(refreshId)?.checked;
  const fid = (state.cfg?.[type]?.fid) || "f62";

  const url = `/api/boards?type=${encodeURIComponent(type)}&fid=${encodeURIComponent(fid)}&limit=${encodeURIComponent(String(boardLimit))}`;
  const data = await getJSON(url);
  const boards = Array.isArray(data) ? data : (data.rows || []);
  state.boardDailyList[type] = boards;
  buildBoardDailyGrid(type, boards);
  // Don't block UI; fetch in background.
  refreshBoardDaily(type, boards, limit, refresh, false);
  sortBoardDailyGrid(type);
}

function batchStatusId(type) {
  return type === "industry" ? "histIndBatchStatus" : "histConBatchStatus";
}

function setBatchStatus(type, text) {
  const el = document.getElementById(batchStatusId(type));
  if (el) el.textContent = text;
}

async function fetchBoardDailyBatchStatus(type) {
  return await getJSON(`/api/board/daily/batch?type=${encodeURIComponent(type)}`);
}

function formatBatchStatus(s) {
  if (!s) return "批量任务：未知";
  if (!s.running && s.total === 0 && s.ok === 0 && s.failed === 0) {
    return "批量任务：未启动";
  }
  const prog = (s.total > 0) ? `${s.ok + s.failed}/${s.total}` : `${s.ok + s.failed}/?`;
  const state = s.running ? "运行中" : "已完成";
  const err = s.last_err ? `，最后错误：${s.last_err}` : "";
  return `批量任务：${state}，进度 ${prog}，成功 ${s.ok}，失败 ${s.failed}${err}`;
}

async function pollBoardDailyBatch(type) {
  try {
    const s = await fetchBoardDailyBatchStatus(type);
    setBatchStatus(type, formatBatchStatus(s));
    if (!s.running && state.batchTimers?.[type]) {
      clearInterval(state.batchTimers[type]);
      state.batchTimers[type] = null;
    }
  } catch (e) {
    console.error(e);
    setBatchStatus(type, "批量任务：状态获取失败");
  }
}

async function startBoardDailyBatch(type) {
  const limitId = type === "industry" ? "histIndLimit" : "histConLimit";
  const limit = Number(document.getElementById(limitId)?.value || "120");
  setBatchStatus(type, "批量任务：启动中...");
  try {
    await postJSON(`/api/board/daily/batch?type=${encodeURIComponent(type)}&limit=${encodeURIComponent(String(limit))}`, {});
    await pollBoardDailyBatch(type);
    if (state.batchTimers?.[type]) clearInterval(state.batchTimers[type]);
    state.batchTimers[type] = setInterval(() => {
      pollBoardDailyBatch(type);
    }, 3000);
  } catch (e) {
    console.error(e);
    setBatchStatus(type, "批量任务：启动失败（可能已有任务在运行）");
  }
}

async function fetchBoardTrend(boardCode) {
  const url = `/api/board/trend?board=${encodeURIComponent(boardCode)}`;
  return await getJSON(url);
}

function renderTrendChart(points) {
  const canvas = document.getElementById("trendChart");
  const hint = document.getElementById("trendHint");
  if (!canvas) return;
  const norm = normalizeBoardTrend(points);
  if (norm.labels.length < 2) {
    if (hint) {
      hint.textContent = "暂无数据";
      hint.hidden = false;
    }
    return;
  }
  if (hint) hint.hidden = true;
  drawLineChart(canvas, norm.labels, norm.values);
}

async function loadTrendView(boardCode) {
  const hint = document.getElementById("trendHint");
  const input = document.getElementById("trendBoard");
  const title = document.getElementById("trendTitle");
  if (hint) {
    hint.textContent = "加载中...";
    hint.hidden = false;
  }
  const code = String(boardCode || "").trim().toUpperCase();
  if (input && code) input.value = code;
  if (!code) {
    if (hint) {
      hint.textContent = "请输入板块代码";
      hint.hidden = false;
    }
    return;
  }
  try {
    const data = await fetchBoardTrend(code);
    const points = data?.points || [];
    state.trendPoints = points;
    if (title) title.textContent = `板块趋势（${code}）`;
    renderTrendChart(points);
  } catch (e) {
    console.error(e);
    if (hint) {
      hint.textContent = "加载失败";
      hint.hidden = false;
    }
  }
}

async function refreshBoardTrends(type, boards, onlyMissing = false) {
  const cache = state.boardCharts[type];
  if (!cache || cache.size === 0) return;
  let list = (boards || []).map(b => b.code || b.Code).filter(Boolean);
  if (onlyMissing) {
    list = list.filter(code => {
      const entry = cache.get(code);
      return !(entry && entry.points && entry.points.length >= 2);
    });
  }
  if (list.length === 0) return;
  const runId = ++state.boardTrendRunId[type];
  const batchSize = state.boardTrendCfg.batchSize;
  const concurrency = state.boardTrendCfg.concurrency;
  let offset = 0;
  while (offset < list.length) {
    if (state.boardTrendRunId[type] !== runId) return;
    const batch = list.slice(offset, offset + batchSize);
    offset += batchSize;
    let idx = 0;
    const run = async () => {
      while (idx < batch.length) {
        const code = batch[idx++];
        try {
          const data = await fetchBoardTrend(code);
          const points = data?.points || [];
          const entry = cache.get(code);
          if (entry && points.length >= 2) {
            entry.points = points;
            const norm = normalizeBoardTrend(points);
            if (norm.labels.length >= 2) {
              drawLineChart(entry.canvas, norm.labels, norm.values);
            }
          }
        } catch (e) {
          console.error(e);
        }
      }
    };
    const workers = [];
    for (let i = 0; i < concurrency; i++) workers.push(run());
    await Promise.all(workers);
    if (offset < list.length) {
      await new Promise(r => setTimeout(r, state.boardTrendCfg.gapMs));
    }
  }
}

async function loadHistory() {
  try {
    setPill(true, "loading...");
    const source = document.getElementById("histSource").value;
    const kind = document.getElementById("histKind").value;
    const limit = Number(document.getElementById("histLimit").value || "200");
    const fid = (state.cfg?.market_agg?.fid) || "f62";

    let rows;
    if (source.startsWith("board_")) {
      const tp = source === "board_concept_sum" ? "concept" : "industry";
      const url = `/api/history/board_sum?type=${encodeURIComponent(tp)}&fid=${encodeURIComponent(fid)}&kind=${encodeURIComponent(kind)}&limit=${encodeURIComponent(String(limit))}`;
      rows = await getJSON(url);
      state.historyMeta = { source, kind, fid, limit, boardType: tp };
    } else {
      const src = source === "market_allstocks_sum" ? "allstocks_sum" : "industry_sum";
      const url = `/api/history/market_agg?source=${encodeURIComponent(src)}&fid=${encodeURIComponent(fid)}&kind=${encodeURIComponent(kind)}&limit=${encodeURIComponent(String(limit))}`;
      rows = await getJSON(url);
      state.historyMeta = { source, kind, fid, limit, marketSource: src };
    }
    state.historyRows = rows;
    renderHistoryChart(state.historyMeta, state.historyRows);
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
        const td1 = document.createElement("td"); td1.textContent = fmtBJTime(r.ts_utc || r.TSUTC || "");
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
    const title = document.getElementById("histChartTitle");
    if (title) {
      const unit = kind === "rt" ? "条" : "天";
      title.textContent = `近 ${limit} ${unit}（折线）`;
    }
    setPill(true, "loaded");
    setTimeout(() => setPill(true, "connected"), 700);
  } catch (e) {
    console.error(e);
    setPill(false, "history error");
  }
}

function renderIndustryChartHome(rows) {
  const canvas = document.getElementById("homeIndustryChart");
  if (!canvas) return;
  const hint = document.getElementById("homeIndustryHint");
  if (hint) hint.hidden = true;
  const labels = [];
  const values = [];
  if (!rows) {
    if (hint) {
      hint.textContent = "暂无数据";
      hint.hidden = false;
    }
    return;
  }

  if (rows.mode === "daily") {
    const td = rows.tradeDate || "-";
    const v = Number(rows.value);
    if (!Number.isFinite(v)) {
      if (hint) {
        hint.textContent = "暂无数据";
        hint.hidden = false;
      }
      return;
    }
    const mmdd = td.length >= 10 ? td.slice(5, 10) : td;
    labels.push(`${mmdd} 09:30`, `${mmdd} 15:00`);
    values.push(v, v);
    drawLineChart(canvas, labels, values);
    return;
  }

  (rows.items || rows || []).forEach(r => {
    const v = Number(r.value ?? r.Value);
    if (!Number.isFinite(v)) return;
    const t = fmtBJTime(r.ts_utc || r.TSUTC || "");
    if (t === "-") return;
    const hm = Number(t.slice(11, 13)) * 60 + Number(t.slice(14, 16));
    if (hm < 9 * 60 + 30 || hm > 15 * 60) return;
    labels.push(t.slice(5, 19)); // include seconds for 30s cadence
    values.push(v);
  });
  if (labels.length === 1) {
    labels.push(labels[0]);
    values.push(values[0]);
  }
  if (labels.length === 0) {
    if (hint) {
      hint.textContent = "暂无数据";
      hint.hidden = false;
    }
    return;
  }
  drawLineChart(canvas, labels, values);
}

function renderIndexChartHome(canvasId, hintId, points) {
  const canvas = document.getElementById(canvasId);
  const hint = document.getElementById(hintId);
  if (!canvas) return;
  const s = trendToSeries(points, 9 * 60 + 30, 15 * 60, "hm");
  if (s.labels.length < 2) {
    if (hint) {
      hint.textContent = "暂无数据";
      hint.hidden = false;
    }
    return;
  }
  if (hint) hint.hidden = true;
  drawLineChart(canvas, s.labels, s.values);
}

async function loadIndexChartsHome() {
  // Use Eastmoney "secid" explicitly to avoid ambiguity with stock codes (e.g. 000001 is both stock and index).
  const load = async (secid) => {
    const data = await getJSON(`/api/secid/trend?secid=${encodeURIComponent(secid)}`);
    return data?.points || [];
  };
  try {
    const [shPts, szPts] = await Promise.all([
      load("1.000001"), // 上证指数
      load("0.399001"), // 深证成指
    ]);
    state.homeIndex = { shPts, szPts };
    renderIndexChartHome("homeSHChart", "homeSHHint", shPts);
    renderIndexChartHome("homeSZChart", "homeSZHint", szPts);
  } catch (e) {
    console.error(e);
    const showErr = (hintId) => {
      const hint = document.getElementById(hintId);
      if (!hint) return;
      hint.textContent = "加载失败";
      hint.hidden = false;
    };
    showErr("homeSHHint");
    showErr("homeSZHint");
  }
}

async function loadIndustryChartHome() {
  try {
    const fid = (state.cfg?.market_agg?.fid) || "f62";
    const loadPriceSumBackfill = async () => {
      const url = `/api/history/board_price_sum?type=industry&fid=${encodeURIComponent(fid)}&limit=1200`;
      const rows = await getJSON(url);
      if (!Array.isArray(rows) || rows.length === 0) return false;
      state.homeChartRows = { mode: "rt", items: rows };
      renderIndustryChartHome(state.homeChartRows);
      return true;
    };
    const loadFromBoards = async () => {
      const url = `/api/boards?type=industry&fid=${encodeURIComponent(fid)}&limit=500`;
      const data = await getJSON(url);
      const rows = Array.isArray(data) ? data : (data.rows || []);
      if (!rows || rows.length === 0) return false;
      let sum = 0;
      rows.forEach(r => {
        const v = Number(r.price ?? r.Price);
        if (Number.isFinite(v)) sum += v;
      });
      if (!Number.isFinite(sum)) return false;
      const item = { ts_utc: new Date().toISOString(), value: sum };
      if (!state.homeChartRows || state.homeChartRows.mode !== "rt") {
        state.homeChartRows = { mode: "rt", items: [] };
      }
      state.homeChartRows.items = (state.homeChartRows.items || []).concat([item]).slice(-800);
      renderIndustryChartHome(state.homeChartRows);
      return true;
    };
    const loadDailyLast = async () => {
      const url = `/api/history/market_agg?source=industry_sum&fid=${encodeURIComponent(fid)}&kind=daily&limit=5`;
      const rows = await getJSON(url);
      const last = (rows || []).slice(-1)[0];
      if (last) {
        state.homeChartRows = { mode: "daily", tradeDate: last.trade_date || last.TradeDate, value: last.value ?? last.Value };
        renderIndustryChartHome(state.homeChartRows);
        return true;
      }
      return false;
    };

    const loadRT = async () => {
      const url = `/api/history/market_agg?source=industry_sum&fid=${encodeURIComponent(fid)}&kind=rt&limit=800`;
      const rows = await getJSON(url);
      state.homeChartRows = { mode: "rt", items: rows };
      renderIndustryChartHome(state.homeChartRows);
      return Array.isArray(rows) && rows.length > 0;
    };

    if (isWeekendBJ() || isBefore0930BJ()) {
      if (!(await loadDailyLast())) {
        await loadRT();
      }
      return;
    }

    if (isAfterCloseBJ()) {
      if (state.homeChartRows) {
        renderIndustryChartHome(state.homeChartRows);
        return;
      }
      if (!(await loadRT())) {
        await loadDailyLast();
      }
      return;
    }

    // During trading hours, prefer live board sum to avoid stale rt snapshots.
    if (!state.homeChartRows || state.homeChartRows.mode !== "rt" || !(state.homeChartRows.items || []).length) {
      await loadPriceSumBackfill();
    }
    if (!(await loadFromBoards())) {
      await loadRT();
    }
  } catch (e) {
    console.error(e);
  }
}

async function bootRoute() {
  clearTimers();
  const route = getRoute();
  setRoute(route);
  state.trendPoints = null;
  state.homeIndex = null;

  try {
    await refreshConfig();
  } catch {
    setPill(false, "config error");
  }

  if (route === "home") {
    await refreshRealtimeOnce();
    if (!isAfterCloseBJ()) {
      state.timers.push(setInterval(refreshRealtimeOnce, 10000));
    }
    await loadIndustryChartHome();
    if (!isAfterCloseBJ()) {
      state.timers.push(setInterval(loadIndustryChartHome, 30000));
    } else {
      state.marketClosed = true;
      startMissingTrendRefresh();
    }
    await loadIndexChartsHome();
    if (!isAfterCloseBJ()) {
      state.timers.push(setInterval(loadIndexChartsHome, 30000));
    }
    await refreshBuildInfo();
  } else if (route === "industry") {
    try {
      await loadBoardsFor("industry", null, "boardHintInd");
    } catch (e) {
      console.error(e);
    }
    if (!isAfterCloseBJ()) {
      state.timers.push(setInterval(async () => {
        try {
          await loadBoardsFor("industry", null, "boardHintInd");
        } catch (e) {
          console.error(e);
        }
      }, 30000));
    } else {
      state.marketClosed = true;
      startMissingTrendRefresh();
    }
  } else if (route === "concept") {
    try {
      await loadBoardsFor("concept", null, "boardHintCon");
    } catch (e) {
      console.error(e);
    }
    if (!isAfterCloseBJ()) {
      state.timers.push(setInterval(async () => {
        try {
          await loadBoardsFor("concept", null, "boardHintCon");
        } catch (e) {
          console.error(e);
        }
      }, 30000));
    } else {
      state.marketClosed = true;
      startMissingTrendRefresh();
    }
  } else if (route === "history-industry") {
    try {
      await loadBoardDailyView("industry");
    } catch (e) {
      console.error(e);
    }
    await pollBoardDailyBatch("industry");
    if (state.batchTimers?.industry) clearInterval(state.batchTimers.industry);
    state.batchTimers.industry = setInterval(() => pollBoardDailyBatch("industry"), 3000);
  } else if (route === "history-concept") {
    try {
      await loadBoardDailyView("concept");
    } catch (e) {
      console.error(e);
    }
    await pollBoardDailyBatch("concept");
    if (state.batchTimers?.concept) clearInterval(state.batchTimers.concept);
    state.batchTimers.concept = setInterval(() => pollBoardDailyBatch("concept"), 3000);
  } else if (route === "history") {
    await loadHistory();
  } else if (route === "trend") {
    const q = getRouteQuery();
    const code = q.board ? String(q.board).trim() : "";
    if (code) {
      await loadTrendView(code);
    } else if (document.getElementById("trendHint")) {
      document.getElementById("trendHint").textContent = "请输入板块代码";
      document.getElementById("trendHint").hidden = false;
    }
  } else if (route === "settings") {
    // Don't auto-refresh config here; it would overwrite unsaved UI edits.
  }
}

wire();
bootRoute();
