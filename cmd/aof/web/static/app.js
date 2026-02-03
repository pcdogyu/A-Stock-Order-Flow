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
  document.getElementById("aggInterval").value = cfg.market_agg?.interval_seconds ?? 120;
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

function colorVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name);
  return (v && v.trim()) ? v.trim() : fallback;
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

  const bg = colorVar("--bg0", "#0b0e14");
  const stroke = colorVar("--stroke", "#223042");
  const muted = colorVar("--muted", "#a8b4c6");
  const accent = colorVar("--accent", "#46d6a3");

  ctx.clearRect(0, 0, parentW, parentH);
  ctx.fillStyle = "#0b1018";
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

function renderHistoryChart(meta, rows) {
  const panel = document.getElementById("histChartPanel");
  const canvas = document.getElementById("histChart");
  if (!panel || !canvas) return;

  const source = meta?.source || "";
  const kind = meta?.kind || "daily";
  const show = source === "industry_sum";
  panel.hidden = !show;
  if (!show) return;

  const labels = [];
  const values = [];
  (rows || []).forEach(r => {
    const v = Number(r.value ?? r.Value);
    if (!Number.isFinite(v)) return;
    if (kind === "rt") {
      const t = fmtBJTime(r.ts_utc || r.TSUTC || "");
      labels.push(t === "-" ? "-" : t.slice(5, 16)); // "MM-DD HH:mm"
      values.push(v);
    } else {
      labels.push(r.trade_date || r.TradeDate || "-");
      values.push(v);
    }
  });

  drawLineChart(canvas, labels, values);
}

function clearCanvas(canvas) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 600;
  const h = canvas.clientHeight || 160;
  canvas.width = Math.floor(w * dpr);
  canvas.height = Math.floor(h * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
}

function drawSparkline(canvas, labels, values) {
  if (!canvas) return;
  const parentW = canvas.clientWidth || 600;
  const parentH = canvas.clientHeight || 160;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(parentW * dpr);
  canvas.height = Math.floor(parentH * dpr);

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  const stroke = colorVar("--stroke", "#223042");
  const muted = colorVar("--muted", "#a8b4c6");
  const accent = colorVar("--accent", "#46d6a3");

  ctx.clearRect(0, 0, parentW, parentH);
  ctx.fillStyle = "#0b1018";
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

  const padL = 10, padR = 10, padT = 10, padB = 22;
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

  ctx.fillStyle = muted;
  ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
  ctx.textAlign = "left";
  ctx.fillText(String(labels[0] ?? ""), padL, parentH - 8);
  ctx.textAlign = "right";
  ctx.fillText(String(labels[n - 1] ?? ""), parentW - padR, parentH - 8);
  ctx.textAlign = "left";
}

async function loadSparkForBoard(type, boardCode, boardName) {
  const canvasId = type === "industry" ? "sparkInd" : "sparkCon";
  const titleId = type === "industry" ? "sparkTitleInd" : "sparkTitleCon";
  const canvas = document.getElementById(canvasId);
  const title = document.getElementById(titleId);
  if (!canvas || !title) return;

  if (!boardCode) {
    title.textContent = "成份股走势：-";
    clearCanvas(canvas);
    return;
  }

  title.textContent = "成份股走势：loading...";
  try {
    const data = await getJSON(`/api/board/constituents?board=${encodeURIComponent(boardCode)}&pn=1&pz=1`);
    const stock = (data?.rows || [])[0];
    if (!stock || !(stock.code || stock.Code)) {
      title.textContent = "成份股走势：-";
      clearCanvas(canvas);
      return;
    }
    const code = stock.code || stock.Code;
    const name = stock.name || stock.Name || "";
    const pct = stock.pct ?? stock.Pct;
    const trend = await getJSON(`/api/stock/trend?code=${encodeURIComponent(code)}`);
    const pts = trend?.points || [];
    if (!Array.isArray(pts) || pts.length < 2) {
      title.textContent = `成份股走势（${boardName || boardCode}）：${name} ${code}（无数据）`;
      clearCanvas(canvas);
      return;
    }

    const labels = pts.map(p => String(p.ts || p.TS || "").slice(11, 16)); // "HH:mm"
    const values = pts.map(p => Number(p.price ?? p.Price));
    const pctTxt = Number.isFinite(Number(pct)) ? ` ${(Number(pct)).toFixed(2)}%` : "";
    title.textContent = `成份股走势（${boardName || boardCode}）：${name} ${code}${pctTxt}`;
    drawSparkline(canvas, labels, values);

    state.sparkByType[type] = { board: boardCode, stock: { code, name }, points: pts };
  } catch (e) {
    console.error(e);
    title.textContent = `成份股走势（${boardName || boardCode}）：加载失败`;
    clearCanvas(canvas);
  }
}

function renderSparkFromCache(type) {
  const canvasId = type === "industry" ? "sparkInd" : "sparkCon";
  const canvas = document.getElementById(canvasId);
  const cached = state.sparkByType?.[type];
  if (!canvas || !cached || !Array.isArray(cached.points) || cached.points.length < 2) return;
  const labels = cached.points.map(p => String(p.ts || p.TS || "").slice(11, 16));
  const values = cached.points.map(p => Number(p.price ?? p.Price));
  drawSparkline(canvas, labels, values);
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
  const nbHint = document.getElementById("nbHint");
  if (!nb) {
    setText("nbSh", "-");
    setText("nbSz", "-");
    if (nbHint) nbHint.hidden = true;
  } else {
    const sh = nb.SH?.NetBuyAmt ?? nb.sh?.netBuyAmt ?? nb.sh?.net_buy_amt;
    const sz = nb.SZ?.NetBuyAmt ?? nb.sz?.netBuyAmt ?? nb.sz?.net_buy_amt;
    const shBuy = nb.SH?.BuyAmt ?? nb.sh?.buyAmt ?? nb.sh?.buy_amt;
    const shSell = nb.SH?.SellAmt ?? nb.sh?.sellAmt ?? nb.sh?.sell_amt;
    const szBuy = nb.SZ?.BuyAmt ?? nb.sz?.buyAmt ?? nb.sz?.buy_amt;
    const szSell = nb.SZ?.SellAmt ?? nb.sz?.sellAmt ?? nb.sz?.sell_amt;
    const looksWithheld = (v) => (v === 0 || v === "0" || v === "0.0");
    const withheld =
      looksWithheld(sh) && looksWithheld(sz) &&
      looksWithheld(shBuy) && looksWithheld(shSell) &&
      looksWithheld(szBuy) && looksWithheld(szSell);
    setText("nbSh", withheld ? "-" : fmtMoney(Number(sh)));
    setText("nbSz", withheld ? "-" : fmtMoney(Number(sz)));
    if (nbHint) nbHint.hidden = !withheld;
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
  if (h === "history" || h === "settings" || h === "home") return h;
  return "home";
}

let state = {
  cfg: null,
  timers: [],
  boardType: "industry",
  boardCode: null,
  boardPN: 1,
  boardPZ: 30,
  historyMeta: null,
  historyRows: null,
  sparkByType: {
    industry: { board: null, stock: null, points: null },
    concept: { board: null, stock: null, points: null },
  },
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
  window.addEventListener("hashchange", () => bootRoute());
  window.addEventListener("resize", () => {
    const route = getRoute();
    if (route === "history") {
      if (state.historyMeta && state.historyRows) {
        renderHistoryChart(state.historyMeta, state.historyRows);
      }
      return;
    }
    if (route === "home") {
      renderSparkFromCache("industry");
      renderSparkFromCache("concept");
    }
  });

  document.getElementById("btnPrev")?.addEventListener("click", async () => {
    if (!state.boardCode) return;
    if (state.boardPN > 1) state.boardPN--;
    try { await loadConstituents(); } catch (e) { console.error(e); }
  });
  document.getElementById("btnNext")?.addEventListener("click", async () => {
    if (!state.boardCode) return;
    state.boardPN++;
    try { await loadConstituents(); } catch (e) { console.error(e); }
  });

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

function setBoardType(tp) {
  state.boardType = tp;
  state.boardCode = null;
  state.boardPN = 1;
  const sel = document.getElementById("boardSel");
  if (sel) sel.textContent = "未选择";
  const tbody = document.querySelector("#tblCon tbody");
  if (tbody) tbody.innerHTML = "";
}

async function loadBoardsFor(type, listId, hintId) {
  const fid = (type === "concept" ? state.cfg?.concept?.fid : state.cfg?.industry?.fid) || "f62";
  const url = `/api/boards?type=${encodeURIComponent(type)}&fid=${encodeURIComponent(fid)}&limit=50`;
  const data = await getJSON(url);
  const boards = Array.isArray(data) ? data : (data.rows || []);
  const boardHint = document.getElementById(hintId);
  if (boardHint) {
    const show = !Array.isArray(data) && !!data.from_live;
    boardHint.hidden = !show;
    if (show) {
      boardHint.textContent = "市场休市或未抓到快照，已即时拉取板块数据。";
    }
  }
  const list = document.getElementById(listId);
  if (!list) return;
  list.innerHTML = "";
  boards.forEach(b => {
    const div = document.createElement("div");
    div.className = "item";
    div.dataset.code = b.code;
    div.innerHTML = `<div class="name">${b.name} <span class="code">${b.code}</span></div><div class="val">${fmtMoney(b.value)}</div>`;
    div.addEventListener("click", async () => {
      list.querySelectorAll(".item").forEach(x => x.classList.remove("active"));
      div.classList.add("active");
      state.boardType = type;
      state.boardCode = b.code;
      state.boardPN = 1;
      const sel = document.getElementById("boardSel");
      if (sel) sel.textContent = `${b.name} (${b.code})`;
      try { await loadSparkForBoard(type, b.code, b.name); } catch {}
      await loadConstituents();
    });
    list.appendChild(div);
  });

  // Auto-render a sparkline for the first board in each list.
  if (boards.length > 0 && !state.sparkByType?.[type]?.board) {
    const first = boards[0];
    if (first && first.code) {
      const firstEl = list.querySelector(".item");
      if (firstEl) firstEl.classList.add("active");
      await loadSparkForBoard(type, first.code, first.name);
    }
  }
}

async function loadConstituents() {
  if (!state.boardCode) return;
  const url = `/api/board/constituents?board=${encodeURIComponent(state.boardCode)}&pn=${encodeURIComponent(String(state.boardPN))}&pz=${encodeURIComponent(String(state.boardPZ))}`;
  const data = await getJSON(url);
  const rows = data.rows || [];
  const tbody = document.querySelector("#tblCon tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  rows.forEach(r => {
    const tr = document.createElement("tr");
    const td = (t, cls) => { const x=document.createElement("td"); if(cls) x.className=cls; x.textContent=t; return x; };
    tr.appendChild(td(r.code));
    tr.appendChild(td(r.name || "-"));
    tr.appendChild(td(String(r.price ?? "-"), "num"));
    tr.appendChild(td((r.pct ?? "-") + (Number.isFinite(r.pct) ? "%" : ""), "num"));
    tr.appendChild(td(String(r.open ?? "-"), "num"));
    tr.appendChild(td(String(r.high ?? "-"), "num"));
    tr.appendChild(td(String(r.low ?? "-"), "num"));
    tbody.appendChild(tr);
  });
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
    state.historyMeta = { source, kind, fid, limit };
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
    try {
      setBoardType(state.boardType);
      await Promise.all([
        loadBoardsFor("industry", "boardListInd", "boardHintInd"),
        loadBoardsFor("concept", "boardListCon", "boardHintCon"),
      ]);
    } catch (e) {
      console.error(e);
    }
    await refreshBuildInfo();
  } else if (route === "history") {
    await loadHistory();
  } else if (route === "settings") {
    // Don't auto-refresh config here; it would overwrite unsaved UI edits.
  }
}

wire();
bootRoute();
