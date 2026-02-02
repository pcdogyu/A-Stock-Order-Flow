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
  return text
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(Boolean);
}

function fill(cfg) {
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

async function refresh() {
  try {
    setPill(true, "loading...");
    const cfg = await getJSON("/api/config");
    fill(cfg);
    setPill(true, "connected");
  } catch (e) {
    console.error(e);
    setPill(false, "error");
  }
}

function wire() {
  document.getElementById("formRealtime").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      realtime_interval_seconds: Number(document.getElementById("rtInterval").value),
      only_during_trading_hours: document.getElementById("onlyTradeHours").checked,
    };
    try {
      setPill(true, "saving...");
      const cfg = await postJSON("/api/config", payload);
      fill(cfg);
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
      fill(cfg);
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
      fill(cfg);
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
      fill(cfg);
      setPill(true, "saved");
      setTimeout(() => setPill(true, "connected"), 700);
    } catch (e) {
      console.error(e);
      setPill(false, "save failed");
    }
  });
}

wire();
refresh();
setInterval(refresh, 5000);

