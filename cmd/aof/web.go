package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/market"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/memstore"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/runtimecfg"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/store/sqlite"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/symbol"
)

//go:embed web/static/*
var webFS embed.FS

func newWebServer(mgr *runtimecfg.Manager, db *sql.DB, mem *memstore.Store) http.Handler {
	if mem == nil {
		mem = memstore.New()
	}
	em := eastmoney.NewClient()
	seedMemFromDB(db, mem)
	var rtMu sync.Mutex
	var rtCache memstore.Snapshot
	var rtCacheAt time.Time
	boardCache := newBoardCache()
	boardTrendCache := newBoardTrendCache()
	lastCommit := resolveLastCommitTime()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"last_commit_time": lastCommit})
	})

	mux.HandleFunc("/api/realtime", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rtMu.Lock()
		useCache := time.Since(rtCacheAt) < 10*time.Second && !isSnapshotEmpty(rtCache)
		snap := rtCache
		rtMu.Unlock()

		if !useCache {
			if dbSnap, ok, err := sqlite.LoadLatestRTSnapshot(db); err == nil && ok {
				ts, err := time.Parse(time.RFC3339Nano, dbSnap.TSUTC)
				if err != nil {
					ts = time.Now().UTC()
				}
				snap = memstore.Snapshot{
					TSUTC:        ts,
					Northbound:   dbSnap.Northbound,
					Fundflow:     dbSnap.Fundflow,
					ToplistByFID: dbSnap.ToplistByFID,
					BoardsByKey:  dbSnap.BoardsByKey,
					AggByKey:     dbSnap.AggByKey,
				}
				rtMu.Lock()
				rtCache = snap
				rtCacheAt = time.Now()
				rtMu.Unlock()
			} else {
				snap = mem.SnapshotLatest()
			}
		}
		writeJSON(w, http.StatusOK, snap)
	})

	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, toConfigView(mgr.Get()))
		case http.MethodPost:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			var p runtimecfg.Patch
			if err := json.Unmarshal(body, &p); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			cfg, err := mgr.Update(p)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, toConfigView(cfg))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/history/market_agg", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		kind := r.URL.Query().Get("kind")
		if kind == "" {
			kind = "daily"
		}
		source := r.URL.Query().Get("source")
		fid := r.URL.Query().Get("fid")
		if source == "" || fid == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "source and fid are required"})
			return
		}
		limit := parseLimit(r.URL.Query().Get("limit"), 200, 2000)
		if kind == "rt" {
			rows, err := sqlite.QueryMarketAggRT(db, source, fid, limit)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, rows)
			return
		}
		rows, err := sqlite.QueryMarketAggDaily(db, source, fid, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rows)
	})

	mux.HandleFunc("/api/history/board_sum", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		kind := r.URL.Query().Get("kind")
		if kind == "" {
			kind = "daily"
		}
		tp := r.URL.Query().Get("type")
		if tp == "" {
			tp = "industry"
		}
		if tp != "industry" && tp != "concept" {
			tp = "industry"
		}
		fid := r.URL.Query().Get("fid")
		if fid == "" {
			fid = "f62"
		}
		limit := parseLimit(r.URL.Query().Get("limit"), 200, 2000)
		if kind == "rt" {
			rows, err := sqlite.QueryBoardSumRT(db, tp, fid, limit)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, rows)
			return
		}
		rows, err := sqlite.QueryBoardSumDaily(db, tp, fid, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rows)
	})

	// Board list from in-memory snapshot:
	// GET /api/boards?type=industry|concept&fid=f62&limit=50
	mux.HandleFunc("/api/boards", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		tp := r.URL.Query().Get("type")
		if tp == "" {
			tp = "industry"
		}
		if tp != "industry" && tp != "concept" {
			tp = "industry"
		}
		fid := r.URL.Query().Get("fid")
		limit := parseLimit(r.URL.Query().Get("limit"), 50, 200)

		cfg := mgr.Get()
		var bcfg config.BoardConfig
		if tp == "concept" {
			bcfg = cfg.Concept
		} else {
			bcfg = cfg.Industry
		}
		if fid == "" {
			if bcfg.FID != "" {
				fid = bcfg.FID
			} else {
				fid = "f62"
			}
		}

		rows := []eastmoney.TopItem(nil)
		if !market.IsCNTradingTime(time.Now()) {
			if _, dbRows, err := sqlite.QueryBoardRTLatest(db, tp, fid, limit); err == nil && len(dbRows) > 0 {
				rows = dbRows
			}
		}
		if len(rows) == 0 {
			snap := mem.SnapshotLatest()
			key := tp + ":" + fid
			rows = snap.BoardsByKey[key]
		}
		if len(rows) == 0 {
			if _, dbRows, err := sqlite.QueryBoardRTLatest(db, tp, fid, limit); err == nil && len(dbRows) > 0 {
				rows = dbRows
			}
		}
		fromLive := false
		if len(rows) == 0 && bcfg.Enabled && bcfg.FS != "" {
			var items []eastmoney.TopItem
			var err error
			if tp == "concept" && !bcfg.CollectAll {
				items, err = em.BoardListTop(r.Context(), bcfg.FS, fid, bcfg.TopSize)
			} else {
				items, err = em.BoardListAll(r.Context(), bcfg.FS, fid)
			}
			if err == nil && len(items) > 0 {
				ts := time.Now().UTC()
				mem.SetBoard(ts, tp, fid, items)
				if tp == "industry" {
					var sum float64
					for _, it := range items {
						sum += it.Value
					}
					mem.SetAgg(ts, "industry_sum", fid, sum)
				}
				rows = items
				fromLive = true
			}
		}
		if len(rows) > limit {
			rows = rows[:limit]
		}
		type boardInfo struct {
			Code  string  `json:"code"`
			Name  string  `json:"name"`
			Value float64 `json:"value"`
			Pct   float64 `json:"pct"`
			Price float64 `json:"price"`
		}
		out := make([]boardInfo, 0, len(rows))
		for _, it := range rows {
			out = append(out, boardInfo{Code: it.Code, Name: it.Name, Value: it.Value, Pct: it.Pct, Price: it.Price})
		}
		writeJSON(w, http.StatusOK, map[string]any{"rows": out, "from_live": fromLive})
	})

	// Board constituents (stocks with today's move):
	// GET /api/board/constituents?board=BK0457&pn=1&pz=50
	mux.HandleFunc("/api/board/constituents", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		board := r.URL.Query().Get("board")
		if board == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "board is required (e.g. BK0457)"})
			return
		}
		pn := parseLimit(r.URL.Query().Get("pn"), 1, 1000000)
		pz := parseLimit(r.URL.Query().Get("pz"), 50, 100)
		total, rows, err := em.BoardConstituents(r.Context(), board, pn, pz)
		if err != nil {
			select {
			case <-r.Context().Done():
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": r.Context().Err().Error()})
				return
			case <-time.After(300 * time.Millisecond):
			}
			total, rows, err = em.BoardConstituents(r.Context(), board, pn, pz)
		}
		if err != nil {
			if cached, ok := boardCache.Get(board, pn, pz); ok {
				writeJSON(w, http.StatusOK, map[string]any{
					"total":  cached.total,
					"rows":   cached.rows,
					"ts_utc": cached.tsUTC,
					"cached": true,
					"error":  err.Error(),
				})
				return
			}
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error":   err.Error(),
				"board":   board,
				"pn":      pn,
				"pz":      pz,
				"ts_utc":  time.Now().UTC(),
				"message": "board constituents fetch failed",
			})
			return
		}
		boardCache.Set(board, pn, pz, total, rows)
		writeJSON(w, http.StatusOK, map[string]any{"total": total, "rows": rows, "ts_utc": time.Now().UTC()})
	})

	// Board intraday trend (today):
	// GET /api/board/trend?board=BK0457
	mux.HandleFunc("/api/board/trend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		board := strings.TrimSpace(r.URL.Query().Get("board"))
		if board == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "board is required (e.g. BK0457)"})
			return
		}
		if cached, ok := boardTrendCache.Get(board); ok {
			writeJSON(w, http.StatusOK, map[string]any{"board": board, "points": cached.points, "ts_utc": cached.tsUTC, "cached": true})
			return
		}
		points, err := em.BoardTrends1D(r.Context(), board)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "board": board})
			return
		}
		boardTrendCache.Set(board, points)
		writeJSON(w, http.StatusOK, map[string]any{"board": board, "points": points, "ts_utc": time.Now().UTC()})
	})

	// Stock intraday trend (today):
	// GET /api/stock/trend?code=600519
	mux.HandleFunc("/api/stock/trend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		code := strings.TrimSpace(r.URL.Query().Get("code"))
		if code == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "code is required (e.g. 600519)"})
			return
		}
		secid, err := symbol.ToEastmoneySecIDFromCode(code)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		points, err := em.StockTrends1D(r.Context(), secid)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": code, "secid": secid, "points": points, "ts_utc": time.Now().UTC()})
	})

	// Static UI.
	sub, _ := fs.Sub(webFS, "web/static")
	fileServer := http.FileServer(http.FS(sub))
	mux.Handle("/static/", http.StripPrefix("/static/", noCache(fileServer)))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			// Let fileServer try to serve embedded file if it exists.
			// Prevent directory traversal: only allow /static/* here.
			if strings.HasPrefix(r.URL.Path, "/static/") {
				http.StripPrefix("/static/", noCache(fileServer)).ServeHTTP(w, r)
				return
			}
			http.NotFound(w, r)
			return
		}
		b, err := webFS.ReadFile("web/static/index.html")
		if err != nil {
			http.Error(w, "ui not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(b)
	})

	return logRequests(mux)
}

func seedMemFromDB(db *sql.DB, mem *memstore.Store) {
	if db == nil || mem == nil {
		return
	}
	snap, ok, err := sqlite.LoadLatestRTSnapshot(db)
	if err != nil || !ok {
		return
	}
	ts, err := time.Parse(time.RFC3339Nano, snap.TSUTC)
	if err != nil {
		ts = time.Now().UTC()
	}
	if snap.Northbound != nil {
		mem.SetNorthbound(ts, *snap.Northbound)
	}
	if len(snap.Fundflow) > 0 {
		mem.SetFundflow(ts, snap.Fundflow)
	}
	for fid, rows := range snap.ToplistByFID {
		mem.SetToplist(ts, fid, rows)
	}
	for key, rows := range snap.BoardsByKey {
		if bt, fid, ok := split2Key(key); ok {
			mem.SetBoard(ts, bt, fid, rows)
		}
	}
	for key, v := range snap.AggByKey {
		if source, fid, ok := split2Key(key); ok {
			mem.SetAgg(ts, source, fid, v)
		}
	}
}

func split2Key(s string) (a, b string, ok bool) {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return "", "", false
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func isSnapshotEmpty(s memstore.Snapshot) bool {
	return s.Northbound == nil &&
		len(s.Fundflow) == 0 &&
		len(s.ToplistByFID) == 0 &&
		len(s.BoardsByKey) == 0 &&
		len(s.AggByKey) == 0
}

func resolveLastCommitTime() string {
	cmd := exec.Command("git", "log", "-1", "--format=%cd", "--date=format-local:%Y-%m-%d %H:%M")
	cmd.Dir = resolveRepoRoot()
	cmd.Env = append(os.Environ(), "TZ=Asia/Shanghai")
	out, err := cmd.Output()
	if err != nil {
		return "-"
	}
	s := strings.TrimSpace(string(out))
	if s == "" {
		return "-"
	}
	return s
}

func resolveRepoRoot() string {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err == nil {
		if s := strings.TrimSpace(string(out)); s != "" {
			return s
		}
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

type boardCache struct {
	mu    sync.RWMutex
	byKey map[string]cachedBoard
}

type cachedBoard struct {
	total int
	rows  []eastmoney.QuoteItem
	tsUTC time.Time
}

func newBoardCache() *boardCache {
	return &boardCache{byKey: make(map[string]cachedBoard)}
}

func (c *boardCache) key(board string, pn, pz int) string {
	return fmt.Sprintf("%s:%d:%d", board, pn, pz)
}

func (c *boardCache) Set(board string, pn, pz, total int, rows []eastmoney.QuoteItem) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byKey[c.key(board, pn, pz)] = cachedBoard{
		total: total,
		rows:  append([]eastmoney.QuoteItem(nil), rows...),
		tsUTC: time.Now().UTC(),
	}
}

func (c *boardCache) Get(board string, pn, pz int) (cachedBoard, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.byKey[c.key(board, pn, pz)]
	return v, ok
}

type boardTrendCache struct {
	mu    sync.RWMutex
	byKey map[string]cachedBoardTrend
}

type cachedBoardTrend struct {
	points []eastmoney.TrendPoint
	tsUTC  time.Time
}

func newBoardTrendCache() *boardTrendCache {
	return &boardTrendCache{byKey: make(map[string]cachedBoardTrend)}
}

func (c *boardTrendCache) Set(board string, points []eastmoney.TrendPoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byKey[board] = cachedBoardTrend{
		points: append([]eastmoney.TrendPoint(nil), points...),
		tsUTC:  time.Now().UTC(),
	}
}

func (c *boardTrendCache) Get(board string) (cachedBoardTrend, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.byKey[board]
	if !ok {
		return cachedBoardTrend{}, false
	}
	if time.Since(v.tsUTC) > 25*time.Second {
		return cachedBoardTrend{}, false
	}
	return v, true
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

type configView struct {
	DBPath    string   `json:"db_path"`
	Watchlist []string `json:"watchlist"`

	Realtime struct {
		IntervalSeconds        int  `json:"interval_seconds"`
		OnlyDuringTradingHours bool `json:"only_during_trading_hours"`
	} `json:"realtime"`

	Toplist struct {
		Size int    `json:"size"`
		FS   string `json:"fs"`
		FID  string `json:"fid"`
	} `json:"toplist"`

	Industry   config.BoardConfig      `json:"industry"`
	Concept    config.BoardConfig      `json:"concept"`
	MarketAgg  config.MarketAggConfig  `json:"market_agg"`
	BoardTrend config.BoardTrendConfig `json:"board_trend"`
}

func toConfigView(cfg config.Config) configView {
	var v configView
	v.DBPath = cfg.DBPath
	v.Watchlist = cfg.Watchlist
	v.Realtime.IntervalSeconds = cfg.Realtime.IntervalSeconds
	if cfg.Realtime.OnlyDuringHours != nil {
		v.Realtime.OnlyDuringTradingHours = *cfg.Realtime.OnlyDuringHours
	} else {
		v.Realtime.OnlyDuringTradingHours = true
	}
	v.Toplist.Size = cfg.Toplist.Size
	v.Toplist.FS = cfg.Toplist.FS
	v.Toplist.FID = cfg.Toplist.FID
	v.Industry = cfg.Industry
	v.Concept = cfg.Concept
	v.MarketAgg = cfg.MarketAgg
	v.BoardTrend = cfg.BoardTrend
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func parseLimit(s string, def, max int) int {
	if s == "" {
		return def
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}
