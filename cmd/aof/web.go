package main

import (
	"context"
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
	secidTrendCache := newBoardTrendCache()
	boardDailyBatch := newBoardDailyBatch()
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

	// Board price sum (intraday, from board_rt.price):
	// GET /api/history/board_price_sum?type=industry|concept&fid=f62&limit=1200
	mux.HandleFunc("/api/history/board_price_sum", func(w http.ResponseWriter, r *http.Request) {
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
		if fid == "" {
			fid = "f62"
		}
		limit := parseLimit(r.URL.Query().Get("limit"), 1200, 5000)

		loc, _ := time.LoadLocation("Asia/Shanghai")
		now := time.Now().In(loc)
		start := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, loc).UTC()
		end := time.Date(now.Year(), now.Month(), now.Day(), 15, 0, 0, 0, loc).UTC()
		rows, err := sqlite.QueryBoardPriceSumRT(db, tp, fid, sqlite.FixedRFC3339Nano(start), sqlite.FixedRFC3339Nano(end), limit)
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
		refresh := r.URL.Query().Get("refresh") == "1" || strings.EqualFold(r.URL.Query().Get("refresh"), "true")
		now := time.Now()
		interval := time.Duration(bcfg.IntervalSeconds) * time.Second
		if interval <= 0 {
			interval = 60 * time.Second
		}
		stale := true
		if ts := mem.BoardTS(tp, fid); !ts.IsZero() {
			if now.Sub(ts) <= 2*interval {
				stale = false
			}
		}

		rows := []eastmoney.TopItem(nil)
		if !market.IsCNTradingTime(now) {
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
		allowLive := bcfg.Enabled && bcfg.FS != "" && (refresh || market.IsCNTradingTime(now))
		if (len(rows) == 0 || stale) && allowLive {
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
						sum += it.Price
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

	// Board daily history (fundflow):
	// GET /api/board/daily?board=BK0457&type=industry&fid=f62&limit=90&refresh=1
	mux.HandleFunc("/api/board/daily", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		board := strings.TrimSpace(r.URL.Query().Get("board"))
		if board == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "board is required (e.g. BK0457)"})
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
		limit := parseLimit(r.URL.Query().Get("limit"), 120, 2000)
		refresh := r.URL.Query().Get("refresh")

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

		points, name, err := sqlite.QueryBoardDailyByCode(db, tp, fid, board, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		needFetch := len(points) < limit || refresh == "1" || refresh == "true"
		var fetchErr error
		if needFetch {
			rows, err := em.BoardFundflowDailySeries(r.Context(), board, limit)
			if err != nil {
				fetchErr = err
			} else if len(rows) > 0 {
				series := make([]sqlite.BoardDailySeriesPoint, 0, len(rows))
				for _, row := range rows {
					code := row.Code
					if code == "" {
						code = board
					}
					series = append(series, sqlite.BoardDailySeriesPoint{
						TradeDate: row.TradeDate,
						Code:      code,
						Name:      row.Name,
						Value:     row.NetMain,
						Price:     0,
						Pct:       0,
					})
					if name == "" && row.Name != "" {
						name = row.Name
					}
				}
				if err := sqlite.UpsertBoardDailySeries(db, tp, fid, series); err != nil {
					fetchErr = err
				}
			}
			points, name, _ = sqlite.QueryBoardDailyByCode(db, tp, fid, board, limit)
		}

		out := map[string]any{
			"board":  board,
			"name":   name,
			"points": points,
			"fid":    fid,
			"type":   tp,
		}
		if fetchErr != nil {
			out["error"] = fetchErr.Error()
		}
		writeJSON(w, http.StatusOK, out)
	})

	// Batch load board daily history into SQLite (long running).
	// POST /api/board/daily/batch?type=concept&limit=360
	// GET  /api/board/daily/batch?type=concept
	mux.HandleFunc("/api/board/daily/batch", func(w http.ResponseWriter, r *http.Request) {
		tp := r.URL.Query().Get("type")
		if tp == "" {
			tp = "concept"
		}
		if tp != "industry" && tp != "concept" {
			tp = "concept"
		}
		limit := parseLimit(r.URL.Query().Get("limit"), 360, 2000)

		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, boardDailyBatch.Status(tp))
			return
		case http.MethodPost:
			cfg := mgr.Get()
			var bcfg config.BoardConfig
			if tp == "concept" {
				bcfg = cfg.Concept
			} else {
				bcfg = cfg.Industry
			}
			if bcfg.FS == "" {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "board fs not configured"})
				return
			}
			fid := bcfg.FID
			if fid == "" {
				fid = "f62"
			}
			ok := boardDailyBatch.Start(tp, func(job *boardDailyJob) {
				runBoardDailyBatch(job, em, db, tp, bcfg.FS, fid, limit)
			})
			if !ok {
				writeJSON(w, http.StatusConflict, map[string]any{"error": "batch already running"})
				return
			}
			writeJSON(w, http.StatusAccepted, boardDailyBatch.Status(tp))
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
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
		// Prefer SQLite intraday series (from board_rt) to avoid flaky external endpoints.
		loc, _ := time.LoadLocation("Asia/Shanghai")
		now := time.Now().In(loc)
		trendDay := now
		hm := now.Hour()*60 + now.Minute()
		if hm < 9*60+30 {
			if ts, err := sqlite.QueryBoardRTLatestTimestampByCode(db, board); err == nil && ts != "" {
				if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					trendDay = t.In(loc)
				}
			}
		}
		start := time.Date(trendDay.Year(), trendDay.Month(), trendDay.Day(), 9, 30, 0, 0, loc).UTC()
		end := time.Date(trendDay.Year(), trendDay.Month(), trendDay.Day(), 15, 0, 0, 0, loc).UTC()
		rows, err := sqlite.QueryBoardRTSeriesByCode(db, board, sqlite.FixedRFC3339Nano(start), sqlite.FixedRFC3339Nano(end), 1200)
		if err == nil && len(rows) >= 2 {
			// Some board types (notably concept) may have a stale/flat "price" in clist snapshots.
			// If the intraday series looks stale (flat or too few distinct values), prefer the dedicated trend endpoint.
			minV, maxV := rows[0].Value, rows[0].Value
			distinct := make(map[int64]struct{}, 64)
			flatRun, maxFlatRun := 1, 1
			last := rows[0].Value
			// Quantize to 1e-4 to avoid floating noise.
			distinct[int64(last*1e4+0.5)] = struct{}{}
			for i := 1; i < len(rows); i++ {
				v := rows[i].Value
				if v < minV {
					minV = v
				}
				if v > maxV {
					maxV = v
				}
				distinct[int64(v*1e4+0.5)] = struct{}{}
				if v == last {
					flatRun++
					if flatRun > maxFlatRun {
						maxFlatRun = flatRun
					}
				} else {
					flatRun = 1
					last = v
				}
			}
			// Heuristics for "stale": essentially flat, or step function with very few distinct values,
			// or an excessively long flat run (e.g. clist snapshots not updating).
			if (maxV-minV) < 1e-9 || len(distinct) <= 6 || maxFlatRun >= 30 {
				goto fetchFromRemote
			}
			points := make([]eastmoney.TrendPoint, 0, len(rows))
			for _, r := range rows {
				if ts, err := time.Parse(time.RFC3339Nano, r.TSUTC); err == nil {
					points = append(points, eastmoney.TrendPoint{TS: ts.In(loc).Format("2006-01-02 15:04:05"), Price: r.Value})
				}
			}
			boardTrendCache.Set(board, points)
			writeJSON(w, http.StatusOK, map[string]any{"board": board, "points": points, "ts_utc": time.Now().UTC()})
			return
		}

	fetchFromRemote:
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

	// SecID intraday trend (today):
	// GET /api/secid/trend?secid=1.000001
	mux.HandleFunc("/api/secid/trend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		secid := strings.TrimSpace(r.URL.Query().Get("secid"))
		if secid == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "secid is required (e.g. 1.000001)"})
			return
		}
		if cached, ok := secidTrendCache.Get(secid); ok {
			writeJSON(w, http.StatusOK, map[string]any{"secid": secid, "points": cached.points, "ts_utc": cached.tsUTC, "cached": true})
			return
		}
		points, err := em.StockTrends1D(r.Context(), secid)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "secid": secid})
			return
		}
		secidTrendCache.Set(secid, points)
		writeJSON(w, http.StatusOK, map[string]any{"secid": secid, "points": points, "ts_utc": time.Now().UTC()})
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

type boardDailyBatch struct {
	mu   sync.Mutex
	jobs map[string]*boardDailyJob
}

type boardDailyJob struct {
	mu        sync.Mutex
	running   bool
	startedAt time.Time
	updatedAt time.Time
	total     int
	ok        int
	failed    int
	lastErr   string
}

type boardDailyStatus struct {
	Type      string `json:"type"`
	Running   bool   `json:"running"`
	StartedAt string `json:"started_at"`
	UpdatedAt string `json:"updated_at"`
	Total     int    `json:"total"`
	Ok        int    `json:"ok"`
	Failed    int    `json:"failed"`
	LastErr   string `json:"last_err,omitempty"`
}

func newBoardDailyBatch() *boardDailyBatch {
	return &boardDailyBatch{jobs: make(map[string]*boardDailyJob)}
}

func (b *boardDailyBatch) get(tp string) *boardDailyJob {
	b.mu.Lock()
	defer b.mu.Unlock()
	job := b.jobs[tp]
	if job == nil {
		job = &boardDailyJob{}
		b.jobs[tp] = job
	}
	return job
}

func (b *boardDailyBatch) Start(tp string, run func(job *boardDailyJob)) bool {
	job := b.get(tp)
	job.mu.Lock()
	if job.running {
		job.mu.Unlock()
		return false
	}
	job.running = true
	job.startedAt = time.Now().UTC()
	job.updatedAt = job.startedAt
	job.total = 0
	job.ok = 0
	job.failed = 0
	job.lastErr = ""
	job.mu.Unlock()

	go run(job)
	return true
}

func (b *boardDailyBatch) Status(tp string) boardDailyStatus {
	job := b.get(tp)
	job.mu.Lock()
	defer job.mu.Unlock()
	return boardDailyStatus{
		Type:      tp,
		Running:   job.running,
		StartedAt: formatRFC3339Nano(job.startedAt),
		UpdatedAt: formatRFC3339Nano(job.updatedAt),
		Total:     job.total,
		Ok:        job.ok,
		Failed:    job.failed,
		LastErr:   job.lastErr,
	}
}

func (j *boardDailyJob) setTotal(n int) {
	j.mu.Lock()
	j.total = n
	j.updatedAt = time.Now().UTC()
	j.mu.Unlock()
}

func (j *boardDailyJob) markOk() {
	j.mu.Lock()
	j.ok++
	j.updatedAt = time.Now().UTC()
	j.mu.Unlock()
}

func (j *boardDailyJob) markFail(err error) {
	j.mu.Lock()
	j.failed++
	if err != nil {
		j.lastErr = err.Error()
	}
	j.updatedAt = time.Now().UTC()
	j.mu.Unlock()
}

func (j *boardDailyJob) finish() {
	j.mu.Lock()
	j.running = false
	j.updatedAt = time.Now().UTC()
	j.mu.Unlock()
}

func runBoardDailyBatch(job *boardDailyJob, em *eastmoney.Client, db *sql.DB, tp, fs, fid string, limit int) {
	defer job.finish()
	ctx := context.Background()
	items, err := em.BoardListAll(ctx, fs, fid)
	if err != nil {
		job.markFail(err)
		return
	}
	job.setTotal(len(items))
	nameByCode := make(map[string]string, len(items))
	for _, it := range items {
		if it.Code != "" {
			nameByCode[it.Code] = it.Name
		}
	}

	for _, it := range items {
		code := it.Code
		if code == "" {
			job.markFail(fmt.Errorf("empty board code"))
			continue
		}
		rows, err := em.BoardFundflowDailySeries(ctx, code, limit)
		if err != nil {
			job.markFail(err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(rows) == 0 {
			job.markFail(fmt.Errorf("empty series for %s", code))
			time.Sleep(120 * time.Millisecond)
			continue
		}
		series := make([]sqlite.BoardDailySeriesPoint, 0, len(rows))
		for _, row := range rows {
			name := row.Name
			if name == "" {
				name = nameByCode[code]
			}
			c := row.Code
			if c == "" {
				c = code
			}
			series = append(series, sqlite.BoardDailySeriesPoint{
				TradeDate: row.TradeDate,
				Code:      c,
				Name:      name,
				Value:     row.NetMain,
				Price:     0,
				Pct:       0,
			})
		}
		if err := sqlite.UpsertBoardDailySeries(db, tp, fid, series); err != nil {
			job.markFail(err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		job.markOk()
		time.Sleep(120 * time.Millisecond)
	}
}

func formatRFC3339Nano(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
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
