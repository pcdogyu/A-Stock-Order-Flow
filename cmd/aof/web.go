package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/memstore"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/runtimecfg"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/store/sqlite"
)

//go:embed web/static/*
var webFS embed.FS

func newWebServer(mgr *runtimecfg.Manager, db *sql.DB, mem *memstore.Store) http.Handler {
	if mem == nil {
		mem = memstore.New()
	}
	em := eastmoney.NewClient()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("/api/realtime", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		snap := mem.SnapshotLatest()
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
		fid := r.URL.Query().Get("fid")
		if fid == "" {
			fid = "f62"
		}
		limit := parseLimit(r.URL.Query().Get("limit"), 50, 200)

		snap := mem.SnapshotLatest()
		key := tp + ":" + fid
		rows := snap.BoardsByKey[key]
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
		writeJSON(w, http.StatusOK, out)
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
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"total": total, "rows": rows, "ts_utc": time.Now().UTC()})
	})

	// Static UI.
	sub, _ := fs.Sub(webFS, "web/static")
	fileServer := http.FileServer(http.FS(sub))
	mux.Handle("/static/", http.StripPrefix("/static/", fileServer))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			// Let fileServer try to serve embedded file if it exists.
			// Prevent directory traversal: only allow /static/* here.
			if strings.HasPrefix(r.URL.Path, "/static/") {
				http.StripPrefix("/static/", fileServer).ServeHTTP(w, r)
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
		_, _ = w.Write(b)
	})

	return logRequests(mux)
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

	Industry config.BoardConfig    `json:"industry"`
	Concept  config.BoardConfig    `json:"concept"`
	MarketAgg config.MarketAggConfig `json:"market_agg"`
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
