package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/memstore"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/market"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/symbol"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/store/sqlite"
)

type ConfigProvider interface {
	Get() config.Config
}

type Collector struct {
	cfgp ConfigProvider
	db  *sql.DB
	em  *eastmoney.Client
	loc *time.Location
	mem *memstore.Store

	lastIndustry time.Time
	lastConcept  time.Time
	lastAllStocks time.Time
}

func New(cfgp ConfigProvider, db *sql.DB, mem *memstore.Store) *Collector {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if mem == nil {
		mem = memstore.New()
	}
	return &Collector{
		cfgp: cfgp,
		db:  db,
		em:  eastmoney.NewClient(),
		loc: loc,
		mem: mem,
	}
}

func (c *Collector) RunRealtime(ctx context.Context) error {
	var lastLog time.Time
	var lastInterval int
	for {
		cfg := c.cfgp.Get()
		if lastInterval != cfg.Realtime.IntervalSeconds || time.Since(lastLog) > time.Minute {
			log.Printf("realtime running: interval=%ds watchlist=%d toplist=%d",
				cfg.Realtime.IntervalSeconds, len(cfg.Watchlist), cfg.Toplist.Size)
			lastInterval = cfg.Realtime.IntervalSeconds
			lastLog = time.Now()
		}

		if err := c.collectRealtimeOnce(ctx, time.Now().In(c.loc), cfg); err != nil {
			log.Printf("realtime tick error: %v", err)
		}

		sleep := time.Duration(cfg.Realtime.IntervalSeconds) * time.Second
		if sleep <= 0 {
			sleep = 10 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (c *Collector) collectRealtimeOnce(ctx context.Context, now time.Time, cfg config.Config) error {
	if cfg.Realtime.OnlyDuringHours != nil && *cfg.Realtime.OnlyDuringHours && !market.IsCNTradingTime(now) {
		return nil
	}

	ts := now.UTC()

	// 1) Northbound (沪股通/深股通)
	nb, err := c.em.NorthboundRealtime(ctx)
	if err != nil {
		return fmt.Errorf("northbound rt: %w", err)
	}
	c.mem.SetNorthbound(ts, nb)

	// 2) Watchlist fundflow (主力/超大/大/中/小)
	secids, err := symbol.ToEastmoneySecIDs(cfg.Watchlist)
	if err != nil {
		return err
	}
	ffRows, err := c.em.FundflowRealtime(ctx, secids)
	if err != nil {
		return fmt.Errorf("fundflow rt: %w", err)
	}
	c.mem.SetFundflow(ts, ffRows)

	// 3) Top list by net main inflow (or any Eastmoney fid field)
	top, err := c.em.TopListDynamic(ctx, cfg.Toplist.FS, cfg.Toplist.FID, cfg.Toplist.Size)
	if err != nil {
		return fmt.Errorf("toplist rt: %w", err)
	}
	c.mem.SetToplist(ts, cfg.Toplist.FID, top)

	// 4) Industry / Concept boards + whole-market aggregate (computed from industry sum)
	if cfg.Industry.Enabled {
		interval := time.Duration(cfg.Industry.IntervalSeconds) * time.Second
		if c.lastIndustry.IsZero() || now.Sub(c.lastIndustry) >= interval {
			items, err := c.em.BoardListAll(ctx, cfg.Industry.FS, cfg.Industry.FID)
			if err != nil {
				log.Printf("industry boards rt err: %v", err)
			} else {
				c.mem.SetBoard(ts, "industry", cfg.Industry.FID, items)
				var sum float64
				for _, it := range items {
					sum += it.Price
				}
				c.mem.SetAgg(ts, "industry_sum", cfg.Industry.FID, sum)
			}
			c.lastIndustry = now
		}
	}

	if cfg.Concept.Enabled {
		interval := time.Duration(cfg.Concept.IntervalSeconds) * time.Second
		if c.lastConcept.IsZero() || now.Sub(c.lastConcept) >= interval {
			var items []eastmoney.TopItem
			var err error
			if cfg.Concept.CollectAll {
				items, err = c.em.BoardListAll(ctx, cfg.Concept.FS, cfg.Concept.FID)
			} else {
				items, err = c.em.BoardListTop(ctx, cfg.Concept.FS, cfg.Concept.FID, cfg.Concept.TopSize)
			}
			if err != nil {
				log.Printf("concept boards rt err: %v", err)
			} else {
				c.mem.SetBoard(ts, "concept", cfg.Concept.FID, items)
			}
			c.lastConcept = now
		}
	}

	// 5) Whole-market aggregate by paging all A-share stocks and summing fid.
	if cfg.MarketAgg.Enabled {
		interval := time.Duration(cfg.MarketAgg.IntervalSeconds) * time.Second
		if c.lastAllStocks.IsZero() || now.Sub(c.lastAllStocks) >= interval {
			sum, total, err := c.em.AllStocksSum(ctx, cfg.MarketAgg.FS, cfg.MarketAgg.FID, cfg.MarketAgg.Concurrency)
			if err != nil {
				log.Printf("allstocks sum rt err: %v", err)
			} else {
				_ = total // kept for logging later if needed
				c.mem.SetAgg(ts, "allstocks_sum", cfg.MarketAgg.FID, sum)
			}
			c.lastAllStocks = now
		}
	}

	return nil
}

// PersistRealtimeSnapshot writes an in-memory snapshot to SQLite "rt" tables.
// Caller controls the interval.
func (c *Collector) PersistRealtimeSnapshot(tsUTC time.Time) error {
	snap := c.mem.Snapshot(tsUTC)
	if snap.Northbound != nil {
		if err := sqlite.UpsertNorthboundRT(c.db, tsUTC, *snap.Northbound); err != nil {
			return err
		}
	}
	if err := sqlite.UpsertFundflowRT(c.db, tsUTC, snap.Fundflow); err != nil {
		return err
	}
	for fid, rows := range snap.ToplistByFID {
		if err := sqlite.UpsertTopListRT(c.db, tsUTC, fid, rows); err != nil {
			return err
		}
	}
	for key, rows := range snap.BoardsByKey {
		bt, fid, ok := split2(key)
		if !ok {
			continue
		}
		if err := sqlite.UpsertBoardRT(c.db, tsUTC, bt, fid, rows); err != nil {
			return err
		}
	}
	for key, v := range snap.AggByKey {
		source, fid, ok := split2(key)
		if !ok {
			continue
		}
		if err := sqlite.UpsertMarketAggRT(c.db, tsUTC, source, fid, v); err != nil {
			return err
		}
	}
	return nil
}

func split2(s string) (a, b string, ok bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

func (c *Collector) RunDaily(ctx context.Context, date time.Time) error {
	cfg := c.cfgp.Get()
	// We treat "daily" as "fetch latest and persist to date bucket".
	// For free sources this is more robust than attempting holiday calendars.
	tradeDate := date.In(c.loc).Format("2006-01-02")
	log.Printf("daily started: trade_date=%s watchlist=%d", tradeDate, len(cfg.Watchlist))

	// 1) Northbound: use realtime endpoint and persist as daily snapshot.
	nb, err := c.em.NorthboundRealtime(ctx)
	if err != nil {
		return fmt.Errorf("northbound daily via rt: %w", err)
	}
	if err := sqlite.UpsertNorthboundDaily(c.db, tradeDate, nb); err != nil {
		return fmt.Errorf("store northbound daily: %w", err)
	}

	// 2) Fundflow daily: use fflow kline endpoint (daily series) and take last entry.
	for _, sym := range cfg.Watchlist {
		secid, err := symbol.ToEastmoneySecID(sym)
		if err != nil {
			log.Printf("skip symbol=%q: %v", sym, err)
			continue
		}
		row, err := c.em.FundflowDailyLatest(ctx, secid)
		if err != nil {
			log.Printf("fundflow daily err symbol=%s: %v", sym, err)
			continue
		}
		if err := sqlite.UpsertFundflowDaily(c.db, row.TradeDate, row); err != nil {
			log.Printf("store fundflow daily err symbol=%s: %v", sym, err)
		}
	}

	// 3) Margin (融资融券) daily: query per-symbol latest record, then store.
	for _, sym := range cfg.Watchlist {
		code, err := symbol.CodeOnly(sym)
		if err != nil {
			log.Printf("skip symbol=%q: %v", sym, err)
			continue
		}
		row, err := c.em.MarginLatestByCode(ctx, code)
		if err != nil {
			log.Printf("margin daily err symbol=%s: %v", sym, err)
			continue
		}
		if err := sqlite.UpsertMarginDaily(c.db, row.TradeDate, row); err != nil {
			log.Printf("store margin daily err symbol=%s: %v", sym, err)
		}
	}

	// 4) Industry/Concept daily snapshots + whole-market aggregate.
	if cfg.Industry.Enabled {
		items, err := c.em.BoardListAll(ctx, cfg.Industry.FS, cfg.Industry.FID)
		if err != nil {
			log.Printf("industry boards daily err: %v", err)
		} else {
			_ = sqlite.UpsertBoardDaily(c.db, tradeDate, "industry", cfg.Industry.FID, items)
			var sum float64
			for _, it := range items {
				sum += it.Price
			}
			_ = sqlite.UpsertMarketAggDaily(c.db, tradeDate, "industry_sum", cfg.Industry.FID, sum)
		}
	}
	if cfg.Concept.Enabled {
		var items []eastmoney.TopItem
		var err error
		if cfg.Concept.CollectAll {
			items, err = c.em.BoardListAll(ctx, cfg.Concept.FS, cfg.Concept.FID)
		} else {
			items, err = c.em.BoardListTop(ctx, cfg.Concept.FS, cfg.Concept.FID, cfg.Concept.TopSize)
		}
		if err != nil {
			log.Printf("concept boards daily err: %v", err)
		} else {
			_ = sqlite.UpsertBoardDaily(c.db, tradeDate, "concept", cfg.Concept.FID, items)
		}
	}

	if cfg.MarketAgg.Enabled {
		sum, _, err := c.em.AllStocksSum(ctx, cfg.MarketAgg.FS, cfg.MarketAgg.FID, cfg.MarketAgg.Concurrency)
		if err != nil {
			log.Printf("allstocks sum daily err: %v", err)
		} else {
			_ = sqlite.UpsertMarketAggDaily(c.db, tradeDate, "allstocks_sum", cfg.MarketAgg.FID, sum)
		}
	}

	// Daily run is a good place to apply retention as well (for cron/task-scheduler usage).
	if cfg.RetentionDays > 0 {
		if err := sqlite.CleanupOldData(c.db, time.Now().UTC(), cfg.RetentionDays); err != nil {
			log.Printf("cleanup err: %v", err)
		}
	}

	return nil
}
