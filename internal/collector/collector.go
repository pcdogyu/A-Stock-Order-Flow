package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
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

	lastIndustry time.Time
	lastConcept  time.Time
	lastAllStocks time.Time
}

func New(cfgp ConfigProvider, db *sql.DB) *Collector {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return &Collector{
		cfgp: cfgp,
		db:  db,
		em:  eastmoney.NewClient(),
		loc: loc,
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
	if err := sqlite.UpsertNorthboundRT(c.db, ts, nb); err != nil {
		return fmt.Errorf("store northbound rt: %w", err)
	}

	// 2) Watchlist fundflow (主力/超大/大/中/小)
	secids, err := symbol.ToEastmoneySecIDs(cfg.Watchlist)
	if err != nil {
		return err
	}
	ffRows, err := c.em.FundflowRealtime(ctx, secids)
	if err != nil {
		return fmt.Errorf("fundflow rt: %w", err)
	}
	if err := sqlite.UpsertFundflowRT(c.db, ts, ffRows); err != nil {
		return fmt.Errorf("store fundflow rt: %w", err)
	}

	// 3) Top list by net main inflow (or any Eastmoney fid field)
	top, err := c.em.TopListDynamic(ctx, cfg.Toplist.FS, cfg.Toplist.FID, cfg.Toplist.Size)
	if err != nil {
		return fmt.Errorf("toplist rt: %w", err)
	}
	if err := sqlite.UpsertTopListRT(c.db, ts, cfg.Toplist.FID, top); err != nil {
		return fmt.Errorf("store toplist rt: %w", err)
	}

	// 4) Industry / Concept boards + whole-market aggregate (computed from industry sum)
	if cfg.Industry.Enabled {
		interval := time.Duration(cfg.Industry.IntervalSeconds) * time.Second
		if c.lastIndustry.IsZero() || now.Sub(c.lastIndustry) >= interval {
			items, err := c.em.BoardListAll(ctx, cfg.Industry.FS, cfg.Industry.FID)
			if err != nil {
				log.Printf("industry boards rt err: %v", err)
			} else {
				if err := sqlite.UpsertBoardRT(c.db, ts, "industry", cfg.Industry.FID, items); err != nil {
					log.Printf("store industry boards rt err: %v", err)
				}
				var sum float64
				for _, it := range items {
					sum += it.Value
				}
				if err := sqlite.UpsertMarketAggRT(c.db, ts, "industry_sum", cfg.Industry.FID, sum); err != nil {
					log.Printf("store market agg rt err: %v", err)
				}
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
				if err := sqlite.UpsertBoardRT(c.db, ts, "concept", cfg.Concept.FID, items); err != nil {
					log.Printf("store concept boards rt err: %v", err)
				}
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
				if err := sqlite.UpsertMarketAggRT(c.db, ts, "allstocks_sum", cfg.MarketAgg.FID, sum); err != nil {
					log.Printf("store allstocks sum rt err: %v", err)
				}
			}
			c.lastAllStocks = now
		}
	}

	return nil
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
				sum += it.Value
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

	return nil
}
