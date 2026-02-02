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

type Collector struct {
	cfg config.Config
	db  *sql.DB
	em  *eastmoney.Client
	loc *time.Location
}

func New(cfg config.Config, db *sql.DB) *Collector {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return &Collector{
		cfg: cfg,
		db:  db,
		em:  eastmoney.NewClient(),
		loc: loc,
	}
}

func (c *Collector) RunRealtime(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(c.cfg.Realtime.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("realtime started: interval=%ds watchlist=%d toplist=%d",
		c.cfg.Realtime.IntervalSeconds, len(c.cfg.Watchlist), c.cfg.Toplist.Size)

	for {
		if err := c.collectRealtimeOnce(ctx, time.Now().In(c.loc)); err != nil {
			log.Printf("realtime tick error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Collector) collectRealtimeOnce(ctx context.Context, now time.Time) error {
	if c.cfg.Realtime.OnlyDuringHours != nil && *c.cfg.Realtime.OnlyDuringHours && !market.IsCNTradingTime(now) {
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
	secids, err := symbol.ToEastmoneySecIDs(c.cfg.Watchlist)
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
	top, err := c.em.TopListDynamic(ctx, c.cfg.Toplist.FS, c.cfg.Toplist.FID, c.cfg.Toplist.Size)
	if err != nil {
		return fmt.Errorf("toplist rt: %w", err)
	}
	if err := sqlite.UpsertTopListRT(c.db, ts, c.cfg.Toplist.FID, top); err != nil {
		return fmt.Errorf("store toplist rt: %w", err)
	}

	return nil
}

func (c *Collector) RunDaily(ctx context.Context, date time.Time) error {
	// We treat "daily" as "fetch latest and persist to date bucket".
	// For free sources this is more robust than attempting holiday calendars.
	tradeDate := date.In(c.loc).Format("2006-01-02")
	log.Printf("daily started: trade_date=%s watchlist=%d", tradeDate, len(c.cfg.Watchlist))

	// 1) Northbound: use realtime endpoint and persist as daily snapshot.
	nb, err := c.em.NorthboundRealtime(ctx)
	if err != nil {
		return fmt.Errorf("northbound daily via rt: %w", err)
	}
	if err := sqlite.UpsertNorthboundDaily(c.db, tradeDate, nb); err != nil {
		return fmt.Errorf("store northbound daily: %w", err)
	}

	// 2) Fundflow daily: use fflow kline endpoint (daily series) and take last entry.
	for _, sym := range c.cfg.Watchlist {
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
	for _, sym := range c.cfg.Watchlist {
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

	return nil
}
