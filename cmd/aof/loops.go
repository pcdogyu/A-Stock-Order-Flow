package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/collector"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/store/sqlite"
)

type cfgProvider interface {
	Get() config.Config
}

func runPersistLoop(ctx context.Context, cfgp cfgProvider, c *collector.Collector) {
	var lastInterval int
	for {
		cfg := cfgp.Get()
		interval := cfg.Persist.IntervalSeconds
		if interval <= 0 {
			interval = 60
		}
		if interval != lastInterval {
			log.Printf("persist loop: interval=%ds", interval)
			lastInterval = interval
		}

		if err := c.PersistRealtimeSnapshot(time.Now().UTC()); err != nil {
			log.Printf("persist snapshot err: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(interval) * time.Second):
		}
	}
}

func runCleanupLoop(ctx context.Context, cfgp cfgProvider, db *sql.DB) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	var lastRunDay string

	for {
		cfg := cfgp.Get()
		enabled := true
		if cfg.Cleanup.Enabled != nil {
			enabled = *cfg.Cleanup.Enabled
		}
		if !enabled {
			// Sleep a bit and check again in case settings changed.
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
				continue
			}
		}

		now := time.Now().In(loc)
		today := now.Format("2006-01-02")
		if lastRunDay != today && now.After(nextRunTimeToday(now, cfg.Cleanup.RunAt)) {
			if err := sqlite.CleanupOldData(db, time.Now().UTC(), cfg.RetentionDays); err != nil {
				log.Printf("cleanup err: %v", err)
			} else {
				log.Printf("cleanup ok: retention_days=%d", cfg.RetentionDays)
			}
			lastRunDay = today
		}

		// Tick at 1-minute granularity; this is a once-per-day job.
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Minute):
		}
	}
}

func nextRunTimeToday(now time.Time, runAt string) time.Time {
	// runAt: "HH:MM" Asia/Shanghai
	h, m := 3, 10
	if len(runAt) == 5 && runAt[2] == ':' {
		if v, err := time.Parse("15:04", runAt); err == nil {
			h = v.Hour()
			m = v.Minute()
		}
	}
	return time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, now.Location())
}

