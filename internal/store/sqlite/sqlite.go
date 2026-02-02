package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	// modernc.org/sqlite uses a file path DSN.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func Migrate(db *sql.DB) error {
	stmts := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=NORMAL;`,

		`CREATE TABLE IF NOT EXISTS northbound_rt (
			ts_utc TEXT PRIMARY KEY,
			trade_date TEXT,
			sh_day_net_amt_in REAL,
			sh_net_buy_amt REAL,
			sh_buy_amt REAL,
			sh_sell_amt REAL,
			sz_day_net_amt_in REAL,
			sz_net_buy_amt REAL,
			sz_buy_amt REAL,
			sz_sell_amt REAL
		);`,

		`CREATE TABLE IF NOT EXISTS northbound_daily (
			trade_date TEXT PRIMARY KEY,
			sh_day_net_amt_in REAL,
			sh_net_buy_amt REAL,
			sh_buy_amt REAL,
			sh_sell_amt REAL,
			sz_day_net_amt_in REAL,
			sz_net_buy_amt REAL,
			sz_buy_amt REAL,
			sz_sell_amt REAL
		);`,

		`CREATE TABLE IF NOT EXISTS fundflow_rt (
			ts_utc TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			net_main REAL,
			net_xl REAL,
			net_l REAL,
			net_m REAL,
			net_s REAL,
			PRIMARY KEY (ts_utc, code)
		);`,

		`CREATE TABLE IF NOT EXISTS fundflow_daily (
			trade_date TEXT NOT NULL,
			secid TEXT NOT NULL,
			code TEXT,
			name TEXT,
			net_main REAL,
			net_xl REAL,
			net_l REAL,
			net_m REAL,
			net_s REAL,
			PRIMARY KEY (trade_date, secid)
		);`,

		`CREATE TABLE IF NOT EXISTS toplist_rt (
			ts_utc TEXT NOT NULL,
			fid TEXT NOT NULL,
			rank INTEGER NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			price REAL,
			pct REAL,
			value REAL,
			PRIMARY KEY (ts_utc, fid, rank)
		);`,

		`CREATE TABLE IF NOT EXISTS board_rt (
			ts_utc TEXT NOT NULL,
			board_type TEXT NOT NULL, -- "industry" | "concept"
			fid TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			price REAL,
			pct REAL,
			value REAL,
			PRIMARY KEY (ts_utc, board_type, fid, code)
		);`,

		`CREATE TABLE IF NOT EXISTS board_daily (
			trade_date TEXT NOT NULL,
			board_type TEXT NOT NULL,
			fid TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			price REAL,
			pct REAL,
			value REAL,
			PRIMARY KEY (trade_date, board_type, fid, code)
		);`,

		`CREATE TABLE IF NOT EXISTS market_agg_rt (
			ts_utc TEXT NOT NULL,
			source TEXT NOT NULL, -- e.g. "industry_sum"
			fid TEXT NOT NULL,
			value REAL,
			PRIMARY KEY (ts_utc, source, fid)
		);`,

		`CREATE TABLE IF NOT EXISTS market_agg_daily (
			trade_date TEXT NOT NULL,
			source TEXT NOT NULL,
			fid TEXT NOT NULL,
			value REAL,
			PRIMARY KEY (trade_date, source, fid)
		);`,

		`CREATE TABLE IF NOT EXISTS margin_daily (
			trade_date TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			market TEXT,
			rzye REAL,
			rzmre REAL,
			rzche REAL,
			rzjme REAL,
			rqye REAL,
			rqmcl REAL,
			rqchl REAL,
			rqjmg REAL,
			rzrqye REAL,
			PRIMARY KEY (trade_date, code)
		);`,
	}

	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}
