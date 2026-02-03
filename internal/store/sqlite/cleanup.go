package sqlite

import (
	"database/sql"
	"fmt"
	"time"
)

// CleanupOldData deletes rows older than retentionDays from both realtime and daily tables.
// Realtime tables use a fixed-width RFC3339Nano format stored as TEXT, so lexicographic compare works.
func CleanupOldData(db *sql.DB, nowUTC time.Time, retentionDays int) error {
	if retentionDays < 1 {
		return fmt.Errorf("retentionDays must be >= 1")
	}

	utcCutoff := nowUTC.AddDate(0, 0, -retentionDays)
	utcCutoffStr := fixedRFC3339Nano(utcCutoff)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	dateCutoff := nowUTC.In(loc).AddDate(0, 0, -retentionDays).Format("2006-01-02")

	stmts := []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM northbound_rt WHERE ts_utc < ?`, []any{utcCutoffStr}},
		{`DELETE FROM fundflow_rt WHERE ts_utc < ?`, []any{utcCutoffStr}},
		{`DELETE FROM toplist_rt WHERE ts_utc < ?`, []any{utcCutoffStr}},
		{`DELETE FROM board_rt WHERE ts_utc < ?`, []any{utcCutoffStr}},
		{`DELETE FROM market_agg_rt WHERE ts_utc < ?`, []any{utcCutoffStr}},

		{`DELETE FROM northbound_daily WHERE trade_date < ?`, []any{dateCutoff}},
		{`DELETE FROM fundflow_daily WHERE trade_date < ?`, []any{dateCutoff}},
		{`DELETE FROM board_daily WHERE trade_date < ?`, []any{dateCutoff}},
		{`DELETE FROM market_agg_daily WHERE trade_date < ?`, []any{dateCutoff}},
		{`DELETE FROM margin_daily WHERE trade_date < ?`, []any{dateCutoff}},
	}

	for _, st := range stmts {
		if _, err := db.Exec(st.sql, st.args...); err != nil {
			return err
		}
	}
	return nil
}

