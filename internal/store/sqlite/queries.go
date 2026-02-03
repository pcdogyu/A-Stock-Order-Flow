package sqlite

import (
	"database/sql"
)

type MarketAggRTPoint struct {
	TSUTC string  `json:"ts_utc"`
	Value float64 `json:"value"`
}

type MarketAggDailyPoint struct {
	TradeDate string  `json:"trade_date"`
	Value     float64 `json:"value"`
}

type BoardRTPoint struct {
	TSUTC string  `json:"ts_utc"`
	Value float64 `json:"value"`
}

func QueryMarketAggRT(db *sql.DB, source, fid string, limit int) ([]MarketAggRTPoint, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.Query(`
		SELECT ts_utc, value
		FROM market_agg_rt
		WHERE source = ? AND fid = ?
		ORDER BY ts_utc DESC
		LIMIT ?
	`, source, fid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]MarketAggRTPoint, 0, limit)
	for rows.Next() {
		var p MarketAggRTPoint
		if err := rows.Scan(&p.TSUTC, &p.Value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reverse(out)
	return out, nil
}

func QueryMarketAggDaily(db *sql.DB, source, fid string, limit int) ([]MarketAggDailyPoint, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.Query(`
		SELECT trade_date, value
		FROM market_agg_daily
		WHERE source = ? AND fid = ?
		ORDER BY trade_date DESC
		LIMIT ?
	`, source, fid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]MarketAggDailyPoint, 0, limit)
	for rows.Next() {
		var p MarketAggDailyPoint
		if err := rows.Scan(&p.TradeDate, &p.Value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reverse(out)
	return out, nil
}

func QueryBoardRTSeriesByCode(db *sql.DB, code, startUTC, endUTC string, limit int) ([]BoardRTPoint, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := db.Query(`
		SELECT ts_utc, value
		FROM board_rt
		WHERE code = ? AND ts_utc >= ? AND ts_utc <= ?
		ORDER BY ts_utc ASC
		LIMIT ?
	`, code, startUTC, endUTC, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]BoardRTPoint, 0, limit)
	for rows.Next() {
		var p BoardRTPoint
		if err := rows.Scan(&p.TSUTC, &p.Value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
