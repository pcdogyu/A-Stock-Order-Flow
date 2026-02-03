package sqlite

import "database/sql"

type BoardSumRTPoint struct {
	TSUTC string  `json:"ts_utc"`
	Value float64 `json:"value"`
}

type BoardSumDailyPoint struct {
	TradeDate string  `json:"trade_date"`
	Value     float64 `json:"value"`
}

func QueryBoardSumRT(db *sql.DB, boardType, fid string, limit int) ([]BoardSumRTPoint, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.Query(`
		SELECT ts_utc, SUM(value) AS v
		FROM board_rt
		WHERE board_type = ? AND fid = ?
		GROUP BY ts_utc
		ORDER BY ts_utc DESC
		LIMIT ?
	`, boardType, fid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]BoardSumRTPoint, 0, limit)
	for rows.Next() {
		var p BoardSumRTPoint
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

func QueryBoardSumDaily(db *sql.DB, boardType, fid string, limit int) ([]BoardSumDailyPoint, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.Query(`
		SELECT trade_date, SUM(value) AS v
		FROM board_daily
		WHERE board_type = ? AND fid = ?
		GROUP BY trade_date
		ORDER BY trade_date DESC
		LIMIT ?
	`, boardType, fid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]BoardSumDailyPoint, 0, limit)
	for rows.Next() {
		var p BoardSumDailyPoint
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

